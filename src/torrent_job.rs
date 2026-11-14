use crate::bencode::{BValue, encode};
use crate::cli::Cli;
use crate::dht::{DhtNode, Id};
use crate::events::{EventBroker, EventType};
use crate::http::HttpFetcher;
use crate::job::{ErrorCategory, JobManifest, JobPrivateData, JobRegistry, JobState, com_registro};
use crate::magnet::MagnetLink;
use crate::metadata_transfer::MetadataTransfer;
use crate::metainfo::{Info, Metainfo};
use crate::peer_transfer::{PeerReader, PeerTransfer};
use crate::peer_wire::Handshake;
use crate::throttle::RateLimiter;
use crate::torrent_storage::{Layout, TorrentPieceMap, TorrentStorage};
use crate::tracker::{
    MAX_PEERS, Tracker, TrackerClient, TrackerEvent, TrackerRequest, TrackerResponse,
    validar_tracker_url,
};
use crate::tracker_udp::{self, UdpTracker};
use anyhow::{Context, Result, anyhow, ensure};
use futures::{FutureExt, future::join_all};
use sha1::{Digest, Sha1};
use std::collections::{HashSet, VecDeque};
use std::fs;
use std::io::{self, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

const TIMEOUT: Duration = Duration::from_secs(5);
const ANNOUNCE_TIMEOUT: Duration = Duration::from_secs(15);
const METADATA_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) const PRIVATE_TORRENT_CLI_ERROR: &str =
    "private .torrent cannot use DHT and requires --peer or a tracker";

pub(crate) fn tem_fonte_torrent<'a>(
    mut trackers: impl Iterator<Item = &'a str>,
    rede_protegida: bool,
    dht_ligado: bool,
    privado: bool,
) -> bool {
    (dht_ligado && !privado) || trackers.any(|url| validar_tracker_url(url, rede_protegida).is_ok())
}

pub(crate) fn criar_torrent(
    directory: &Path,
    output: &Path,
    data: &[u8],
    metainfo: &Metainfo,
    peers: Vec<SocketAddr>,
) -> Result<String> {
    com_registro(directory, |registry| {
        let source = format!("torrent:{}", crate::hex_lower(&metainfo.info_hash));
        let job = JobManifest::new(&[source], output);
        let filename = PathBuf::from(format!("{}.torrent", job.id));
        let mut copy = fs::File::create_new(directory.join(&filename))?;
        copy.write_all(data)?;
        copy.sync_all()?;
        JobPrivateData::new_torrent(filename, peers).save(directory, &job.id)?;
        let path = job.save(directory)?;
        registry.update(&job, &path);
        Ok(job.id)
    })
}

pub(crate) fn criar_magnet(
    directory: &Path,
    output: &Path,
    link: &str,
    magnet: &MagnetLink,
    peers: Vec<SocketAddr>,
) -> Result<String> {
    com_registro(directory, |registry| {
        let source = format!("torrent:{}", crate::hex_lower(&magnet.info_hash));
        let job = JobManifest::new(&[source], output);
        let private = JobPrivateData {
            magnet: Some(link.to_owned()),
            peers: Some(peers),
            ..JobPrivateData::new(&[])
        };
        private.save(directory, &job.id)?;
        let path = job.save(directory)?;
        registry.update(&job, &path);
        Ok(job.id)
    })
}

fn reconferir(
    storage: &TorrentStorage,
    layout: &Layout,
    hashes: &[[u8; 20]],
    map: &mut TorrentPieceMap,
    cancel_flag: &AtomicBool,
) -> Option<u64> {
    let mut downloaded = 0;
    let mut piece = Vec::new();
    for (index, expected) in hashes.iter().enumerate() {
        if cancel_flag.load(Ordering::Relaxed) {
            return None;
        }
        if !map.has_piece(index) {
            continue;
        }
        let length = layout
            .piece_length
            .min(layout.total_length - index as u64 * layout.piece_length);
        piece.resize(length as usize, 0);
        if storage.read_piece(index, &mut piece).is_ok()
            && <[u8; 20]>::from(Sha1::digest(&piece)) == *expected
        {
            downloaded += length;
        } else {
            map.clear_piece(index);
        }
    }
    Some(downloaded)
}

fn tracker_clients<'a>(
    tracker_urls: impl Iterator<Item = &'a str>,
    rede_protegida: bool,
    has_peer: bool,
) -> Result<Vec<Tracker>> {
    let mut urls = Vec::new();
    for raw in tracker_urls {
        let Ok(mut url) = url::Url::parse(raw) else {
            continue;
        };
        if !matches!(url.scheme(), "http" | "https" | "udp") {
            continue;
        }
        if url.scheme() == "udp" {
            if let Err(err) = tracker_udp::endpoint(&url) {
                crate::saida::erro(format!("Warning: tracker URL rejected: {err}"));
                continue;
            }
            url.set_path("");
            url.set_query(None);
        }
        url.set_fragment(None);
        if !urls.contains(&url) {
            urls.push(url);
            if urls.len() == 10 {
                break;
            }
        }
    }
    let fetcher = if urls.iter().any(|url| url.scheme() != "udp") {
        match HttpFetcher::new(None, None, Some(ANNOUNCE_TIMEOUT), rede_protegida) {
            Ok(fetcher) => Some(fetcher),
            Err(err) if has_peer || urls.iter().any(|url| url.scheme() == "udp") => {
                crate::saida::erro(format!(
                    "Warning: could not initialize HTTP trackers: {err}"
                ));
                None
            }
            Err(err) => return Err(err),
        }
    } else {
        None
    };
    Ok(urls
        .into_iter()
        .filter_map(|url| {
            let tracker = if url.scheme() == "udp" {
                UdpTracker::new(url, rede_protegida).map(Tracker::Udp)
            } else {
                TrackerClient::new(fetcher.clone()?, url.as_str(), rede_protegida)
                    .map(Tracker::Http)
            };
            match tracker {
                Ok(tracker) => Some(tracker),
                Err(err) => {
                    crate::saida::erro(format!("Warning: tracker URL rejected: {err}"));
                    None
                }
            }
        })
        .collect())
}

async fn with_announces<T>(
    trackers: &mut [Tracker],
    request: &TrackerRequest,
    tx: mpsc::UnboundedSender<Result<TrackerResponse>>,
    consume: impl Future<Output = T>,
) -> T {
    let announce = async move {
        join_all(trackers.iter_mut().map(|tracker| {
            let tx = &tx;
            async move {
                let result =
                    match tokio::time::timeout(ANNOUNCE_TIMEOUT, tracker.announce(request)).await {
                        Ok(result) => result,
                        Err(err) => Err(err.into()),
                    };
                // Let healthy trackers supply peers while slow announces are still in flight.
                let _ = tx.send(result);
            }
        }))
        .await;
    };
    tokio::pin!(consume);
    tokio::select! {
        biased;
        () = announce => consume.await,
        result = &mut consume => result,
    }
}

async fn baixar_metadados(
    manual_peers: &[SocketAddr],
    mut announces: mpsc::UnboundedReceiver<Result<TrackerResponse>>,
    request: &TrackerRequest,
    cancel_flag: &Arc<AtomicBool>,
    mut dht: Option<impl Future<Output = Vec<SocketAddr>>>,
) -> Result<(Vec<u8>, SocketAddr, bool), (ErrorCategory, anyhow::Error)> {
    let mut peers = VecDeque::from(manual_peers.to_vec());
    let mut seen: HashSet<_> = manual_peers.iter().copied().collect();
    let mut remaining = MAX_PEERS;
    let mut from_dht = false;
    let mut last_error = anyhow!("no peer provided torrent metadata");
    loop {
        if let Some(peer) = peers.pop_front() {
            let flag = Arc::clone(cancel_flag);
            let (info_hash, peer_id) = (request.info_hash, request.peer_id);
            let result = tokio::task::spawn_blocking(move || {
                ensure!(!flag.load(Ordering::Relaxed), "Cancelled");
                let deadline = Instant::now() + METADATA_TIMEOUT;
                let mut stream = TcpStream::connect_timeout(&peer, TIMEOUT)?;
                MetadataTransfer::download(&mut stream, &info_hash, &peer_id, deadline, &flag)
            })
            .await
            .map_err(|err| (ErrorCategory::Internal, err.into()))?;
            match result {
                Ok(metadata) => return Ok((metadata, peer, from_dht)),
                Err(err) => last_error = err,
            }
        } else if let Some(response) = if remaining > 0 {
            announces.recv().await
        } else {
            None
        } {
            match response {
                Ok(response) => {
                    peers.extend(
                        response
                            .peers
                            .into_iter()
                            .map(SocketAddr::V4)
                            .filter(|peer| seen.insert(*peer))
                            .take(remaining),
                    );
                    remaining -= peers.len();
                }
                Err(err) => last_error = err,
            }
        } else if let Some(search) = dht.take() {
            from_dht = true;
            peers.extend(search.await.into_iter().filter(|peer| seen.insert(*peer)));
        } else {
            return Err((ErrorCategory::Network, last_error));
        }
    }
}

fn save_magnet_torrent(path: &Path, trackers: &[String], metadata: &[u8]) -> Result<()> {
    let tiers = trackers
        .iter()
        .map(|url| BValue::List(vec![BValue::Bytes(url.as_bytes().to_vec())]))
        .collect();
    let tmp = path.with_extension("torrent.tmp");
    match fs::remove_file(&tmp) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err.into()),
    }
    let mut file = fs::File::create_new(&tmp)?;
    let written = (|| -> io::Result<()> {
        file.write_all(b"d13:announce-list")?;
        file.write_all(&encode(&BValue::List(tiers)))?;
        file.write_all(b"4:info")?;
        file.write_all(metadata)?;
        file.write_all(b"e")?;
        file.sync_all()
    })();
    drop(file);
    let saved = written.and_then(|()| fs::rename(&tmp, path));
    if saved.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    saved?;
    Ok(())
}

fn bytes_no_mapa(map: &TorrentPieceMap, layout: &Layout) -> u64 {
    let pieces: u64 = map
        .bitfield
        .iter()
        .map(|byte| u64::from(byte.count_ones()))
        .sum();
    let last_length = (layout.total_length - 1) % layout.piece_length + 1;
    let missing_tail = if map.has_piece(map.num_pieces - 1) {
        layout.piece_length - last_length
    } else {
        0
    };
    pieces * layout.piece_length - missing_tail
}

pub async fn executar(
    mut private: JobPrivateData,
    job: &JobManifest,
    directory: &Path,
    broker: EventBroker,
    args: &Cli,
    global_limiter: Arc<RateLimiter>,
) -> Result<(), (ErrorCategory, anyhow::Error)> {
    let cancel_flag = Arc::new(AtomicBool::new(false));
    let cancelled = || (ErrorCategory::Network, anyhow!("Cancelled"));
    let watch = async {
        let mut poll = tokio::time::interval(Duration::from_millis(250));
        loop {
            poll.tick().await;
            if let Ok(registry) = JobRegistry::load(directory)
                && registry
                    .get(&job.id)
                    .is_some_and(|record| record.state != JobState::Active)
            {
                cancel_flag.store(true, Ordering::Relaxed);
                break;
            }
        }
    }
    .fuse();
    tokio::pin!(watch);

    let mut peer_id = [0; 20];
    peer_id[..8].copy_from_slice(b"-CH0300-");
    getrandom::fill(&mut peer_id[8..]).map_err(|err| {
        (
            ErrorCategory::Internal,
            anyhow!("could not generate peer id: {err}"),
        )
    })?;
    let mut request = TrackerRequest {
        info_hash: [0; 20],
        peer_id,
        port: 6881,
        uploaded: 0,
        downloaded: 0,
        left: 1, // Metadata size does not tell us how many payload bytes are missing.
        compact: true,
        event: TrackerEvent::Started,
    };
    let rede_protegida = args.rpc_bind.is_some() && !args.allow_private_network;
    let dht = if args.disable_dht {
        None
    } else {
        Some(DhtNode::new(
            Id::random().map_err(|err| (ErrorCategory::Internal, err))?,
        ))
    };
    let mut manual_peers = private.peers.clone().unwrap_or_default();
    let mut metadata_trackers = None;
    let mut metadata_peer = None;
    let torrent = private
        .torrent
        .clone()
        .unwrap_or_else(|| format!("{}.torrent", job.id).into());
    let path = directory.join(&torrent);
    let storage_error = |err| (ErrorCategory::Storage, err);
    let invalid = |err| (ErrorCategory::InvalidInput, err);
    let check_hash = |hash: &[u8; 20]| -> Result<()> {
        ensure!(
            job.sources.as_slice() == [format!("torrent:{}", crate::hex_lower(hash))],
            ".torrent info-hash changed"
        );
        Ok(())
    };
    let data = match fs::read(&path) {
        Ok(data) => data,
        Err(err) if private.torrent.is_none() && err.kind() == io::ErrorKind::NotFound => {
            let link = private
                .magnet
                .as_deref()
                .context("missing magnet link")
                .map_err(invalid)?;
            let magnet = MagnetLink::parse(link).map_err(invalid)?;
            check_hash(&magnet.info_hash).map_err(invalid)?;
            let mut trackers = tracker_clients(
                magnet.trackers.iter().map(String::as_str),
                rede_protegida,
                !manual_peers.is_empty() || dht.is_some(),
            )
            .map_err(|err| (ErrorCategory::Internal, err))?;
            request.info_hash = magnet.info_hash;
            let (tx, rx) = mpsc::unbounded_channel();
            let (metadata, peer, from_dht) = tokio::select! {
                biased;
                () = &mut watch => return Err(cancelled()),
                result = with_announces(&mut trackers, &request, tx,
                    baixar_metadados(&manual_peers, rx, &request, &cancel_flag,
                        dht.as_ref().map(|node| node.peers(magnet.info_hash, &[],
                            &args.dht_bootstrap, args.job_dir.as_deref(), rede_protegida)))) => result?,
            };
            save_magnet_torrent(&path, &magnet.trackers, &metadata).map_err(storage_error)?;
            metadata_peer = Some((peer, from_dht));
            metadata_trackers = Some(trackers);
            fs::read(&path)
                .context("could not read .torrent copy")
                .map_err(storage_error)?
        }
        Err(err) => {
            return Err(storage_error(
                anyhow!(err).context("could not read .torrent copy"),
            ));
        }
    };
    let metainfo = Arc::new(Metainfo::from_bytes(&data).map_err(invalid)?);
    check_hash(&metainfo.info_hash).map_err(invalid)?;
    let is_private = metainfo.private;
    let dht = dht.as_ref().filter(|_| !is_private);
    if let Some((peer, from_dht)) = metadata_peer
        && !(from_dht && is_private)
        && !manual_peers.contains(&peer)
    {
        manual_peers.push(peer);
    }
    if is_private
        && manual_peers.is_empty()
        && !tem_fonte_torrent(metainfo.tracker_urls(), rede_protegida, false, is_private)
    {
        return Err(invalid(anyhow!(PRIVATE_TORRENT_CLI_ERROR)));
    }
    if private.torrent.is_none() {
        private.torrent = Some(torrent);
        private.save(directory, &job.id).map_err(storage_error)?;
    }
    let hashes = match &metainfo.info {
        Info::SingleFile(single) => &single.pieces,
        Info::MultiFile(multi) => &multi.pieces,
    };
    let num_pieces = hashes.len();
    let layout = Arc::new(Layout::from_info(&metainfo.info));
    let resuming =
        TorrentPieceMap::exists(directory, &job.id).map_err(|err| (ErrorCategory::Storage, err))?;
    if args.no_overwrite && !resuming {
        for file in &layout.files {
            let path =
                file.path_components
                    .iter()
                    .fold(job.output.clone(), |mut path, component| {
                        path.push(component);
                        path
                    });
            crate::ensure_output_allowed(&path, true)
                .map_err(|err| (ErrorCategory::Storage, err))?;
        }
    }
    let mut map = if resuming {
        TorrentPieceMap::load(directory, &job.id)
            .ok()
            .filter(|map| map.matches_piece_count(num_pieces))
            .unwrap_or_else(|| TorrentPieceMap::new(num_pieces))
    } else {
        let map = TorrentPieceMap::new(num_pieces);
        map.save(directory, &job.id)
            .map_err(|err| (ErrorCategory::Storage, err))?;
        map
    };
    let storage = Arc::new(
        TorrentStorage::new(&job.output, &metainfo.info)
            .map_err(|err| (ErrorCategory::Storage, err))?,
    );
    let resumed_bytes = if resuming {
        let metainfo = Arc::clone(&metainfo);
        let storage = Arc::clone(&storage);
        let flag = Arc::clone(&cancel_flag);
        let verify_layout = Arc::clone(&layout);
        let directory = directory.to_path_buf();
        let id = job.id.clone();
        let mut verify = tokio::task::spawn_blocking(move || {
            let hashes = match &metainfo.info {
                Info::SingleFile(single) => &single.pieces,
                Info::MultiFile(multi) => &multi.pieces,
            };
            let verified = reconferir(&storage, &verify_layout, hashes, &mut map, &flag);
            let saved = if verified.is_some() && !flag.load(Ordering::Relaxed) {
                storage
                    .sync_all()
                    .and_then(|()| map.save(&directory, &id))
                    .map(|_| ())
            } else {
                Ok(())
            };
            (map, verified, saved)
        });
        let (updated_map, verified, saved) = tokio::select! {
            biased;
            () = &mut watch => verify.await,
            joined = &mut verify => joined,
        }
        .map_err(|err| (ErrorCategory::Internal, err.into()))?;
        if cancel_flag.load(Ordering::Relaxed) {
            return Err(cancelled());
        }
        let downloaded = verified.ok_or_else(cancelled)?;
        saved.map_err(|err| (ErrorCategory::Storage, err))?;
        map = updated_map;
        broker.emit(EventType::Progress {
            id: job.id.clone(),
            bytes_downloaded: downloaded,
            bytes_total: Some(layout.total_length),
        });
        if downloaded == layout.total_length {
            return Ok(());
        }
        downloaded
    } else {
        0
    };

    let job_limiter = Arc::new(RateLimiter::new(args.limit_rate));
    let handshake = Handshake {
        info_hash: metainfo.info_hash,
        peer_id,
        supports_extension: false,
    };

    let mut trackers = match metadata_trackers {
        Some(trackers) => trackers,
        None => tracker_clients(
            metainfo.tracker_urls(),
            rede_protegida,
            !manual_peers.is_empty() || dht.is_some(),
        )
        .map_err(|err| (ErrorCategory::Internal, err))?,
    };
    request.info_hash = handshake.info_hash;
    request.left = layout.total_length - resumed_bytes;
    let mut attempted = false;
    let result = async {
        let mut last_error = anyhow!("no peer completed the torrent");
        for round in 0..3 {
            let round_request = TrackerRequest { ..request };
            let (tx, mut rx) = mpsc::unbounded_channel();
            let mut interval = None;
            let mut peers = VecDeque::from(manual_peers.clone());
            let mut seen: HashSet<_> = manual_peers.iter().copied().collect();
            let mut remaining = MAX_PEERS;
            let mut round_dht = dht;
            attempted = !trackers.is_empty();
            let (updated_map, completed) =
                with_announces(&mut trackers, &round_request, tx, async {
                    loop {
                        let Some(peer) = peers.pop_front() else {
                            let response = tokio::select! {
                                biased;
                                () = &mut watch => return Err(cancelled()),
                                response = rx.recv() => response,
                            };
                            match response {
                                Some(Ok(response)) => {
                                    let seconds = u64::from(response.interval.clamp(1, 120));
                                    // ponytail: one delay for the round; use per-tracker timers if tiers need independent retries.
                                    interval = Some(interval.unwrap_or(0).max(seconds));
                                    peers.extend(
                                        response
                                            .peers
                                            .into_iter()
                                            .map(SocketAddr::V4)
                                            .filter(|peer| seen.insert(*peer))
                                            .take(remaining),
                                    );
                                    remaining -= peers.len();
                                }
                                Some(Err(err)) => last_error = err,
                                None => {
                                    let Some(node) = round_dht.take() else {
                                        return Ok((map, false));
                                    };
                                    let found = tokio::select! {
                                        biased;
                                        () = &mut watch => return Err(cancelled()),
                                        peers = node.peers(handshake.info_hash, &metainfo.nodes,
                                            &args.dht_bootstrap, args.job_dir.as_deref(), rede_protegida) => peers,
                                    };
                                    peers.extend(found.into_iter().filter(|peer| seen.insert(*peer)));
                                }
                            }
                            continue;
                        };
                        let handshake = handshake.clone();
                        let peer_storage = Arc::clone(&storage);
                        let metainfo = Arc::clone(&metainfo);
                        let flag = Arc::clone(&cancel_flag);
                        let broker = broker.clone();
                        let id = job.id.clone();
                        let map_directory = directory.to_path_buf();
                        let global_limiter = Arc::clone(&global_limiter);
                        let job_limiter = Arc::clone(&job_limiter);
                        let mut transfer = tokio::task::spawn_blocking(move || {
                            let result = (|| -> Result<()> {
                                ensure!(!flag.load(Ordering::Relaxed), "Cancelled");
                                let mut stream = TcpStream::connect_timeout(&peer, TIMEOUT)?;
                                stream.set_write_timeout(Some(TIMEOUT))?;
                                handshake.write_to(&mut stream)?;
                                Handshake::read_from(
                                    &mut PeerReader {
                                        stream: &mut stream,
                                        deadline: Instant::now() + TIMEOUT,
                                        cancel_flag: Some(&flag),
                                    },
                                    &handshake.info_hash,
                                    &handshake.peer_id,
                                )?;
                                PeerTransfer::new(
                                    &mut stream,
                                    &peer_storage,
                                    &mut map,
                                    &metainfo.info,
                                    Some(&global_limiter),
                                    Some(&job_limiter),
                                    Some(flag),
                                )
                                .with_events(&broker, id.clone())
                                .with_piece_map(&map_directory, &id)
                                .download()
                            })();
                            let saved = peer_storage
                                .sync_all()
                                .and_then(|()| map.save(&map_directory, &id));
                            (map, result, saved)
                        });
                        let (updated_map, result, saved) = tokio::select! {
                            biased;
                            () = &mut watch => transfer.await,
                            joined = &mut transfer => joined,
                        }
                        .map_err(|err| (ErrorCategory::Internal, err.into()))?;
                        map = updated_map;
                        if attempted {
                            let downloaded = bytes_no_mapa(&map, &layout);
                            request.downloaded = downloaded - resumed_bytes;
                            request.left = layout.total_length - downloaded;
                        }
                        if cancel_flag.load(Ordering::Relaxed) {
                            return Err(cancelled());
                        }
                        saved.map_err(|err| (ErrorCategory::Storage, err))?;
                        match result {
                            Ok(()) => return Ok((map, true)),
                            Err(err) => last_error = err,
                        }
                    }
                })
                .await?;
            if completed {
                return Ok(());
            }
            map = updated_map;
            let Some(seconds) = interval.filter(|_| round < 2) else {
                break;
            };
            tokio::select! {
                biased;
                () = &mut watch => return Err(cancelled()),
                () = tokio::time::sleep(Duration::from_secs(seconds)) => {}
            }
            request.event = TrackerEvent::None;
        }
        Err((ErrorCategory::Network, last_error))
    }
    .await;

    request.event = if result.is_ok() {
        TrackerEvent::Completed
    } else {
        TrackerEvent::Stopped
    };
    if attempted {
        let final_announces = trackers
            .iter_mut()
            .map(|tracker| tracker.announce(&request));
        let _ = tokio::time::timeout(TIMEOUT, join_all(final_announces)).await;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metainfo::SingleFileInfo;

    #[test]
    fn saves_magnet_torrent_over_stale_temporary_file() {
        let dir = std::env::temp_dir().join(format!("chorus-b33-magnet-{}", std::process::id()));
        fs::create_dir(&dir).unwrap();
        let path = dir.join("job.torrent");
        let tmp = dir.join("job.torrent.tmp");
        fs::write(&tmp, b"incomplete torrent").unwrap();
        let info = b"d6:lengthi1e4:name1:x12:piece lengthi16384e6:pieces20:01234567890123456789e";
        save_magnet_torrent(&path, &["udp://localhost:6881".into()], info).unwrap();
        let saved = fs::read(&path).unwrap();
        assert!(!tmp.exists());
        fs::remove_dir_all(dir).unwrap();
        let mut expected = b"d13:announce-listll20:udp://localhost:6881ee4:info".to_vec();
        expected.extend_from_slice(info);
        expected.push(b'e');
        assert_eq!(saved, expected);
        assert_eq!(
            Metainfo::from_bytes(&saved).unwrap().info_hash,
            <[u8; 20]>::from(Sha1::digest(info))
        );
    }

    #[test]
    fn resume_recheck_honors_existing_cancellation() {
        let dir = std::env::temp_dir().join(format!("chorus-b31-recheck-{}", std::process::id()));
        let hashes = [[0; 20]];
        let info = Info::SingleFile(SingleFileInfo {
            name: "payload.bin".into(),
            piece_length: 16384,
            pieces: hashes.to_vec(),
            length: 1,
        });
        let storage = TorrentStorage::new(&dir, &info).unwrap();
        let layout = Layout::from_info(&info);
        let mut map = TorrentPieceMap::new(1);
        map.set_piece(0);
        let cancelled = AtomicBool::new(true);
        let verified = reconferir(&storage, &layout, &hashes, &mut map, &cancelled);
        drop(storage);
        fs::remove_dir_all(dir).unwrap();
        assert_eq!(verified, None);
        assert!(map.has_piece(0));
    }
}
