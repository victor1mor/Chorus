use crate::bencode::{BValue, encode};
use crate::cli::Cli;
use crate::dht::{DhtNode, Id};
use crate::events::{EventBroker, EventType};
use crate::http::HttpFetcher;
use crate::job::{ErrorCategory, JobManifest, JobPrivateData, JobRegistry, JobState, com_registro};
use crate::magnet::MagnetLink;
use crate::metadata_transfer::MetadataTransfer;
use crate::metainfo::{Info, Metainfo};
use crate::peer_transfer::{
    AparelhoHostil, EventoAparelho, PeerJob, PeerTransfer, socket_timeout, tamanho_peca,
};
use crate::peer_wire::Handshake;
use crate::quadro::{AparelhoId, QuadroDePecas};
use crate::throttle::RateLimiter;
use crate::torrent_storage::{Layout, TorrentPieceMap, TorrentStorage};
use crate::tracker::{
    MAX_PEERS, Tracker, TrackerClient, TrackerEvent, TrackerRequest, TrackerResponse,
    validar_tracker_url,
};
use crate::tracker_udp::{self, UdpTracker};
use anyhow::{Context, Result, anyhow, ensure};
use futures::{FutureExt, StreamExt, future::join_all, stream::FuturesUnordered};
use sha1::{Digest, Sha1};
use std::collections::{HashSet, VecDeque};
use std::fs;
use std::io::{self, Write};
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinSet;
use tokio::time::timeout;

const MAX_TENTATIVAS: usize = 10;
const MAX_CONECTADOS: usize = 50;
const PORTAS_PARA_BANIR_IP: usize = 3;
const ORCAMENTO_BYTES: u64 = 128 * 1024 * 1024;
const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(1);
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
        let length = tamanho_peca(layout, index);
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
    consume: impl Future<Output = (T, Option<TrackerRequest>)>,
) -> T {
    let mut pending: FuturesUnordered<_> = trackers
        .iter_mut()
        .map(|tracker| {
            let tx = tx.clone();
            async move {
                let result = match timeout(ANNOUNCE_TIMEOUT, tracker.announce(request)).await {
                    Ok(result) => result,
                    Err(err) => Err(err.into()),
                };
                let _ = tx.send(result);
                tracker
            }
        })
        .collect();
    drop(tx);
    let mut ready = Vec::new();
    let (result, final_request) = {
        tokio::pin!(consume);
        loop {
            tokio::select! {
                biased;
                Some(tracker) = pending.next() => ready.push(tracker),
                result = &mut consume => break result,
            }
        }
    };
    let _ = timeout(TIMEOUT, async {
        if let Some(request) = &final_request {
            let mut final_announces: FuturesUnordered<_> =
                ready.into_iter().map(|tracker| tracker.announce(request)).collect();
            loop {
                tokio::select! {
                    Some(tracker) = pending.next() => final_announces.push(tracker.announce(request)),
                    Some(_) = final_announces.next() => {}
                    else => break,
                }
            }
        } else {
            while pending.next().await.is_some() {}
        }
    }).await;
    result
}

async fn save_piece_map(
    storage: &Arc<TorrentStorage>,
    map: &TorrentPieceMap,
    directory: &Path,
    id: &str,
) -> Result<(), (ErrorCategory, anyhow::Error)> {
    let storage = Arc::clone(storage);
    let map = map.clone();
    let directory = directory.to_path_buf();
    let id = id.to_owned();
    tokio::task::spawn_blocking(move || storage.sync_all().and_then(|()| map.save(&directory, &id)))
        .await
        .map_err(|err| (ErrorCategory::Internal, err.into()))?
        .map(|_| ())
        .map_err(|err| (ErrorCategory::Storage, err))
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
                            .todos()
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

/// PEX (BEP 11) addresses go through the same filter as tracker and DHT addresses.
fn enfileirar_descobertos(
    novos: Vec<SocketAddr>,
    peers: &mut VecDeque<SocketAddr>,
    seen: &mut HashSet<SocketAddr>,
    banidos: &HashSet<SocketAddr>,
    ips_banidos: &HashSet<IpAddr>,
    remaining: &mut usize,
    rede_protegida: bool,
) {
    for peer in novos {
        if *remaining == 0 {
            break;
        }
        if crate::rede::endereco_aceito(&peer, rede_protegida)
            && !aparelho_banido(&peer, banidos, ips_banidos)
            && seen.insert(peer)
        {
            peers.push_back(peer);
            *remaining -= 1;
        }
    }
}

fn aparelho_banido(
    peer: &SocketAddr,
    banidos: &HashSet<SocketAddr>,
    ips_banidos: &HashSet<IpAddr>,
) -> bool {
    banidos.contains(peer) || ips_banidos.contains(&peer.ip())
}

async fn baixar_de_aparelho(
    peer: SocketAddr,
    handshake: Handshake,
    tentativa: OwnedSemaphorePermit,
    id: AparelhoId,
    job: Arc<PeerJob>,
    eventos: mpsc::UnboundedSender<EventoAparelho>,
) -> Result<()> {
    let (mut stream, remoto) = {
        let _tentativa = tentativa;
        let mut stream = timeout(TIMEOUT, tokio::net::TcpStream::connect(peer))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connection timed out"))??;
        let remoto = timeout(TIMEOUT, async {
            handshake.write_async(&mut stream).await?;
            Handshake::read_async(&mut stream, &handshake.info_hash, &handshake.peer_id).await
        })
        .await
        .map_err(|_| socket_timeout())??;
        (stream, remoto)
    };
    {
        let mut quadro = job.quadro.lock().unwrap();
        quadro.entrou(id)?;
    }
    let _ = eventos.send(EventoAparelho::Conectou);
    // BEP 6 and BEP 10 only when both sides set the reserved bit.
    let fast = handshake.supports_fast && remoto.supports_fast;
    let extensao = handshake.supports_extension && remoto.supports_extension;
    let result = PeerTransfer::new(&mut stream, &job, id, &eventos)
        .com_extensoes(fast, extensao)
        .download()
        .await;
    {
        let mut quadro = job.quadro.lock().unwrap();
        quadro.saiu(id)?;
    }
    result
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
    peer_id[..8].copy_from_slice(b"-CH0400-");
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
            let (metadata, peer, from_dht) = with_announces(&mut trackers, &request, tx, async {
                let result = tokio::select! {
                    biased;
                    () = &mut watch => Err(cancelled()),
                    result = baixar_metadados(&manual_peers, rx, &request, &cancel_flag,
                        dht.as_ref().map(|node| node.peers(magnet.info_hash, &[],
                            &args.dht_bootstrap, args.job_dir.as_deref(), rede_protegida))) => result,
                };
                (result, None)
            }).await?;
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
        TorrentPieceMap::new(num_pieces)
    };
    let storage = Arc::new(
        TorrentStorage::new(&job.output, &metainfo.info)
            .map_err(|err| (ErrorCategory::Storage, err))?,
    );
    if !resuming {
        save_piece_map(&storage, &map, directory, &job.id).await?;
    }
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

    let quadro = QuadroDePecas::new(
        (0..num_pieces)
            .map(|index| tamanho_peca(&layout, index))
            .collect(),
        &map,
        ORCAMENTO_BYTES,
        None,
    )
    .map_err(|err| (ErrorCategory::Internal, err))?;
    let n = std::thread::available_parallelism().map_or(1, usize::from);
    let peers_job = Arc::new(PeerJob {
        quadro: Mutex::new(quadro),
        storage: Arc::clone(&storage),
        layout: Arc::clone(&layout),
        hashes: hashes.clone(),
        semaforo: Arc::new(Semaphore::new(n)),
        global_limiter,
        job_limiter: RateLimiter::new(args.limit_rate),
        // BEP 27: no PEX on a private torrent.
        pex: !is_private,
    });
    let handshake = Handshake {
        info_hash: metainfo.info_hash,
        peer_id,
        supports_extension: true,
        supports_fast: true,
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
    let mut last_error = anyhow!("no peer completed the torrent");
    let mut banidos: HashSet<SocketAddr> = HashSet::new();
    let mut ips_banidos: HashSet<IpAddr> = HashSet::new();
    let mut downloaded = resumed_bytes;
    let mut checkpoint_bytes = downloaded;
    let mut last_checkpoint = tokio::time::Instant::now();
    // BEP 19: web seeds from `url-list`, through the protected HTTP client.
    let semeadores_web =
        crate::semeador_web::semeadores_validos(&metainfo.url_list, rede_protegida);
    let fetcher_web = HttpFetcher::new(
        Some(std::time::Duration::from_secs(10)),
        Some(std::time::Duration::from_secs(30)),
        None,
        rede_protegida,
    )
    .map_err(|err| (ErrorCategory::Internal, err))?;
    let mut next_id: AparelhoId = 0;
    let registrar_peca = |index, map: &mut TorrentPieceMap, downloaded: &mut u64| {
        map.set_piece(index);
        *downloaded += tamanho_peca(&layout, index);
        broker.emit(EventType::Progress {
            id: job.id.clone(),
            bytes_downloaded: *downloaded,
            bytes_total: Some(layout.total_length),
        });
    };
    for round in 0..3 {
        let round_request = TrackerRequest { ..request };
        let (tx, mut rx) = mpsc::unbounded_channel();
        let (eventos, mut eventos_rx) = mpsc::unbounded_channel();
        let mut aparelhos = JoinSet::new();
        let mut web = JoinSet::new();
        for base in &semeadores_web {
            next_id += 1;
            web.spawn(crate::semeador_web::semear_web(
                base.clone(),
                fetcher_web.clone(),
                next_id,
                Arc::clone(&peers_job),
                eventos.clone(),
            ));
        }
        let tentativas = Arc::new(Semaphore::new(MAX_TENTATIVAS));
        let mut interval = None;
        let mut seen = HashSet::new();
        let mut peers: VecDeque<_> = manual_peers
            .iter()
            .copied()
            .filter(|peer| !aparelho_banido(peer, &banidos, &ips_banidos) && seen.insert(*peer))
            .collect();
        let mut remaining = MAX_PEERS;
        let mut round_dht = dht;
        let mut tracker_closed = false;
        let completed = with_announces(&mut trackers, &round_request, tx, async {
            let result = async {
                loop {
                    let completo = { peers_job.quadro.lock().unwrap().completo() };
                    if completo && downloaded == layout.total_length {
                        return Ok(true);
                    }

                    // Reserve a connection slot while its handshake is in flight.
                    while aparelhos.len() < MAX_CONECTADOS && !peers.is_empty() {
                        let Ok(tentativa) = Arc::clone(&tentativas).try_acquire_owned() else {
                            break;
                        };
                        let peer = peers.pop_front().expect("queued peer");
                        next_id += 1;
                        let id = next_id;
                        let handshake = handshake.clone();
                        let peers_job = Arc::clone(&peers_job);
                        let eventos = eventos.clone();
                        aparelhos.spawn(async move {
                            let result = baixar_de_aparelho(peer, handshake, tentativa, id, peers_job, eventos).await;
                            (peer, result)
                        });
                    }

                    if tracker_closed && peers.is_empty() && aparelhos.is_empty() && web.is_empty() {
                        let Some(node) = round_dht.take() else {
                            return Ok(false);
                        };
                        let found = tokio::select! {
                            biased;
                            () = &mut watch => return Err(cancelled()),
                            peers = node.peers(handshake.info_hash, &metainfo.nodes,
                                &args.dht_bootstrap, args.job_dir.as_deref(), rede_protegida) => peers,
                        };
                        peers.extend(found.into_iter().filter(|peer| !aparelho_banido(peer, &banidos, &ips_banidos) && seen.insert(*peer)));
                        continue;
                    }

                    tokio::select! {
                        biased;
                        () = &mut watch => return Err(cancelled()),
                        () = tokio::time::sleep_until(last_checkpoint + CHECKPOINT_INTERVAL),
                            if downloaded != checkpoint_bytes => {
                            save_piece_map(&storage, &map, directory, &job.id).await?;
                            last_checkpoint = tokio::time::Instant::now();
                            checkpoint_bytes = downloaded;
                        }
                        Some(evento) = eventos_rx.recv() => {
                            match evento {
                                EventoAparelho::Gravou(index) => registrar_peca(index, &mut map, &mut downloaded),
                                EventoAparelho::Descobriu(novos) => enfileirar_descobertos(
                                    novos, &mut peers, &mut seen, &banidos, &ips_banidos, &mut remaining, rede_protegida,
                                ),
                                EventoAparelho::Conectou => {}
                            }
                        }
                        Some(resultado) = web.join_next() => {
                            match resultado {
                                Ok(Err(err)) => last_error = err,
                                Err(err) if !err.is_cancelled() => {
                                    return Err((ErrorCategory::Internal, err.into()));
                                }
                                _ => {}
                            }
                        }
                        Some(result) = aparelhos.join_next() => {
                            match result {
                                Ok((_, Ok(()))) => {}
                                Ok((_, Err(err))) if err.is::<tokio::task::JoinError>() => {
                                    return Err((ErrorCategory::Internal, err));
                                }
                                Ok((peer, Err(err))) => {
                                    if err.is::<AparelhoHostil>() && banidos.insert(peer) {
                                        let ip = peer.ip();
                                        // ponytail: varre os banidos por expulsão; indexar por IP se o enxame exigir.
                                        let portas: HashSet<_> = banidos.iter()
                                            .filter_map(|peer| (peer.ip() == ip).then_some(peer.port()))
                                            .collect();
                                        if portas.len() == PORTAS_PARA_BANIR_IP {
                                            ips_banidos.insert(ip);
                                            peers.retain(|peer| peer.ip() != ip);
                                        }
                                    }
                                    last_error = err;
                                }
                                Err(err) => return Err((ErrorCategory::Internal, err.into())),
                            }
                        }
                        response = rx.recv(), if !tracker_closed => {
                            match response {
                                Some(Ok(response)) => {
                                    let seconds = u64::from(response.interval.clamp(1, 120));
                                    // ponytail: one delay for the round; use per-tracker timers if tiers need independent retries.
                                    interval = Some(interval.unwrap_or(0).max(seconds));
                                    let before = peers.len();
                                    peers.extend(
                                        response.todos()
                                            .filter(|peer| !aparelho_banido(peer, &banidos, &ips_banidos) && seen.insert(*peer)).take(remaining),
                                    );
                                    remaining -= peers.len() - before;
                                }
                                Some(Err(err)) => last_error = err,
                                None => tracker_closed = true,
                            }
                        }
                    }
                }
            }.await;
            aparelhos.shutdown().await;
            web.shutdown().await;
            {
                let _drenado = peers_job.semaforo.acquire_many(n as u32).await
                    .expect("semaphore never closes");
            }
            while let Ok(evento) = eventos_rx.try_recv() {
                if let EventoAparelho::Gravou(index) = evento {
                    registrar_peca(index, &mut map, &mut downloaded);
                }
            }
            let result = if matches!(result, Ok(false)) && round < 2 && interval.is_some() {
                result
            } else {
                let saved = save_piece_map(&storage, &map, directory, &job.id).await;
                if cancel_flag.load(Ordering::Relaxed) { result } else { saved.and(result) }
            };
            let verified = bytes_no_mapa(&map, &layout);
            request.downloaded = verified - resumed_bytes;
            request.left = layout.total_length - verified;
            let event = match &result {
                Ok(true) => Some(TrackerEvent::Completed),
                Err(_) => Some(TrackerEvent::Stopped),
                Ok(false) if round == 2 || interval.is_none() => Some(TrackerEvent::Stopped),
                Ok(false) => None,
            };
            (result, event.map(|event| TrackerRequest { event, ..request }))
        }).await?;
        if completed {
            return Ok(());
        }
        let Some(seconds) = interval.filter(|_| round < 2) else {
            break;
        };
        tokio::select! {
            biased;
            () = &mut watch => {
                let _ = save_piece_map(&storage, &map, directory, &job.id).await;
                let stopped = TrackerRequest { event: TrackerEvent::Stopped, ..request };
                let _ = timeout(TIMEOUT, join_all(trackers.iter_mut().map(|tracker| tracker.announce(&stopped)))).await;
                return Err(cancelled());
            },
            () = tokio::time::sleep(Duration::from_secs(seconds)) => {}
        }
        request.event = TrackerEvent::None;
    }
    Err((ErrorCategory::Network, last_error))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn f16_pex_passa_pelo_filtro_banidos_repetidos_e_teto() {
        let novos: Vec<SocketAddr> = [
            "8.8.8.8:1",
            "8.8.8.8:1",
            "8.8.8.8:0",
            "192.168.0.2:1",
            "9.9.9.9:1",
            "7.7.7.7:1",
            "[2001:db8::1]:1",
            "1.1.1.1:1",
        ]
        .iter()
        .map(|s| s.parse().unwrap())
        .collect();
        let mut peers = VecDeque::new();
        let mut seen = HashSet::new();
        let banidos = HashSet::from(["9.9.9.9:1".parse().unwrap()]);
        let ips_banidos = HashSet::from(["7.7.7.7".parse().unwrap()]);
        let mut remaining = 2;
        enfileirar_descobertos(
            novos,
            &mut peers,
            &mut seen,
            &banidos,
            &ips_banidos,
            &mut remaining,
            true,
        );
        let esperado: Vec<SocketAddr> = vec![
            "8.8.8.8:1".parse().unwrap(),
            "[2001:db8::1]:1".parse().unwrap(),
        ];
        assert_eq!(Vec::from(peers), esperado);
        assert_eq!(
            remaining, 0,
            "teto de aparelhos respeitado: 1.1.1.1 ficou de fora"
        );
        println!(
            "PEX: repetido, porta 0, IP privado, banido e IP banido filtrados; teto respeitado"
        );
    }
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
