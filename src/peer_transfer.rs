#![allow(dead_code)]
use crate::metainfo::Info;
use crate::peer_wire::{PeerMessage, PeerState};
use crate::throttle::RateLimiter;
use crate::torrent_storage::{TorrentPieceMap, TorrentStorage};
use anyhow::{Result, anyhow};
use sha1::{Digest, Sha1};
use std::net::TcpStream;
use std::path::Path;
use std::time::{Duration, Instant};

const BLOCK_SIZE: u32 = 16 * 1024;
const MAX_PENDING_REQUESTS: usize = 5;
const PEER_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_INVALID_BLOCKS: usize = 32;

pub struct PeerTransfer<'a> {
    stream: &'a mut TcpStream,
    state: PeerState,
    storage: &'a TorrentStorage,
    map: &'a mut TorrentPieceMap,
    info: &'a Info,
    global_limiter: Option<&'a RateLimiter>,
    job_limiter: Option<&'a RateLimiter>,
    cancel_flag: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    event_broker: Option<&'a crate::events::EventBroker>,
    job_id: Option<String>,
    piece_map_destination: Option<(&'a Path, &'a str)>,
    invalid_blocks: usize,
}

impl<'a> PeerTransfer<'a> {
    pub fn new(
        stream: &'a mut TcpStream,
        storage: &'a TorrentStorage,
        map: &'a mut TorrentPieceMap,
        info: &'a Info,
        global_limiter: Option<&'a RateLimiter>,
        job_limiter: Option<&'a RateLimiter>,
        cancel_flag: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    ) -> Self {
        Self {
            stream,
            state: PeerState::default(),
            storage,
            map,
            info,
            global_limiter,
            job_limiter,
            cancel_flag,
            event_broker: None,
            job_id: None,
            piece_map_destination: None,
            invalid_blocks: 0,
        }
    }

    pub fn with_events(mut self, broker: &'a crate::events::EventBroker, job_id: String) -> Self {
        self.event_broker = Some(broker);
        self.job_id = Some(job_id);
        self
    }

    pub fn with_piece_map(mut self, directory: &'a Path, id: &'a str) -> Self {
        self.piece_map_destination = Some((directory, id));
        self
    }

    pub fn download(&mut self) -> Result<()> {
        let (num_pieces, piece_length, total_length) = match self.info {
            Info::SingleFile(s) => (s.pieces.len(), s.piece_length as u64, s.length as u64),
            Info::MultiFile(m) => {
                let total: usize = m.files.iter().map(|f| f.length).sum();
                (m.pieces.len(), m.piece_length as u64, total as u64)
            }
        };

        self.stream.set_read_timeout(Some(PEER_TIMEOUT))?;
        self.stream.set_write_timeout(Some(PEER_TIMEOUT))?;

        PeerMessage::Interested.write_to(self.stream)?;
        self.state.am_interested = true;

        let mut bad_pieces = 0;
        let mut downloaded: u64 = (0..num_pieces)
            .filter(|&index| self.map.has_piece(index))
            .map(|index| piece_length.min(total_length - index as u64 * piece_length))
            .sum();
        let mut last_block = Instant::now();
        let mut last_checkpoint = last_block;
        // ponytail: repeated chokes restart the whole piece. Keep partial blocks
        // across chokes if peers with short unchoke windows need support.
        let mut interrupted_pieces = 0;
        let mut next_piece = 0;

        loop {
            if downloaded == total_length {
                return Ok(());
            }
            self.check_progress(last_block)?;

            let Some(piece_idx) = (!self.state.peer_choking)
                .then(|| {
                    self.select_piece(next_piece..num_pieces)
                        .or_else(|| self.select_piece(0..next_piece))
                })
                .flatten()
            else {
                let msg = self.read_message(last_block)?;
                self.state.apply_message(&msg);
                if matches!(msg, PeerMessage::Piece { .. }) {
                    self.reject_block()?;
                }
                continue;
            };

            let piece_len = if piece_idx == num_pieces - 1 {
                total_length - (piece_idx as u64 * piece_length)
            } else {
                piece_length
            };

            let Some(data) = self.download_piece(piece_idx, piece_len as u32, &mut last_block)?
            else {
                interrupted_pieces += 1;
                if interrupted_pieces == 3 {
                    return Err(anyhow!("peer repeatedly choked during piece download"));
                }
                continue;
            };
            let expected_hash = match self.info {
                Info::SingleFile(s) => &s.pieces[piece_idx],
                Info::MultiFile(m) => &m.pieces[piece_idx],
            };
            let hash: [u8; 20] = Sha1::digest(&data).into();
            if &hash != expected_hash {
                bad_pieces += 1;
                if bad_pieces == 3 {
                    return Err(anyhow!("peer sent bad pieces"));
                }
                continue;
            }
            self.storage.write_piece(piece_idx, &data)?;
            interrupted_pieces = 0;
            self.map.set_piece(piece_idx);
            next_piece = piece_idx + 1;
            if let Some((directory, id)) = self.piece_map_destination
                && last_checkpoint.elapsed() >= Duration::from_secs(1)
            {
                self.storage.sync_all()?;
                self.map.save(directory, id)?;
                last_checkpoint = Instant::now();
            }
            last_block = Instant::now();
            downloaded += piece_len;
            if let Some(broker) = self.event_broker
                && let Some(id) = &self.job_id
            {
                broker.emit(crate::events::EventType::Progress {
                    id: id.clone(),
                    bytes_downloaded: downloaded,
                    bytes_total: Some(total_length),
                });
            }
        }
    }

    fn check_progress(&self, last_block: Instant) -> Result<()> {
        if self
            .cancel_flag
            .as_ref()
            .is_some_and(|flag| flag.load(std::sync::atomic::Ordering::Relaxed))
        {
            return Err(anyhow!("Cancelled"));
        }
        if last_block.elapsed() >= PEER_TIMEOUT {
            return Err(anyhow!("peer stopped sending requested blocks"));
        }
        Ok(())
    }

    fn reject_block(&mut self) -> Result<()> {
        self.invalid_blocks += 1;
        if self.invalid_blocks >= MAX_INVALID_BLOCKS {
            return Err(anyhow!("peer sent invalid blocks"));
        }
        Ok(())
    }

    fn read_message(&mut self, last_block: Instant) -> Result<PeerMessage> {
        let message = PeerMessage::read_from(&mut PeerReader {
            stream: self.stream,
            deadline: last_block + PEER_TIMEOUT,
            cancel_flag: self.cancel_flag.as_deref(),
        })?;
        if let PeerMessage::Have(index) = message
            && (index as usize) < self.map.num_pieces
        {
            let index = index as usize;
            let bitfield = self
                .state
                .bitfield
                .get_or_insert_with(|| vec![0; self.map.num_pieces.div_ceil(8)]);
            bitfield.resize(bitfield.len().max(index / 8 + 1), 0);
            bitfield[index / 8] |= 0x80 >> (index % 8);
        }
        Ok(message)
    }

    pub fn seed(&mut self) -> Result<()> {
        self.stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        self.stream
            .set_write_timeout(Some(Duration::from_secs(5)))?;

        PeerMessage::Unchoke.write_to(self.stream)?;
        self.state.am_choking = false;

        loop {
            if let Some(flag) = &self.cancel_flag
                && flag.load(std::sync::atomic::Ordering::Relaxed)
            {
                return Ok(());
            }

            match PeerMessage::read_from(self.stream) {
                Ok(msg) => {
                    self.state.apply_message(&msg);
                    if let PeerMessage::Request {
                        index,
                        begin,
                        length,
                    } = msg
                        && !self.state.am_choking
                    {
                        let _ = self.serve_request(index, begin, length);
                    }
                }
                Err(e) => {
                    if e.kind() != std::io::ErrorKind::TimedOut
                        && e.kind() != std::io::ErrorKind::WouldBlock
                    {
                        return Err(e.into());
                    }
                }
            }
        }
    }

    fn serve_request(&mut self, index: u32, begin: u32, length: u32) -> Result<()> {
        let (num_pieces, piece_length, total_length) = match self.info {
            Info::SingleFile(s) => (s.pieces.len(), s.piece_length as u64, s.length as u64),
            Info::MultiFile(m) => {
                let total: usize = m.files.iter().map(|f| f.length).sum();
                (m.pieces.len(), m.piece_length as u64, total as u64)
            }
        };

        if index as usize >= num_pieces {
            return Ok(());
        }

        let piece_len = if index as usize == num_pieces - 1 {
            (total_length - (index as u64 * piece_length)) as u32
        } else {
            piece_length as u32
        };

        if begin > piece_len || length > piece_len || begin + length > piece_len {
            return Ok(());
        }

        // Limit maximum request size to 128KB to prevent memory exhaustion
        if length > 128 * 1024 {
            return Ok(());
        }

        if !self.map.has_piece(index as usize) {
            return Ok(());
        }

        let mut buf = vec![0u8; piece_len as usize];
        if self.storage.read_piece(index as usize, &mut buf).is_err() {
            return Ok(());
        }

        let block = buf[begin as usize..(begin + length) as usize].to_vec();

        if let Some(gl) = self.global_limiter {
            gl.wait_for_sync(length as u64);
        }
        if let Some(jl) = self.job_limiter {
            jl.wait_for_sync(length as u64);
        }

        PeerMessage::Piece {
            index,
            begin,
            block,
        }
        .write_to(self.stream)?;
        Ok(())
    }

    fn select_piece(&self, candidates: std::ops::Range<usize>) -> Option<usize> {
        for i in candidates {
            if !self.map.has_piece(i) {
                if let Some(bf) = &self.state.bitfield {
                    let byte_idx = i / 8;
                    let bit_idx = 7 - (i % 8);
                    if byte_idx < bf.len() && (bf[byte_idx] & (1 << bit_idx)) != 0 {
                        return Some(i);
                    }
                } else {
                    return Some(i);
                }
            }
        }
        None
    }

    fn download_piece(
        &mut self,
        piece_idx: usize,
        piece_len: u32,
        last_block: &mut Instant,
    ) -> Result<Option<Vec<u8>>> {
        let num_blocks = piece_len.div_ceil(BLOCK_SIZE);
        let mut buffer = vec![0u8; piece_len as usize];
        let mut blocks_requested = vec![false; num_blocks as usize];
        let mut blocks_received = vec![false; num_blocks as usize];
        let mut pending = 0;

        loop {
            if blocks_received.iter().all(|&b| b) {
                return Ok(Some(buffer));
            }
            self.check_progress(*last_block)?;

            for i in 0..num_blocks {
                if pending >= MAX_PENDING_REQUESTS {
                    break;
                }
                if !blocks_requested[i as usize] && !blocks_received[i as usize] {
                    let begin = i * BLOCK_SIZE;
                    let length = if i == num_blocks - 1 {
                        piece_len - begin
                    } else {
                        BLOCK_SIZE
                    };
                    PeerMessage::Request {
                        index: piece_idx as u32,
                        begin,
                        length,
                    }
                    .write_to(self.stream)?;
                    blocks_requested[i as usize] = true;
                    pending += 1;
                }
            }

            let msg = self.read_message(*last_block)?;
            self.state.apply_message(&msg);

            match msg {
                PeerMessage::Piece {
                    index,
                    begin,
                    block,
                } => {
                    let block_idx = (begin / BLOCK_SIZE) as usize;
                    if index != piece_idx as u32
                        || !begin.is_multiple_of(BLOCK_SIZE)
                        || block_idx >= num_blocks as usize
                        || !blocks_requested[block_idx]
                        || blocks_received[block_idx]
                        || block.len() != (piece_len - begin).min(BLOCK_SIZE) as usize
                    {
                        self.reject_block()?;
                        continue;
                    }
                    let end = begin as usize + block.len();
                    buffer[begin as usize..end].copy_from_slice(&block);
                    blocks_received[block_idx] = true;
                    pending -= 1;
                    if let Some(limiter) = self.global_limiter {
                        limiter.wait_for_sync(block.len() as u64);
                    }
                    if let Some(limiter) = self.job_limiter {
                        limiter.wait_for_sync(block.len() as u64);
                    }
                    *last_block = Instant::now();
                }
                PeerMessage::Choke => return Ok(None),
                _ => {}
            }
        }
    }
}

pub(crate) struct PeerReader<'a> {
    pub stream: &'a mut TcpStream,
    pub deadline: Instant,
    pub cancel_flag: Option<&'a std::sync::atomic::AtomicBool>,
}

impl std::io::Read for PeerReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self
            .cancel_flag
            .is_some_and(|flag| flag.load(std::sync::atomic::Ordering::Relaxed))
        {
            return Err(std::io::Error::other("Cancelled"));
        }
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(std::io::ErrorKind::TimedOut.into());
        }
        self.stream.set_read_timeout(Some(remaining))?;
        std::io::Read::read(self.stream, buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metainfo::{FileInfo, MultiFileInfo};
    use sha1::{Digest, Sha1};
    use std::fs;
    use std::net::TcpListener;
    use std::thread;

    fn make_info_multi() -> Info {
        // Two files: 20 bytes and 15 bytes. Total 35 bytes.
        // Piece length: 16 bytes.
        // Pieces: 3 pieces. (16, 16, 3)
        let mut data = vec![];
        for i in 0..35 {
            data.push(i as u8);
        }

        let p1 = &data[0..16];
        let p2 = &data[16..32];
        let p3 = &data[32..35];

        let h1: [u8; 20] = Sha1::digest(p1).into();
        let h2: [u8; 20] = Sha1::digest(p2).into();
        let h3: [u8; 20] = Sha1::digest(p3).into();

        Info::MultiFile(MultiFileInfo {
            name: "test_dir".to_string(),
            piece_length: 16,
            pieces: vec![h1, h2, h3],
            files: vec![
                FileInfo {
                    length: 20,
                    path: vec!["f1.bin".to_string()],
                },
                FileInfo {
                    length: 15,
                    path: vec!["f2.bin".to_string()],
                },
            ],
        })
    }

    #[test]
    fn test_peer_transfer_success_and_resume() {
        let dir = std::env::temp_dir().join("chorus_pt_test");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let info = make_info_multi();
        let storage = TorrentStorage::new(&dir, &info).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();

            let msg = PeerMessage::read_from(&mut stream).unwrap();
            assert_eq!(msg, PeerMessage::Interested);

            // Unchoke and send bitfield saying we have everything
            PeerMessage::Bitfield(vec![0b11100000])
                .write_to(&mut stream)
                .unwrap();
            PeerMessage::Unchoke.write_to(&mut stream).unwrap();

            // Wait for requests for Piece 0
            let req1 = PeerMessage::read_from(&mut stream).unwrap();
            if let PeerMessage::Request {
                index,
                begin,
                length: _,
            } = req1
            {
                assert_eq!(index, 0);
                let mut data = vec![];
                for i in 0..16 {
                    data.push(i as u8);
                }
                PeerMessage::Piece {
                    index,
                    begin,
                    block: data,
                }
                .write_to(&mut stream)
                .unwrap();
            }

            // Simulate disconnect before piece 1 finishes (or kill simulation)
        });

        let mut stream = TcpStream::connect(addr).unwrap();
        let mut map = TorrentPieceMap::new(3);

        let mut pt = PeerTransfer::new(&mut stream, &storage, &mut map, &info, None, None, None);
        // It will fail because the peer disconnects after piece 0
        let _ = pt.download();

        assert!(map.has_piece(0));
        assert!(!map.has_piece(1));
        assert!(!map.has_piece(2));

        handle.join().unwrap();

        // Now test resume!
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let handle2 = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let msg = PeerMessage::read_from(&mut stream).unwrap();
            assert_eq!(msg, PeerMessage::Interested);

            PeerMessage::Bitfield(vec![0b11100000])
                .write_to(&mut stream)
                .unwrap();
            PeerMessage::Unchoke.write_to(&mut stream).unwrap();

            // Client should request piece 1 next since 0 is done
            let req2 = PeerMessage::read_from(&mut stream).unwrap();
            if let PeerMessage::Request {
                index,
                begin,
                length: _,
            } = req2
            {
                assert_eq!(index, 1);
                let mut data = vec![];
                for i in 16..32 {
                    data.push(i as u8);
                }
                PeerMessage::Piece {
                    index,
                    begin,
                    block: data,
                }
                .write_to(&mut stream)
                .unwrap();
            }

            // Then piece 2
            let req3 = PeerMessage::read_from(&mut stream).unwrap();
            if let PeerMessage::Request {
                index,
                begin,
                length: _,
            } = req3
            {
                assert_eq!(index, 2);
                let mut data = vec![];
                for i in 32..35 {
                    data.push(i as u8);
                }
                PeerMessage::Piece {
                    index,
                    begin,
                    block: data,
                }
                .write_to(&mut stream)
                .unwrap();
            }
        });

        let mut stream2 = TcpStream::connect(addr).unwrap();
        // Keep the map from before
        let mut pt2 = PeerTransfer::new(&mut stream2, &storage, &mut map, &info, None, None, None);
        pt2.download().unwrap();

        assert!(map.has_piece(0));
        assert!(map.has_piece(1));
        assert!(map.has_piece(2));

        handle2.join().unwrap();
    }

    #[test]
    fn test_peer_transfer_seeding() {
        let dir = std::env::temp_dir().join("chorus_pt_seed_test");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let info = make_info_multi();
        let storage = TorrentStorage::new(&dir, &info).unwrap();

        let expected_hash_0 = match &info {
            Info::MultiFile(m) => m.pieces[0],
            _ => panic!("Expected MultiFile"),
        };

        let mut data_0 = vec![];
        for i in 0..16 {
            data_0.push(i as u8);
        }
        storage
            .write_piece_and_validate(0, &data_0, &expected_hash_0)
            .unwrap();

        let mut map = TorrentPieceMap::new(3);
        map.set_piece(0); // We only have piece 0

        let limiter = crate::throttle::RateLimiter::new(1024); // 1KB/s

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut pt = PeerTransfer::new(
                &mut stream,
                &storage,
                &mut map,
                &info,
                Some(&limiter),
                None,
                None,
            );

            // Should exit because we force client disconnect or we time out, but we just want to run it
            let _ = pt.seed();
        });

        let mut stream = TcpStream::connect(addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();

        let msg = PeerMessage::read_from(&mut stream).unwrap();
        assert_eq!(msg, PeerMessage::Unchoke);

        PeerMessage::Request {
            index: 0,
            begin: 0,
            length: 16,
        }
        .write_to(&mut stream)
        .unwrap();

        let msg = PeerMessage::read_from(&mut stream).unwrap();
        if let PeerMessage::Piece {
            index,
            begin,
            block,
        } = msg
        {
            assert_eq!(index, 0);
            assert_eq!(begin, 0);
            assert_eq!(block.len(), 16);
            assert_eq!(block, data_0);
        } else {
            panic!("Expected piece");
        }

        // Request a piece they don't have
        PeerMessage::Request {
            index: 1,
            begin: 0,
            length: 16,
        }
        .write_to(&mut stream)
        .unwrap();

        // Should timeout because they don't respond
        let res = PeerMessage::read_from(&mut stream);
        assert!(res.is_err());

        drop(stream);
        let _ = handle.join();
    }

    #[test]
    fn test_peer_transfer_full_leech() {
        let dir_seed = std::env::temp_dir().join("chorus_pt_leech_seed");
        let dir_leech = std::env::temp_dir().join("chorus_pt_leech_client");
        let _ = fs::remove_dir_all(&dir_seed);
        let _ = fs::remove_dir_all(&dir_leech);
        fs::create_dir_all(&dir_seed).unwrap();
        fs::create_dir_all(&dir_leech).unwrap();

        let info = make_info_multi();
        let (num_pieces, _piece_len, _total_len) = match &info {
            Info::MultiFile(m) => (m.pieces.len(), m.piece_length, 35),
            _ => panic!("Expected MultiFile"),
        };

        // Initialize Seeder storage with all pieces
        let storage_seed = TorrentStorage::new(&dir_seed, &info).unwrap();
        let mut map_seed = TorrentPieceMap::new(num_pieces);
        let mut data = vec![];
        for i in 0..35 {
            data.push(i as u8);
        }
        let pieces = match &info {
            Info::MultiFile(m) => &m.pieces,
            _ => panic!("Expected MultiFile"),
        };
        storage_seed
            .write_piece_and_validate(0, &data[0..16], &pieces[0])
            .unwrap();
        storage_seed
            .write_piece_and_validate(1, &data[16..32], &pieces[1])
            .unwrap();
        storage_seed
            .write_piece_and_validate(2, &data[32..35], &pieces[2])
            .unwrap();
        map_seed.set_piece(0);
        map_seed.set_piece(1);
        map_seed.set_piece(2);

        let limiter = crate::throttle::RateLimiter::new(10 * 1024); // 10KB/s
        let cancel_flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cancel_clone = cancel_flag.clone();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let info_seed = info.clone();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut pt = PeerTransfer::new(
                &mut stream,
                &storage_seed,
                &mut map_seed,
                &info_seed,
                Some(&limiter),
                None,
                Some(cancel_clone),
            );
            // Simulate the seed loop, which should terminate when cancel_flag is true
            let _ = pt.seed();
        });

        // Initialize Leecher storage with 0 pieces
        let storage_leech = TorrentStorage::new(&dir_leech, &info).unwrap();
        let mut map_leech = TorrentPieceMap::new(num_pieces);
        let mut stream_leech = TcpStream::connect(addr).unwrap();
        let mut pt_leech = PeerTransfer::new(
            &mut stream_leech,
            &storage_leech,
            &mut map_leech,
            &info,
            None,
            None,
            None,
        );

        // Leecher downloads the whole file
        pt_leech.download().unwrap();

        // Validate Leecher got everything
        assert!(map_leech.has_piece(0));
        assert!(map_leech.has_piece(1));
        assert!(map_leech.has_piece(2));

        // Signal seeder to stop
        cancel_flag.store(true, std::sync::atomic::Ordering::Relaxed);

        let _ = handle.join();
        let _ = fs::remove_dir_all(&dir_seed);
        let _ = fs::remove_dir_all(&dir_leech);
    }
}
