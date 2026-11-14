use crate::bencode::{BValue, DecodeLimits, decode, decode_prefix, encode};
use crate::metainfo::Info;
use crate::peer_transfer::PeerReader;
use crate::peer_wire::{Handshake, PeerMessage};
use anyhow::{Result, bail, ensure};
use std::collections::BTreeMap;
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

const LOCAL_METADATA_ID: u8 = 1;
const PIECE_SIZE: usize = 16 * 1024;
const MAX_METADATA_SIZE: i64 = 16 * 1024 * 1024;

pub struct MetadataTransfer;

impl MetadataTransfer {
    pub fn download(
        stream: &mut TcpStream,
        info_hash: &[u8; 20],
        local_peer_id: &[u8; 20],
        deadline: Instant,
        cancel_flag: &AtomicBool,
    ) -> Result<Vec<u8>> {
        let mut reader = PeerReader {
            stream,
            deadline,
            cancel_flag: Some(cancel_flag),
        };
        let prepare_write = |stream: &TcpStream| -> Result<()> {
            ensure!(!cancel_flag.load(Ordering::Relaxed), "Cancelled");
            let remaining = deadline.saturating_duration_since(Instant::now());
            ensure!(!remaining.is_zero(), "Metadata deadline expired");
            stream.set_write_timeout(Some(remaining))?;
            Ok(())
        };

        let hs = Handshake {
            info_hash: *info_hash,
            peer_id: *local_peer_id,
            supports_extension: true,
        };
        prepare_write(reader.stream)?;
        hs.write_to(reader.stream)?;

        let hs_reply = Handshake::read_from(&mut reader, info_hash, local_peer_id)?;
        ensure!(
            hs_reply.supports_extension,
            "Peer does not support extensions"
        );

        // Send extended handshake
        let mut m_dict = BTreeMap::new();
        m_dict.insert(
            b"ut_metadata".to_vec(),
            BValue::Int(i64::from(LOCAL_METADATA_ID)),
        );

        let mut ext_hs_dict = BTreeMap::new();
        ext_hs_dict.insert(b"m".to_vec(), BValue::Dict(m_dict));

        let payload = encode(&BValue::Dict(ext_hs_dict));
        prepare_write(reader.stream)?;
        PeerMessage::Extended(0, payload).write_to(reader.stream)?;

        // Wait for peer's extended handshake
        let hello = loop {
            if let PeerMessage::Extended(0, payload) = PeerMessage::read_from(&mut reader)? {
                break decode(&payload)?;
            }
        };
        let BValue::Dict(hello) = hello else {
            bail!("Invalid extended handshake from peer");
        };
        let Some(BValue::Int(size @ 1..=MAX_METADATA_SIZE)) =
            hello.get(b"metadata_size".as_slice())
        else {
            bail!("Invalid metadata_size: expected 1 byte to 16 MiB");
        };
        let metadata_size = usize::try_from(*size)?;
        let remote_ut_metadata_id = match hello.get(b"m".as_slice()) {
            Some(BValue::Dict(extensions)) => match extensions.get(b"ut_metadata".as_slice()) {
                Some(BValue::Int(id @ 1..=255)) => u8::try_from(*id)?,
                _ => bail!("Invalid remote ut_metadata id"),
            },
            _ => bail!("Missing peer extension map"),
        };

        let mut buffer = vec![0u8; metadata_size];

        for (index, piece) in buffer.chunks_mut(PIECE_SIZE).enumerate() {
            let mut req_dict = BTreeMap::new();
            let index = i64::try_from(index)?;
            req_dict.insert(b"msg_type".to_vec(), BValue::Int(0));
            req_dict.insert(b"piece".to_vec(), BValue::Int(index));
            let req_payload = encode(&BValue::Dict(req_dict));
            prepare_write(reader.stream)?;
            PeerMessage::Extended(remote_ut_metadata_id, req_payload).write_to(reader.stream)?;

            loop {
                let PeerMessage::Extended(id, payload) = PeerMessage::read_from(&mut reader)?
                else {
                    continue;
                };
                // BEP 10 uses the extension ID advertised by the receiver.
                if id != LOCAL_METADATA_ID {
                    continue;
                }
                let (header, prefix_len) = decode_prefix(&payload, DecodeLimits::default())?;
                let BValue::Dict(header) = header else {
                    bail!("Invalid metadata message");
                };
                match header.get(b"msg_type".as_slice()) {
                    Some(BValue::Int(1)) => {}
                    Some(BValue::Int(2)) => bail!("Peer rejected metadata request"),
                    Some(BValue::Int(_)) => continue,
                    _ => bail!("Invalid metadata message type"),
                }
                ensure!(
                    header.get(b"piece".as_slice()) == Some(&BValue::Int(index)),
                    "Unexpected metadata piece"
                );
                if let Some(total) = header.get(b"total_size".as_slice()) {
                    ensure!(total == &BValue::Int(*size), "Metadata total_size changed");
                }
                let data = &payload[prefix_len..];
                ensure!(data.len() == piece.len(), "Invalid metadata piece size");
                piece.copy_from_slice(data);
                break;
            }
        }

        let has_pieces = match Info::from_bytes(&buffer, info_hash)? {
            Info::SingleFile(info) => !info.pieces.is_empty(),
            Info::MultiFile(info) => !info.pieces.is_empty(),
        };
        ensure!(has_pieces, "torrent total length must be positive");
        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha1::{Digest, Sha1};
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_metadata_transfer_success() {
        let mut info = BTreeMap::new();
        info.insert(b"length".to_vec(), BValue::Int(20));
        info.insert(b"name".to_vec(), BValue::Bytes(b"test.txt".to_vec()));
        info.insert(b"piece length".to_vec(), BValue::Int(16384));
        info.insert(b"pieces".to_vec(), BValue::Bytes(vec![0; 20]));

        let info_bytes = encode(&BValue::Dict(info));
        let metadata_size = info_bytes.len();

        let mut hasher = Sha1::new();
        hasher.update(&info_bytes);
        let info_hash: [u8; 20] = hasher.finalize().into();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let local_peer_id = [1u8; 20];
        let remote_peer_id = [2u8; 20];

        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();

            // 1. Read handshake
            let hs = Handshake::read_from(&mut stream, &info_hash, &remote_peer_id).unwrap();
            assert!(hs.supports_extension);

            // 2. Write handshake
            let hs_reply = Handshake {
                info_hash,
                peer_id: remote_peer_id,
                supports_extension: true,
            };
            hs_reply.write_to(&mut stream).unwrap();

            // 3. Read client extended handshake
            let msg = PeerMessage::read_from(&mut stream).unwrap();
            if let PeerMessage::Extended(0, _payload) = msg {
                // Client sent its extended handshake
            } else {
                panic!("Expected extended handshake");
            }

            // 4. Send peer extended handshake
            let mut m_dict = BTreeMap::new();
            m_dict.insert(b"ut_metadata".to_vec(), BValue::Int(3));
            let mut ext_hs_dict = BTreeMap::new();
            ext_hs_dict.insert(b"m".to_vec(), BValue::Dict(m_dict));
            ext_hs_dict.insert(b"metadata_size".to_vec(), BValue::Int(metadata_size as i64));

            let payload = encode(&BValue::Dict(ext_hs_dict));
            PeerMessage::Extended(0, payload)
                .write_to(&mut stream)
                .unwrap();

            // 5. Read metadata piece request
            let msg = PeerMessage::read_from(&mut stream).unwrap();
            assert!(matches!(msg, PeerMessage::Extended(3, _)));

            // 6. Send metadata piece data
            let mut reply_dict = BTreeMap::new();
            reply_dict.insert(b"msg_type".to_vec(), BValue::Int(1)); // data
            reply_dict.insert(b"piece".to_vec(), BValue::Int(0));
            reply_dict.insert(b"total_size".to_vec(), BValue::Int(metadata_size as i64));

            let mut reply_payload = encode(&BValue::Dict(reply_dict));
            reply_payload.extend_from_slice(&info_bytes); // The dictionary is followed by the actual data

            PeerMessage::Extended(1, reply_payload)
                .write_to(&mut stream)
                .unwrap();
        });

        let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).unwrap();
        let downloaded = MetadataTransfer::download(
            &mut stream,
            &info_hash,
            &local_peer_id,
            Instant::now() + Duration::from_secs(5),
            &AtomicBool::new(false),
        )
        .unwrap();
        assert_eq!(<[u8; 20]>::from(Sha1::digest(&downloaded)), info_hash);

        handle.join().unwrap();
    }

    #[test]
    fn keep_alives_do_not_extend_metadata_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut client =
            TcpStream::connect_timeout(&listener.local_addr().unwrap(), Duration::from_secs(1))
                .unwrap();
        let (mut peer, _) = listener.accept().unwrap();
        peer.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        peer.set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let hash = [3; 20];
        let sender = thread::spawn(move || {
            Handshake::read_from(&mut peer, &hash, &[2; 20]).unwrap();
            Handshake {
                info_hash: hash,
                peer_id: [2; 20],
                supports_extension: true,
            }
            .write_to(&mut peer)
            .unwrap();
            assert!(matches!(
                PeerMessage::read_from(&mut peer).unwrap(),
                PeerMessage::Extended(0, _)
            ));
            let stop = Instant::now() + Duration::from_secs(3);
            while Instant::now() < stop {
                if PeerMessage::KeepAlive.write_to(&mut peer).is_err() {
                    break;
                }
                thread::sleep(Duration::from_millis(50));
            }
        });
        let start = Instant::now();
        let error = MetadataTransfer::download(
            &mut client,
            &hash,
            &[1; 20],
            start + Duration::from_secs(1),
            &AtomicBool::new(false),
        )
        .unwrap_err();
        let elapsed = start.elapsed();
        drop(client);
        sender.join().unwrap();
        assert!(
            elapsed < Duration::from_secs(2),
            "keep-alives held the connection for {elapsed:?}: {error:#}"
        );
        let kind = error.downcast_ref::<std::io::Error>().unwrap().kind();
        assert!(matches!(
            kind,
            std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
        ));
    }
}
