#![allow(dead_code)]

use std::io::{Error, ErrorKind, Read, Write};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const BITTORRENT_PROTOCOL: &[u8; 19] = b"BitTorrent protocol";
pub const MAX_MESSAGE_SIZE: u32 = 1024 * 1024; // 1MB, prevents malicious huge lengths

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handshake {
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub supports_extension: bool,
    /// BEP 6 (Fast extension): reserved[7] & 0x04.
    pub supports_fast: bool,
}

impl Handshake {
    pub fn write_to(&self, stream: &mut impl Write) -> std::io::Result<()> {
        stream.write_all(&self.bytes())
    }

    pub async fn write_async(&self, stream: &mut (impl AsyncWrite + Unpin)) -> std::io::Result<()> {
        stream.write_all(&self.bytes()).await
    }

    fn bytes(&self) -> [u8; 68] {
        let mut buf = [0u8; 68];
        buf[0] = 19;
        buf[1..20].copy_from_slice(BITTORRENT_PROTOCOL);
        buf[20..28].fill(0); // reserved
        if self.supports_extension {
            buf[25] |= 0x10;
        }
        if self.supports_fast {
            buf[27] |= 0x04;
        }
        buf[28..48].copy_from_slice(&self.info_hash);
        buf[48..68].copy_from_slice(&self.peer_id);
        buf
    }

    pub fn read_from(
        stream: &mut impl Read,
        expected_info_hash: &[u8; 20],
        local_peer_id: &[u8; 20],
    ) -> std::io::Result<Self> {
        let mut buf = [0u8; 68];
        stream.read_exact(&mut buf)?;
        Self::decode(&buf, expected_info_hash, local_peer_id)
    }

    pub async fn read_async(
        stream: &mut (impl AsyncRead + Unpin),
        expected_info_hash: &[u8; 20],
        local_peer_id: &[u8; 20],
    ) -> std::io::Result<Self> {
        let mut buf = [0u8; 68];
        read_exact_async(stream, &mut buf).await?;
        Self::decode(&buf, expected_info_hash, local_peer_id)
    }

    fn decode(
        buf: &[u8; 68],
        expected_info_hash: &[u8; 20],
        local_peer_id: &[u8; 20],
    ) -> std::io::Result<Self> {
        if buf[0] != 19 || &buf[1..20] != BITTORRENT_PROTOCOL {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Invalid protocol string",
            ));
        }

        let mut info_hash = [0u8; 20];
        info_hash.copy_from_slice(&buf[28..48]);

        if &info_hash != expected_info_hash {
            return Err(Error::new(ErrorKind::InvalidData, "Info-hash mismatch"));
        }

        let mut peer_id = [0u8; 20];
        peer_id.copy_from_slice(&buf[48..68]);

        if &peer_id == local_peer_id {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Self-connection rejected",
            ));
        }

        let supports_extension = (buf[25] & 0x10) != 0;
        let supports_fast = (buf[27] & 0x04) != 0;

        Ok(Handshake {
            info_hash,
            peer_id,
            supports_extension,
            supports_fast,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerMessage {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    Bitfield(Vec<u8>),
    Request {
        index: u32,
        begin: u32,
        length: u32,
    },
    Piece {
        index: u32,
        begin: u32,
        block: Vec<u8>,
    },
    Cancel {
        index: u32,
        begin: u32,
        length: u32,
    },
    Port(u16),
    Extended(u8, Vec<u8>),
    // BEP 6 (Fast extension). Only valid when both sides set the reserved bit.
    SuggestPiece(u32),
    HaveAll,
    HaveNone,
    RejectRequest {
        index: u32,
        begin: u32,
        length: u32,
    },
    AllowedFast(u32),
}

impl PeerMessage {
    pub fn read_from(stream: &mut impl Read) -> std::io::Result<Self> {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf)?;
        let mut msg_buf = vec![0; Self::message_length(len_buf)?];
        if !msg_buf.is_empty() {
            stream.read_exact(&mut msg_buf)?;
        }
        Self::decode(&msg_buf)
    }

    pub async fn read_async(stream: &mut (impl AsyncRead + Unpin)) -> std::io::Result<Self> {
        let mut len_buf = [0u8; 4];
        read_exact_async(stream, &mut len_buf).await?;
        let mut msg_buf = vec![0; Self::message_length(len_buf)?];
        read_exact_async(stream, &mut msg_buf).await?;
        Self::decode(&msg_buf)
    }

    fn message_length(len_buf: [u8; 4]) -> std::io::Result<usize> {
        let len = u32::from_be_bytes(len_buf);
        if len > MAX_MESSAGE_SIZE {
            return Err(Error::new(ErrorKind::InvalidData, "Message too large"));
        }
        Ok(len as usize)
    }

    fn decode(msg_buf: &[u8]) -> std::io::Result<Self> {
        let Some((&id, payload)) = msg_buf.split_first() else {
            return Ok(PeerMessage::KeepAlive);
        };
        match id {
            0 => Ok(PeerMessage::Choke),
            1 => Ok(PeerMessage::Unchoke),
            2 => Ok(PeerMessage::Interested),
            3 => Ok(PeerMessage::NotInterested),
            4 => {
                if payload.len() != 4 {
                    return Err(Error::new(ErrorKind::InvalidData, "Invalid Have payload"));
                }
                let index = u32::from_be_bytes(payload.try_into().unwrap());
                Ok(PeerMessage::Have(index))
            }
            5 => Ok(PeerMessage::Bitfield(payload.to_vec())),
            6 => {
                if payload.len() != 12 {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        "Invalid Request payload",
                    ));
                }
                let index = u32::from_be_bytes(payload[0..4].try_into().unwrap());
                let begin = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                let length = u32::from_be_bytes(payload[8..12].try_into().unwrap());
                Ok(PeerMessage::Request {
                    index,
                    begin,
                    length,
                })
            }
            7 => {
                if payload.len() < 8 {
                    return Err(Error::new(ErrorKind::InvalidData, "Invalid Piece payload"));
                }
                let index = u32::from_be_bytes(payload[0..4].try_into().unwrap());
                let begin = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                let block = payload[8..].to_vec();
                Ok(PeerMessage::Piece {
                    index,
                    begin,
                    block,
                })
            }
            8 => {
                if payload.len() != 12 {
                    return Err(Error::new(ErrorKind::InvalidData, "Invalid Cancel payload"));
                }
                let index = u32::from_be_bytes(payload[0..4].try_into().unwrap());
                let begin = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                let length = u32::from_be_bytes(payload[8..12].try_into().unwrap());
                Ok(PeerMessage::Cancel {
                    index,
                    begin,
                    length,
                })
            }
            9 => {
                if payload.len() != 2 {
                    return Err(Error::new(ErrorKind::InvalidData, "Invalid Port payload"));
                }
                let port = u16::from_be_bytes(payload.try_into().unwrap());
                Ok(PeerMessage::Port(port))
            }
            13 | 17 => {
                if payload.len() != 4 {
                    return Err(Error::new(ErrorKind::InvalidData, "Invalid Fast payload"));
                }
                let index = u32::from_be_bytes(payload.try_into().unwrap());
                Ok(if id == 13 {
                    PeerMessage::SuggestPiece(index)
                } else {
                    PeerMessage::AllowedFast(index)
                })
            }
            14 | 15 => {
                if !payload.is_empty() {
                    return Err(Error::new(ErrorKind::InvalidData, "Invalid Fast payload"));
                }
                Ok(if id == 14 {
                    PeerMessage::HaveAll
                } else {
                    PeerMessage::HaveNone
                })
            }
            16 => {
                if payload.len() != 12 {
                    return Err(Error::new(ErrorKind::InvalidData, "Invalid Reject payload"));
                }
                Ok(PeerMessage::RejectRequest {
                    index: u32::from_be_bytes(payload[0..4].try_into().unwrap()),
                    begin: u32::from_be_bytes(payload[4..8].try_into().unwrap()),
                    length: u32::from_be_bytes(payload[8..12].try_into().unwrap()),
                })
            }
            20 => {
                if payload.is_empty() {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        "Invalid Extended payload",
                    ));
                }
                let ext_id = payload[0];
                let ext_payload = payload[1..].to_vec();
                Ok(PeerMessage::Extended(ext_id, ext_payload))
            }
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                format!("Unknown message id: {}", id),
            )),
        }
    }

    pub fn write_to(&self, stream: &mut impl Write) -> std::io::Result<()> {
        match self {
            PeerMessage::KeepAlive => stream.write_all(&[0, 0, 0, 0]),
            PeerMessage::Choke => stream.write_all(&[0, 0, 0, 1, 0]),
            PeerMessage::Interested => stream.write_all(&[0, 0, 0, 1, 2]),
            PeerMessage::NotInterested => stream.write_all(&[0, 0, 0, 1, 3]),
            PeerMessage::Have(index) => {
                let mut buf = vec![0, 0, 0, 5, 4];
                buf.extend_from_slice(&index.to_be_bytes());
                stream.write_all(&buf)
            }
            PeerMessage::Bitfield(bitfield) => {
                let len = 1 + bitfield.len() as u32;
                let mut buf = len.to_be_bytes().to_vec();
                buf.push(5);
                buf.extend_from_slice(bitfield);
                stream.write_all(&buf)
            }
            PeerMessage::Request {
                index,
                begin,
                length,
            } => {
                let mut buf = vec![0, 0, 0, 13, 6];
                buf.extend_from_slice(&index.to_be_bytes());
                buf.extend_from_slice(&begin.to_be_bytes());
                buf.extend_from_slice(&length.to_be_bytes());
                stream.write_all(&buf)
            }
            #[cfg(test)]
            PeerMessage::Piece {
                index,
                begin,
                block,
            } => {
                let len = 9 + block.len() as u32;
                let mut buf = len.to_be_bytes().to_vec();
                buf.push(7);
                buf.extend_from_slice(&index.to_be_bytes());
                buf.extend_from_slice(&begin.to_be_bytes());
                buf.extend_from_slice(block);
                stream.write_all(&buf)
            }
            PeerMessage::Cancel {
                index,
                begin,
                length,
            } => {
                let mut buf = vec![0, 0, 0, 13, 8];
                buf.extend_from_slice(&index.to_be_bytes());
                buf.extend_from_slice(&begin.to_be_bytes());
                buf.extend_from_slice(&length.to_be_bytes());
                stream.write_all(&buf)
            }
            PeerMessage::Port(port) => {
                let mut buf = vec![0, 0, 0, 3, 9];
                buf.extend_from_slice(&port.to_be_bytes());
                stream.write_all(&buf)
            }
            PeerMessage::Extended(ext_id, ext_payload) => {
                let len = 2 + ext_payload.len() as u32;
                let mut buf = len.to_be_bytes().to_vec();
                buf.push(20);
                buf.push(*ext_id);
                buf.extend_from_slice(ext_payload);
                stream.write_all(&buf)
            }
            PeerMessage::HaveAll => stream.write_all(&[0, 0, 0, 1, 14]),
            PeerMessage::HaveNone => stream.write_all(&[0, 0, 0, 1, 15]),
            PeerMessage::SuggestPiece(index) | PeerMessage::AllowedFast(index) => {
                let id = if matches!(self, PeerMessage::SuggestPiece(_)) {
                    13
                } else {
                    17
                };
                let mut buf = vec![0, 0, 0, 5, id];
                buf.extend_from_slice(&index.to_be_bytes());
                stream.write_all(&buf)
            }
            PeerMessage::RejectRequest {
                index,
                begin,
                length,
            } => {
                let mut buf = vec![0, 0, 0, 13, 16];
                buf.extend_from_slice(&index.to_be_bytes());
                buf.extend_from_slice(&begin.to_be_bytes());
                buf.extend_from_slice(&length.to_be_bytes());
                stream.write_all(&buf)
            }
            _ => Err(Error::new(
                ErrorKind::InvalidInput,
                "Message cannot be sent",
            )),
        }
    }

    pub async fn write_async(&self, stream: &mut (impl AsyncWrite + Unpin)) -> std::io::Result<()> {
        let mut bytes = Vec::new();
        self.write_to(&mut bytes)?;
        stream.write_all(&bytes).await
    }
}

async fn read_exact_async(
    stream: &mut (impl AsyncRead + Unpin),
    buf: &mut [u8],
) -> std::io::Result<()> {
    stream.read_exact(buf).await.map(|_| ()).map_err(|err| {
        if err.kind() == ErrorKind::UnexpectedEof {
            Error::new(ErrorKind::UnexpectedEof, "failed to fill whole buffer")
        } else {
            err
        }
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerState {
    pub am_choking: bool,
    pub am_interested: bool,
    pub peer_choking: bool,
    pub peer_interested: bool,
    pub bitfield: Option<Vec<u8>>,
}

impl Default for PeerState {
    fn default() -> Self {
        Self {
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            bitfield: None,
        }
    }
}

impl PeerState {
    pub fn apply_message(&mut self, msg: &PeerMessage) {
        match msg {
            PeerMessage::Choke => self.peer_choking = true,
            PeerMessage::Unchoke => self.peer_choking = false,
            PeerMessage::Interested => self.peer_interested = true,
            PeerMessage::NotInterested => self.peer_interested = false,
            PeerMessage::Bitfield(b) if self.bitfield.is_none() => {
                self.bitfield = Some(b.clone());
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{TcpListener, TcpStream};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_handshake_and_messages() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let expected_info_hash = [1u8; 20];
        let local_peer_id = [2u8; 20];
        let remote_peer_id = [3u8; 20];

        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(1)))
                .unwrap();

            // Server reads handshake
            let hs =
                Handshake::read_from(&mut stream, &expected_info_hash, &remote_peer_id).unwrap();
            assert_eq!(hs.peer_id, local_peer_id);

            // Server writes handshake
            let hs_reply = Handshake {
                info_hash: expected_info_hash,
                peer_id: remote_peer_id,
                supports_extension: false,
                supports_fast: false,
            };
            hs_reply.write_to(&mut stream).unwrap();

            // Server reads a message
            let msg = PeerMessage::read_from(&mut stream).unwrap();
            assert_eq!(msg, PeerMessage::Interested);

            // The fake peer lets the client send requests.
            stream.write_all(&[0, 0, 0, 1, 1]).unwrap();
        });

        // Client
        let mut stream = TcpStream::connect(addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(1)))
            .unwrap();

        let hs_client = Handshake {
            info_hash: expected_info_hash,
            peer_id: local_peer_id,
            supports_extension: false,
            supports_fast: false,
        };
        hs_client.write_to(&mut stream).unwrap();

        let hs_resp =
            Handshake::read_from(&mut stream, &expected_info_hash, &local_peer_id).unwrap();
        assert_eq!(hs_resp.peer_id, remote_peer_id);

        PeerMessage::Interested.write_to(&mut stream).unwrap();

        let msg = PeerMessage::read_from(&mut stream).unwrap();
        assert_eq!(msg, PeerMessage::Unchoke);

        let mut state = PeerState::default();
        state.apply_message(&msg);
        assert!(!state.peer_choking);

        handle.join().unwrap();
    }

    #[test]
    fn test_handshake_invalid_info_hash() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();

            let mut buf = [0u8; 68];
            buf[0] = 19;
            buf[1..20].copy_from_slice(BITTORRENT_PROTOCOL);
            buf[20..28].fill(0);
            buf[28..48].copy_from_slice(&[9u8; 20]); // Wrong info_hash
            buf[48..68].copy_from_slice(&[2u8; 20]);
            stream.write_all(&buf).unwrap();
        });

        let mut stream = TcpStream::connect(addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();

        let res = Handshake::read_from(&mut stream, &[1u8; 20], &[10u8; 20]);
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().to_string(), "Info-hash mismatch");

        handle.join().unwrap();
    }

    #[test]
    fn test_reject_self_connection() {
        let expected_info_hash = [1u8; 20];
        let my_peer_id = [2u8; 20];
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let hs = Handshake {
                info_hash: expected_info_hash,
                peer_id: my_peer_id, // Same peer_id
                supports_extension: false,
                supports_fast: false,
            };
            hs.write_to(&mut stream).unwrap();
        });

        let mut stream = TcpStream::connect(addr).unwrap();
        let res = Handshake::read_from(&mut stream, &expected_info_hash, &my_peer_id);

        assert!(res.is_err());
        assert_eq!(res.unwrap_err().to_string(), "Self-connection rejected");

        handle.join().unwrap();
    }

    #[test]
    fn test_message_too_large() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();

            // Send length prefix > MAX_MESSAGE_SIZE
            let huge_len = MAX_MESSAGE_SIZE + 1;
            stream.write_all(&huge_len.to_be_bytes()).unwrap();
        });

        let mut stream = TcpStream::connect(addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();

        let res = PeerMessage::read_from(&mut stream);
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().to_string(), "Message too large");

        handle.join().unwrap();
    }
}

#[cfg(test)]
mod fuzz_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn fuzz_peer_wire_message_decode(data in any::<Vec<u8>>()) {
            if data.len() >= 4 {
                let mut buf = std::io::Cursor::new(data);
                let _ = PeerMessage::read_from(&mut buf);
            }
        }
    }
}
