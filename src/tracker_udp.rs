use crate::rede::ip_proibido;
use crate::tracker::{MAX_PEERS, TrackerEvent, TrackerRequest, TrackerResponse, compact_peers};
use anyhow::{Context, Result, anyhow, bail, ensure};
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::net::{UdpSocket, lookup_host};
use tokio::time::{Instant, timeout};
use url::Url;

const PROTOCOL_ID: u64 = 0x41727101980;
const CONNECT: u32 = 0;
const ANNOUNCE: u32 = 1;
const ERROR: u32 = 3;

pub fn endpoint(url: &Url) -> Result<(&str, u16)> {
    let host = url
        .host_str()
        .filter(|host| !host.is_empty())
        .context("UDP tracker requires a host")?;
    let port = url
        .port()
        .filter(|port| *port != 0)
        .context("UDP tracker requires a nonzero port")?;
    Ok((host.trim_matches(['[', ']']), port))
}

pub struct UdpTracker {
    url: Url,
    rede_protegida: bool,
    key: [u8; 4],
    connection: Option<Connection>,
}

struct Connection {
    socket: UdpSocket,
    id: [u8; 8],
    expires: Instant,
}

impl UdpTracker {
    pub fn new(url: Url, rede_protegida: bool) -> Result<Self> {
        endpoint(&url)?;
        Ok(Self {
            url,
            rede_protegida,
            key: random_id()?,
            connection: None,
        })
    }

    pub async fn announce(&mut self, req: &TrackerRequest) -> Result<TrackerResponse> {
        let mut target = None;
        for addr in lookup_host(endpoint(&self.url)?).await? {
            ensure!(
                !self.rede_protegida || !ip_proibido(addr.ip()),
                "UDP tracker resolves to a private or local address"
            );
            if target.is_none() && addr.is_ipv4() {
                target = Some(addr);
            }
        }
        let target = target.context("UDP tracker has no IPv4 address")?;
        let mut response = vec![0; 65536];
        if !self.connection.as_ref().is_some_and(|connection| {
            connection.expires > Instant::now()
                && connection.socket.peer_addr().ok() == Some(target)
        }) {
            self.connection = None;
            self.connection = Some(connect(target, &mut response).await?);
        }
        let connection = self.connection.as_ref().unwrap();

        let event: u32 = match req.event {
            TrackerEvent::None => 0,
            TrackerEvent::Completed => 1,
            TrackerEvent::Started => 2,
            TrackerEvent::Stopped => 3,
        };
        let mut packet = [0; 98];
        packet[..8].copy_from_slice(&connection.id);
        packet[8..12].copy_from_slice(&ANNOUNCE.to_be_bytes());
        packet[12..16].copy_from_slice(&random_id()?);
        packet[16..36].copy_from_slice(&req.info_hash);
        packet[36..56].copy_from_slice(&req.peer_id);
        packet[56..64].copy_from_slice(&req.downloaded.to_be_bytes());
        packet[64..72].copy_from_slice(&req.left.to_be_bytes());
        packet[72..80].copy_from_slice(&req.uploaded.to_be_bytes());
        packet[80..84].copy_from_slice(&event.to_be_bytes());
        packet[88..92].copy_from_slice(&self.key);
        packet[92..96].copy_from_slice(&(MAX_PEERS as u32).to_be_bytes());
        packet[96..98].copy_from_slice(&req.port.to_be_bytes());

        let bytes = exchange(
            &connection.socket,
            &packet,
            ANNOUNCE,
            Some(connection.expires),
            &mut response,
        )
        .await?;
        ensure!(
            bytes.len() >= 20,
            "UDP announce response is shorter than 20 bytes"
        );
        let interval = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
        let peers = compact_peers(&bytes[20..], self.rede_protegida)?;
        Ok(TrackerResponse { interval, peers })
    }
}

fn random_id() -> Result<[u8; 4]> {
    let mut bytes = [0; 4];
    getrandom::fill(&mut bytes)
        .map_err(|err| anyhow!("could not generate UDP tracker ID: {err}"))?;
    Ok(bytes)
}

async fn connect(target: SocketAddr, response: &mut [u8]) -> Result<Connection> {
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await?;
    socket.connect(target).await?;
    let mut packet = [0; 16];
    packet[..8].copy_from_slice(&PROTOCOL_ID.to_be_bytes());
    packet[12..16].copy_from_slice(&random_id()?);
    let bytes = exchange(&socket, &packet, CONNECT, None, response).await?;
    Ok(Connection {
        socket,
        id: bytes[8..16].try_into().unwrap(),
        expires: Instant::now() + Duration::from_secs(60),
    })
}

async fn exchange<'a>(
    socket: &UdpSocket,
    request: &[u8],
    action: u32,
    expires: Option<Instant>,
    buffer: &'a mut [u8],
) -> Result<&'a [u8]> {
    // ponytail: use 2/4/8 s instead of BEP 15's 15·2ⁿ s to fit the job's 15 s announce deadline.
    for seconds in [2, 4, 8] {
        ensure!(
            expires.is_none_or(|deadline| Instant::now() < deadline),
            "UDP tracker connection ID expired"
        );
        let received = timeout(Duration::from_secs(seconds), async {
            socket.send(request).await?;
            loop {
                let len = socket.recv(buffer).await?;
                if len < 8 || buffer[4..8] != request[12..16] {
                    continue;
                }
                let reply_action = u32::from_be_bytes(buffer[..4].try_into().unwrap());
                if reply_action == ERROR {
                    let reason = String::from_utf8_lossy(&buffer[8..len.min(8 + 256)]);
                    bail!("{}", &reason[..reason.floor_char_boundary(256)]);
                }
                if reply_action == action && (action != CONNECT || len >= 16) {
                    return Ok(len);
                }
            }
        })
        .await;
        if let Ok(len) = received {
            return Ok(&buffer[..len?]);
        }
    }
    bail!("UDP tracker timed out")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::UdpSocket as TestSocket;
    use std::thread;

    fn request() -> TrackerRequest {
        TrackerRequest {
            info_hash: [1; 20],
            peer_id: [2; 20],
            port: 6881,
            uploaded: 0,
            downloaded: 0,
            left: 1,
            compact: true,
            event: TrackerEvent::Started,
        }
    }

    async fn announce_with_replies(replies: Vec<Vec<u8>>) -> Vec<Result<TrackerResponse>> {
        let deadline = Duration::from_secs(2);
        let socket = TestSocket::bind("127.0.0.1:0").unwrap();
        socket.set_read_timeout(Some(deadline)).unwrap();
        socket.set_write_timeout(Some(deadline)).unwrap();
        let url = Url::parse(&format!("udp://{}", socket.local_addr().unwrap())).unwrap();
        let mut tracker = UdpTracker::new(url, false).unwrap();
        let count = replies.len();
        let server = thread::spawn(move || {
            let mut packet = [0; 128];
            let (len, client) = socket.recv_from(&mut packet).unwrap();
            assert_eq!(len, 16);
            let mut connected = [0; 16];
            connected[4..8].copy_from_slice(&packet[12..16]);
            connected[8..].copy_from_slice(&42u64.to_be_bytes());
            socket.send_to(&connected, client).unwrap();
            for mut reply in replies {
                let (len, sender) = socket.recv_from(&mut packet).unwrap();
                assert_eq!(len, 98);
                assert_eq!(sender, client);
                assert_eq!(packet[..8], 42u64.to_be_bytes());
                reply[4..8].copy_from_slice(&packet[12..16]);
                socket.send_to(&reply, client).unwrap();
            }
        });
        let results = timeout(deadline, async {
            let mut results = Vec::new();
            for _ in 0..count {
                results.push(tracker.announce(&request()).await);
            }
            results
        })
        .await;
        server.join().unwrap();
        results.unwrap()
    }

    #[tokio::test]
    async fn rejects_malformed_announces_and_bounds_error_text() {
        let mut short = vec![0; 19];
        short[3] = 1;
        let mut uneven = vec![0; 21];
        uneven[3] = 1;
        let mut failure = vec![0; 8];
        failure[3] = 3;
        failure.extend_from_slice(&[0xff; 512]);
        let results = announce_with_replies(vec![short, uneven, failure]).await;
        let errors: Vec<_> = results
            .into_iter()
            .map(|result| result.unwrap_err().to_string())
            .collect();
        assert!(errors[0].contains("shorter than 20 bytes"));
        assert!(errors[1].contains("not a multiple of 6"));
        assert_eq!(errors[2], "�".repeat(85));
        assert!(errors[2].len() <= 256);
    }

    #[tokio::test]
    async fn reuses_one_connection_for_two_announces() {
        let mut reply = vec![0; 20];
        reply[3] = 1;
        reply[11] = 30;
        for response in announce_with_replies(vec![reply.clone(), reply]).await {
            let response = response.unwrap();
            assert_eq!(response.interval, 30);
            assert!(response.peers.is_empty());
        }
    }

    #[tokio::test]
    async fn protected_network_rejects_resolved_loopback() {
        for host in ["localhost", "127.0.0.1"] {
            let url = Url::parse(&format!("udp://{host}:6881")).unwrap();
            let mut tracker = UdpTracker::new(url, true).unwrap();
            let err = timeout(Duration::from_secs(2), tracker.announce(&request()))
                .await
                .unwrap()
                .unwrap_err();
            assert_eq!(
                err.to_string(),
                "UDP tracker resolves to a private or local address"
            );
        }
    }
}
