use crate::bencode::{self, BValue, DecodeLimits};
use crate::http::{HttpFetcher, content_length_from_headers};
use crate::rede::{ip_proibido, validar_url};
use crate::tracker_udp::UdpTracker;
use anyhow::{Result, anyhow, ensure};
use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddrV4};
use url::Url;

const MAX_RESPONSE: usize = 1024 * 1024;
pub const MAX_PEERS: usize = 200;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackerEvent {
    Started,
    Stopped,
    Completed,
    None,
}

impl TrackerEvent {
    fn as_str(&self) -> Option<&'static str> {
        match self {
            TrackerEvent::Started => Some("started"),
            TrackerEvent::Stopped => Some("stopped"),
            TrackerEvent::Completed => Some("completed"),
            TrackerEvent::None => None,
        }
    }
}

pub struct TrackerRequest {
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub port: u16,
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
    pub compact: bool,
    pub event: TrackerEvent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackerResponse {
    pub interval: u32,
    pub peers: Vec<SocketAddrV4>,
}

pub enum Tracker {
    Http(TrackerClient),
    Udp(UdpTracker),
}

impl Tracker {
    pub async fn announce(&mut self, request: &TrackerRequest) -> Result<TrackerResponse> {
        match self {
            Self::Http(tracker) => tracker.announce(request).await,
            Self::Udp(tracker) => tracker.announce(request).await,
        }
    }
}

pub fn validar_tracker_url(raw: &str, rede_protegida: bool) -> Result<()> {
    let url = Url::parse(raw)?;
    if url.scheme() == "udp" {
        let (host, _) = crate::tracker_udp::endpoint(&url)?;
        ensure!(
            !rede_protegida || !host.parse::<IpAddr>().is_ok_and(ip_proibido),
            "destination is a private or local address"
        );
        Ok(())
    } else {
        validar_url(raw, rede_protegida)
    }
}

pub fn url_encode_bytes(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len() * 3);
    for &b in data {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'.' | b'-' | b'_' | b'~' => {
                out.push(b as char);
            }
            _ => {
                out.push_str(&format!("%{:02X}", b));
            }
        }
    }
    out
}

#[derive(Clone)]
pub struct TrackerClient {
    fetcher: HttpFetcher,
    announce_url: Url,
    rede_protegida: bool,
}

impl TrackerClient {
    pub fn new(fetcher: HttpFetcher, announce_url: &str, rede_protegida: bool) -> Result<Self> {
        validar_url(announce_url, rede_protegida)?;
        Ok(Self {
            fetcher,
            announce_url: Url::parse(announce_url)?,
            rede_protegida,
        })
    }

    pub async fn announce(&self, req: &TrackerRequest) -> Result<TrackerResponse> {
        let mut query = String::new();
        query.push_str(&format!("info_hash={}", url_encode_bytes(&req.info_hash)));
        query.push_str(&format!("&peer_id={}", url_encode_bytes(&req.peer_id)));
        query.push_str(&format!("&port={}", req.port));
        query.push_str(&format!("&uploaded={}", req.uploaded));
        query.push_str(&format!("&downloaded={}", req.downloaded));
        query.push_str(&format!("&left={}", req.left));
        query.push_str(&format!("&compact={}", if req.compact { 1 } else { 0 }));

        if let Some(event_str) = req.event.as_str() {
            query.push_str(&format!("&event={}", event_str));
        }

        let mut url = self.announce_url.clone();
        if let Some(existing) = url.query().filter(|query| !query.is_empty()) {
            query = format!("{existing}&{query}");
        }
        url.set_query(Some(&query));
        url.set_fragment(None);

        let mut resp = self
            .fetcher
            .fetch_full(url.as_str())
            .await
            .map_err(crate::saida::redigir_erro_http)?;
        ensure!(
            content_length_from_headers(resp.headers())
                .is_none_or(|len| len <= MAX_RESPONSE as u64),
            "Tracker response exceeds 1 MiB"
        );
        let mut bytes = Vec::new();
        while let Some(chunk) = resp
            .chunk()
            .await
            .map_err(|err| crate::saida::redigir_erro_http(err.into()))?
        {
            ensure!(
                chunk.len() <= MAX_RESPONSE - bytes.len(),
                "Tracker response exceeds 1 MiB"
            );
            bytes.extend_from_slice(&chunk);
        }

        parse_response(&bytes, self.rede_protegida)
    }
}

fn parse_response(bytes: &[u8], rede_protegida: bool) -> Result<TrackerResponse> {
    let limits = DecodeLimits::default();
    let dict = bencode::decode_dict_with_offsets(bytes, limits)
        .map_err(|e| anyhow!("Failed to decode tracker response: {}", e))?;

    if let Some((BValue::Bytes(failure_reason), _, _)) = dict.get(b"failure reason".as_slice()) {
        let reason = String::from_utf8_lossy(&failure_reason[..failure_reason.len().min(256)]);
        return Err(anyhow!("Tracker failure: {}", reason));
    }

    let interval = match dict.get(b"interval".as_slice()) {
        Some((BValue::Int(i), _, _)) => {
            if *i < 0 {
                return Err(anyhow!("Negative interval"));
            }
            u32::try_from(*i).unwrap_or(u32::MAX)
        }
        _ => return Err(anyhow!("Missing or invalid interval in tracker response")),
    };

    let Some((BValue::Bytes(peers_bytes), _, _)) = dict.get(b"peers".as_slice()) else {
        return Err(anyhow!(
            "Tracker response requires a compact IPv4 peers list"
        ));
    };
    let peers = compact_peers(peers_bytes, rede_protegida)?;
    Ok(TrackerResponse { interval, peers })
}

pub fn compact_peers(bytes: &[u8], rede_protegida: bool) -> Result<Vec<SocketAddrV4>> {
    let (entries, remainder) = bytes.as_chunks::<6>();
    ensure!(
        remainder.is_empty(),
        "Compact peers length is not a multiple of 6"
    );
    let mut seen = HashSet::new();
    Ok(entries
        .iter()
        .map(|&[a, b, c, d, hi, lo]| {
            SocketAddrV4::new(Ipv4Addr::new(a, b, c, d), u16::from_be_bytes([hi, lo]))
        })
        .filter(|peer| {
            peer.port() != 0
                && !peer.ip().is_unspecified()
                && (!rede_protegida || !ip_proibido((*peer.ip()).into()))
                && seen.insert(*peer)
        })
        .take(MAX_PEERS)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_url_encode_bytes() {
        let data =
            b"\x12\x34\x56\x78\x9a\xbc\xde\xf1\x23\x45\x67\x89\xab\xcd\xef\x12\x34\x56\x78\x9a";
        let encoded = url_encode_bytes(data);
        assert_eq!(encoded, "%124Vx%9A%BC%DE%F1%23Eg%89%AB%CD%EF%124Vx%9A");

        let safe = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_~";
        let encoded_safe = url_encode_bytes(safe);
        assert_eq!(encoded_safe, std::str::from_utf8(safe).unwrap());
    }

    struct TrackerFixture {
        pub addr: String,
        pub requests: Arc<Mutex<Vec<String>>>,
    }

    impl TrackerFixture {
        fn new(responses: Vec<Vec<u8>>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = format!("http://{}", listener.local_addr().unwrap());
            let requests = Arc::new(Mutex::new(Vec::new()));

            let reqs_clone = Arc::clone(&requests);
            std::thread::spawn(move || {
                let mut iter = responses.into_iter();
                for stream in listener.incoming().flatten() {
                    let mut stream = stream;
                    let mut buf = [0; 1024];
                    if let Ok(read) = stream.read(&mut buf)
                        && read > 0
                    {
                        let req_str = String::from_utf8_lossy(&buf[..read]).to_string();
                        let path_query =
                            req_str.split_whitespace().nth(1).unwrap_or("").to_string();
                        reqs_clone.lock().unwrap().push(path_query);
                    }

                    let response_body = if let Some(body) = iter.next() {
                        body
                    } else {
                        b"d14:failure reason12:Out of peers.e".to_vec()
                    };

                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: text/plain\r\n\r\n",
                        response_body.len()
                    );
                    let mut full_resp = response.into_bytes();
                    full_resp.extend_from_slice(&response_body);

                    let _ = stream.write_all(&full_resp);
                }
            });

            Self { addr, requests }
        }
    }

    #[tokio::test]
    async fn test_tracker_announce_success() -> Result<()> {
        let mut dict = BTreeMap::new();
        dict.insert(b"interval".to_vec(), BValue::Int(1800));

        // IP: 192.168.1.100, port: 6881 -> C0 A8 01 64 1A E1
        let compact_peer = vec![192, 168, 1, 100, 26, 225];
        dict.insert(b"peers".to_vec(), BValue::Bytes(compact_peer));

        let resp_bytes = bencode::encode(&BValue::Dict(dict));

        let fixture = TrackerFixture::new(vec![resp_bytes]);

        let fetcher = HttpFetcher::new(None, None, Some(std::time::Duration::from_secs(5)), false)?;
        let client = TrackerClient::new(fetcher, &format!("{}/announce", fixture.addr), false)?;

        let req = TrackerRequest {
            info_hash: [1; 20],
            peer_id: [2; 20],
            port: 6881,
            uploaded: 0,
            downloaded: 0,
            left: 1000,
            compact: true,
            event: TrackerEvent::Started,
        };

        let resp = client.announce(&req).await?;

        assert_eq!(resp.interval, 1800);
        assert_eq!(resp.peers.len(), 1);
        assert_eq!(
            resp.peers[0],
            SocketAddrV4::new(Ipv4Addr::new(192, 168, 1, 100), 6881)
        );

        let reqs = fixture.requests.lock().unwrap();
        assert_eq!(reqs.len(), 1);
        assert!(reqs[0].contains("event=started"));
        assert!(reqs[0].contains("compact=1"));

        Ok(())
    }

    #[tokio::test]
    async fn test_tracker_failure_reason() -> Result<()> {
        let mut dict = BTreeMap::new();
        dict.insert(
            b"failure reason".to_vec(),
            BValue::Bytes(b"Invalid info_hash".to_vec()),
        );

        let resp_bytes = bencode::encode(&BValue::Dict(dict));

        let fixture = TrackerFixture::new(vec![resp_bytes]);

        let fetcher = HttpFetcher::new(None, None, Some(std::time::Duration::from_secs(5)), false)?;
        let client = TrackerClient::new(fetcher, &format!("{}/announce", fixture.addr), false)?;

        let req = TrackerRequest {
            info_hash: [1; 20],
            peer_id: [2; 20],
            port: 6881,
            uploaded: 0,
            downloaded: 0,
            left: 1000,
            compact: true,
            event: TrackerEvent::None,
        };

        let err = client.announce(&req).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("Tracker failure: Invalid info_hash")
        );

        Ok(())
    }

    #[test]
    fn protected_tracker_and_peers_are_filtered() -> Result<()> {
        let fetcher = HttpFetcher::new(None, None, None, true)?;
        let Err(err) = TrackerClient::new(fetcher, "http://127.0.0.1:6881/announce", true) else {
            panic!("accepted a local tracker in protected mode");
        };
        assert!(err.to_string().contains("private or local address"));

        let mut response = b"d8:intervali1e5:peers42:".to_vec();
        response.extend_from_slice(&[
            127, 0, 0, 1, 1, 1, 10, 0, 0, 1, 1, 1, 169, 254, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 8, 8, 8,
            8, 0, 0, 8, 8, 8, 8, 1, 1, 8, 8, 8, 8, 1, 1,
        ]);
        response.push(b'e');
        assert_eq!(
            parse_response(&response, true)?.peers,
            ["8.8.8.8:257".parse()?]
        );
        assert_eq!(parse_response(&response, false)?.peers.len(), 4);
        Ok(())
    }
}

#[cfg(test)]
mod fuzz_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn fuzz_tracker_response_parser(data in any::<Vec<u8>>()) {
            let _ = parse_response(&data, true);
        }
    }
}
