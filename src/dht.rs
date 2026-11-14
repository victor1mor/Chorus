use crate::bencode::{BValue, DecodeLimits, decode_with_limits, encode};
use crate::rede::ip_proibido;
use crate::tracker::{MAX_PEERS, compact_peers};
use anyhow::{Result, anyhow};
use futures::future::join_all;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::Path;
use std::time::Duration;
use tokio::net::{UdpSocket, lookup_host};
use tokio::time::{Instant, timeout, timeout_at};

const PACKET_LIMIT: usize = 2048;
const CACHE_NAME: &str = "dht-nodes.dat";
const CACHE_LIMIT: usize = 200;
const QUERY_TIMEOUT: Duration = Duration::from_secs(2);
const SEARCH_TIMEOUT: Duration = Duration::from_secs(30);
const PARALLEL_QUERIES: usize = 3;
const MAX_QUERIES: usize = 64;
const CLOSEST: usize = 8;

type Node = ([u8; 20], SocketAddrV4);

pub(crate) struct Id([u8; 20]);

impl Id {
    pub(crate) fn random() -> Result<Self> {
        Ok(Self(random_bytes()?))
    }
}

fn random_bytes<const N: usize>() -> Result<[u8; N]> {
    let mut bytes = [0; N];
    getrandom::fill(&mut bytes)
        .map_err(|err| anyhow!("could not generate DHT randomness: {err}"))?;
    Ok(bytes)
}

pub(crate) struct DhtNode {
    id: Id,
}

impl DhtNode {
    pub(crate) fn new(id: Id) -> Self {
        Self { id }
    }

    pub(crate) async fn peers(
        &self,
        info_hash: [u8; 20],
        torrent_nodes: &[String],
        bootstrap: &[String],
        directory: Option<&Path>,
        protected: bool,
    ) -> Vec<SocketAddr> {
        match self
            .discover(info_hash, torrent_nodes, bootstrap, directory, protected)
            .await
        {
            Ok(peers) => peers.into_iter().map(SocketAddr::V4).collect(),
            Err(err) => {
                crate::saida::erro(format!("Warning: DHT lookup failed: {err}"));
                Vec::new()
            }
        }
    }

    async fn discover(
        &self,
        info_hash: [u8; 20],
        torrent_nodes: &[String],
        bootstrap: &[String],
        directory: Option<&Path>,
        protected: bool,
    ) -> Result<Vec<SocketAddrV4>> {
        let deadline = Instant::now() + SEARCH_TIMEOUT;
        let path = directory.map(|dir| dir.join(CACHE_NAME));
        let cached = path
            .as_deref()
            .map_or_else(Vec::new, |path| load_cache(path, protected));
        let mut candidates: HashMap<_, _> =
            cached.iter().map(|&(id, addr)| (addr, Some(id))).collect();
        let resolved =
            join_all(bootstrap.iter().chain(torrent_nodes).take(MAX_QUERIES).map(
                |endpoint| async move { timeout(QUERY_TIMEOUT, lookup_host(endpoint)).await },
            ))
            .await;
        for addrs in resolved.into_iter().filter_map(|result| result.ok()?.ok()) {
            for addr in addrs
                .filter_map(|addr| match addr {
                    SocketAddr::V4(addr) if node_allowed(addr, protected) => Some(addr),
                    _ => None,
                })
                .take(MAX_QUERIES)
            {
                candidates.entry(addr).or_insert(None);
            }
        }
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        let (peers, verified) = self
            .search(&socket, info_hash, candidates, protected, deadline)
            .await?;
        if let Some(path) = path
            && !verified.is_empty()
            && let Err(err) = save_cache(&path, verified, &cached)
        {
            crate::saida::erro(format!("Warning: could not save DHT cache: {err}"));
        }
        Ok(peers)
    }

    async fn search(
        &self,
        socket: &UdpSocket,
        info_hash: [u8; 20],
        mut candidates: HashMap<SocketAddrV4, Option<[u8; 20]>>,
        protected: bool,
        deadline: Instant,
    ) -> Result<(Vec<SocketAddrV4>, Vec<Node>)> {
        let mut queried = HashSet::new();
        let mut pending: Vec<(SocketAddrV4, [u8; 2], Instant)> = Vec::new();
        let mut verified = Vec::new();
        let mut peers = Vec::new();
        // A full UDP buffer lets Windows discard oversized datagrams without WSAEMSGSIZE.
        let mut packet = vec![0; 65536];
        while Instant::now() < deadline && peers.len() < MAX_PEERS {
            pending.retain(|(addr, _, expires)| {
                if *expires <= Instant::now() {
                    candidates.remove(addr);
                    false
                } else {
                    true
                }
            });
            // ponytail: scan the bounded shortlist by XOR distance. Use an ordered queue
            // if the lookup budget grows enough for repeated scans to matter.
            let mut closest: Vec<_> = candidates.iter().collect();
            closest.sort_unstable_by_key(|(addr, id)| {
                let distance = id.map_or([u8::MAX; 20], |id| {
                    std::array::from_fn(|i| id[i] ^ info_hash[i])
                });
                (id.is_none(), distance, **addr)
            });
            if closest.len() >= CLOSEST
                && closest.iter().take(CLOSEST).all(|(addr, id)| {
                    id.is_some()
                        && queried.contains(*addr)
                        && !pending.iter().any(|(waiting, _, _)| waiting == *addr)
                })
            {
                break;
            }
            let slots = (PARALLEL_QUERIES - pending.len()).min(MAX_QUERIES - queried.len());
            let next: Vec<_> = closest
                .into_iter()
                .map(|(&addr, _)| addr)
                .filter(|addr| !queried.contains(addr))
                .take(slots)
                .collect();
            for addr in next {
                let transaction = random_bytes()?;
                let expires = Instant::now() + QUERY_TIMEOUT;
                queried.insert(addr);
                if timeout_at(
                    expires.min(deadline),
                    socket.send_to(&query(self.id.0, info_hash, transaction), addr),
                )
                .await
                .is_ok_and(|result| result.is_ok())
                {
                    pending.push((addr, transaction, expires));
                } else {
                    candidates.remove(&addr);
                }
            }

            let Some(expires) = pending.iter().map(|(_, _, expires)| *expires).min() else {
                if slots > 0 && candidates.keys().any(|addr| !queried.contains(addr)) {
                    continue;
                }
                break;
            };
            let Ok(received) =
                timeout_at(expires.min(deadline), socket.recv_from(&mut packet)).await
            else {
                continue;
            };
            let (len, from) = match received {
                Ok(packet) => packet,
                // An ICMP port error can surface via recv_from on Windows.
                Err(err)
                    if matches!(
                        err.kind(),
                        io::ErrorKind::ConnectionRefused | io::ErrorKind::ConnectionReset
                    ) =>
                {
                    continue;
                }
                Err(err) => return Err(err.into()),
            };
            let Some((index, id, response)) = response(&packet[..len], from, &pending) else {
                continue;
            };
            let (addr, _, _) = pending.swap_remove(index);
            candidates.insert(addr, Some(id));
            verified.push((id, addr));
            if let Some(BValue::Bytes(bytes)) = response.get(b"nodes".as_slice()) {
                for (id, addr) in compact_nodes(bytes, protected) {
                    candidates.entry(addr).or_insert(Some(id));
                }
            }
            if let Some(BValue::List(values)) = response.get(b"values".as_slice()) {
                for peer in value_peers(values, protected) {
                    if !peers.contains(&peer) {
                        peers.push(peer);
                        if peers.len() == MAX_PEERS {
                            break;
                        }
                    }
                }
            }
        }
        Ok((peers, verified))
    }
}

fn query(id: [u8; 20], info_hash: [u8; 20], transaction: [u8; 2]) -> Vec<u8> {
    let args = BValue::Dict(BTreeMap::from([
        (b"id".to_vec(), BValue::Bytes(id.to_vec())),
        (b"info_hash".to_vec(), BValue::Bytes(info_hash.to_vec())),
    ]));
    encode(&BValue::Dict(BTreeMap::from([
        (b"a".to_vec(), args),
        (b"q".to_vec(), BValue::Bytes(b"get_peers".to_vec())),
        (b"ro".to_vec(), BValue::Int(1)),
        (b"t".to_vec(), BValue::Bytes(transaction.to_vec())),
        (b"y".to_vec(), BValue::Bytes(b"q".to_vec())),
    ])))
}

type Dict = BTreeMap<Vec<u8>, BValue>;

fn response(
    packet: &[u8],
    from: SocketAddr,
    pending: &[(SocketAddrV4, [u8; 2], Instant)],
) -> Option<(usize, [u8; 20], Dict)> {
    if packet.len() > PACKET_LIMIT {
        return None;
    }
    let limits = DecodeLimits {
        max_depth: 5,
        max_items: 300,
        max_byte_len: PACKET_LIMIT,
    };
    let BValue::Dict(mut msg) = decode_with_limits(packet, limits).ok()? else {
        return None;
    };
    if msg.contains_key(b"q".as_slice())
        || msg.remove(b"y".as_slice())? != BValue::Bytes(b"r".to_vec())
    {
        return None;
    }
    let BValue::Bytes(transaction) = msg.remove(b"t".as_slice())? else {
        return None;
    };
    let index = pending.iter().position(|(addr, t, expires)| {
        SocketAddr::V4(*addr) == from && transaction == t && Instant::now() < *expires
    })?;
    let BValue::Dict(mut data) = msg.remove(b"r".as_slice())? else {
        return None;
    };
    let BValue::Bytes(id) = data.remove(b"id".as_slice())? else {
        return None;
    };
    Some((index, id.try_into().ok()?, data))
}

fn node_allowed(addr: SocketAddrV4, protected: bool) -> bool {
    addr.port() != 0
        && !addr.ip().is_unspecified()
        && (!protected || !ip_proibido((*addr.ip()).into()))
}

fn compact_nodes(bytes: &[u8], protected: bool) -> impl Iterator<Item = Node> + '_ {
    let (nodes, tail) = bytes.as_chunks::<26>();
    nodes
        .iter()
        .filter_map(move |&[id @ .., a, b, c, d, hi, lo]| {
            let addr = SocketAddrV4::new(Ipv4Addr::new(a, b, c, d), u16::from_be_bytes([hi, lo]));
            (tail.is_empty() && node_allowed(addr, protected)).then_some((id, addr))
        })
}

fn value_peers(values: &[BValue], protected: bool) -> Vec<SocketAddrV4> {
    let compact: Vec<_> = values
        .iter()
        .filter_map(|value| match value {
            BValue::Bytes(bytes) if bytes.len() == 6 => Some(bytes.as_slice()),
            _ => None,
        })
        .flatten()
        .copied()
        .collect();
    compact_peers(&compact, protected).unwrap_or_default()
}

fn load_cache(path: &Path, protected: bool) -> Vec<Node> {
    let Ok(meta) = fs::symlink_metadata(path) else {
        return Vec::new();
    };
    if !meta.is_file() || meta.len() > (CACHE_LIMIT * 26) as u64 {
        return Vec::new();
    }
    let mut bytes = Vec::new();
    if File::open(path)
        .and_then(|file| {
            file.take((CACHE_LIMIT * 26 + 1) as u64)
                .read_to_end(&mut bytes)
        })
        .is_err()
        || bytes.len() > CACHE_LIMIT * 26
    {
        return Vec::new();
    }
    compact_nodes(&bytes, protected).collect()
}

fn save_cache(path: &Path, verified: Vec<Node>, cached: &[Node]) -> io::Result<()> {
    let mut seen = HashSet::new();
    let tmp = path.with_extension(format!(
        "dat.{:016x}.tmp",
        u64::from_ne_bytes(random_bytes().map_err(io::Error::other)?)
    ));
    let mut file = File::create_new(&tmp)?;
    let written = (|| {
        for (id, addr) in verified
            .into_iter()
            .chain(cached.iter().copied())
            .filter(|(_, addr)| seen.insert(*addr))
            .take(CACHE_LIMIT)
        {
            file.write_all(&id)?;
            file.write_all(&addr.ip().octets())?;
            file.write_all(&addr.port().to_be_bytes())?;
        }
        file.sync_all()
    })();
    drop(file);
    let saved = written.and_then(|()| fs::rename(&tmp, path));
    if saved.is_err() {
        let _ = fs::remove_file(tmp);
    }
    saved
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_responses_with_wrong_transaction_or_address() {
        let addr = "127.0.0.1:1000".parse().unwrap();
        let pending = [(addr, *b"ab", Instant::now() + QUERY_TIMEOUT)];
        let packet = b"d1:rd2:id20:01234567890123456789e1:t2:ab1:y1:re";
        assert!(response(packet, SocketAddr::V4(addr), &pending).is_some());
        let mut wrong_transaction = *packet;
        let transaction = packet.windows(2).position(|bytes| bytes == b"ab").unwrap();
        wrong_transaction[transaction] = b'c';
        assert!(response(&wrong_transaction, SocketAddr::V4(addr), &pending).is_none());
        assert!(response(packet, "127.0.0.1:1001".parse().unwrap(), &pending).is_none());

        let limits = [
            [
                b"d1:rd2:id20:01234567890123456789e1:t2:ab1:y1:r1:z2000:".as_slice(),
                &[b'x'; 2000],
                b"e",
            ]
            .concat(),
            [
                b"d1:rd2:id20:01234567890123456789e1:t2:ab1:y1:r1:zllllll".as_slice(),
                b"eeeeeee",
            ]
            .concat(),
        ];
        for packet in limits {
            assert!(response(&packet, SocketAddr::V4(addr), &pending).is_none());
        }
    }

    #[test]
    fn rejects_misaligned_nodes_and_caps_values() {
        let node = [1; 27];
        assert_eq!(compact_nodes(&node[..26], false).count(), 1);
        assert_eq!(compact_nodes(&node, false).count(), 0);
        let values: Vec<_> = (1..=1000_u16)
            .map(|port| BValue::Bytes([&[8, 8, 8, 8][..], &port.to_be_bytes()].concat()))
            .collect();
        assert_eq!(value_peers(&values, true).len(), 200);
    }

    #[test]
    fn protected_network_filters_nodes_and_values() {
        let endpoints = [
            [127, 0, 0, 1, 1, 1],
            [10, 0, 0, 1, 1, 2],
            [8, 8, 8, 8, 1, 3],
        ];
        let nodes: Vec<_> = endpoints
            .iter()
            .flat_map(|addr| [[1; 20].as_slice(), addr].concat())
            .collect();
        let values: Vec<_> = endpoints
            .iter()
            .map(|addr| BValue::Bytes(addr.to_vec()))
            .collect();
        let public = "8.8.8.8:259".parse().unwrap();
        assert_eq!(compact_nodes(&nodes, false).count(), 3);
        assert_eq!(
            compact_nodes(&nodes, true).collect::<Vec<_>>(),
            [([1; 20], public)]
        );
        assert_eq!(value_peers(&values, false).len(), 3);
        assert_eq!(value_peers(&values, true), [public]);
    }

    #[tokio::test]
    async fn silent_nodes_respect_search_deadline() {
        let silent = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let SocketAddr::V4(addr) = silent.local_addr().unwrap() else {
            unreachable!()
        };
        let socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();
        let deadline = Instant::now() + Duration::from_millis(75);
        let node = DhtNode::new(Id([0; 20]));
        let (peers, verified) = timeout(
            Duration::from_secs(1),
            node.search(
                &socket,
                [1; 20],
                HashMap::from([(addr, None)]),
                false,
                deadline,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(Instant::now() >= deadline);
        assert!(peers.is_empty() && verified.is_empty());
        silent
            .set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        assert!(silent.recv_from(&mut [0; 128]).is_ok());
    }
}
