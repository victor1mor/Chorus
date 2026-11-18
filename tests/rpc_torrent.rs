use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use sha1::{Digest, Sha1};
use std::fs;
use std::io::ErrorKind;
use std::net::{SocketAddr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};

const MIB: usize = 1024 * 1024;
const DEADLINE: Duration = Duration::from_secs(15);
const DATA: &[u8] = b"Chorus RPC torrent payload\n";

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let mut nonce = [0; 8];
        getrandom::fill(&mut nonce).unwrap();
        let path = std::env::temp_dir().join(format!(
            "chorus-rpc-torrent-{}-{:016x}",
            std::process::id(),
            u64::from_ne_bytes(nonce)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

async fn daemon(
    dir: &Path,
    allow_private_network: bool,
    bootstrap: Option<SocketAddr>,
) -> (Child, SocketAddr) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_Chorus"));
    command
        .args([
            "--rpc-bind",
            "127.0.0.1:0",
            "--rpc-auth-token",
            "rpc-torrent-test-token",
            "--job-dir",
        ])
        .arg(dir.join("jobs"))
        .arg("--download-dir")
        .arg(dir.join("downloads"))
        .env("NO_PROXY", "127.0.0.1,localhost")
        .stdout(Stdio::piped())
        .kill_on_drop(true);
    if let Some(address) = bootstrap {
        assert!(address.ip().is_loopback());
        command.args(["--dht-bootstrap", &address.to_string()]);
    } else {
        command.arg("--disable-dht");
    }
    if allow_private_network {
        command.arg("--allow-private-network");
    }
    let mut child = command.spawn().unwrap();
    let mut line = String::new();
    BufReader::new(child.stdout.as_mut().unwrap())
        .read_line(&mut line)
        .await
        .unwrap();
    let address = line
        .trim_end()
        .strip_prefix("Chorus: RPC/WebSocket Server listening on ")
        .expect("RPC startup address")
        .parse()
        .unwrap();
    (child, address)
}

async fn rpc(connection: &mut BufReader<TcpStream>, method: &str, params: Value) -> Value {
    let request = json!({ "jsonrpc": "2.0", "method": method, "params": params, "id": 1 });
    connection
        .get_mut()
        .write_all(format!("{request}\n").as_bytes())
        .await
        .unwrap();
    let mut line = String::new();
    connection.read_line(&mut line).await.unwrap();
    let response: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(response["id"], 1);
    response
}

fn info() -> Vec<u8> {
    let mut info = format!(
        "d6:lengthi{}e4:name8:file.bin12:piece lengthi16384e6:pieces20:",
        DATA.len()
    )
    .into_bytes();
    info.extend_from_slice(&Sha1::digest(DATA));
    info.push(b'e');
    info
}

fn torrent(info: &[u8], tracker: Option<&str>) -> Vec<u8> {
    let mut torrent = vec![b'd'];
    if let Some(tracker) = tracker {
        torrent.extend_from_slice(format!("8:announce{}:{tracker}", tracker.len()).as_bytes());
    }
    torrent.extend_from_slice(b"4:info");
    torrent.extend_from_slice(info);
    torrent.push(b'e');
    torrent
}

fn magnet(info: &[u8]) -> String {
    let hash: String = Sha1::digest(info)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("magnet:?xt=urn:btih:{hash}")
}

async fn tracker(listener: TcpListener, peer: SocketAddrV4, announces: usize) {
    let mut body = b"d8:intervali1e5:peers6:".to_vec();
    body.extend_from_slice(&peer.ip().octets());
    body.extend_from_slice(&peer.port().to_be_bytes());
    body.push(b'e');
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    for _ in 0..announces {
        let (socket, _) = listener.accept().await.unwrap();
        let mut request = BufReader::new(socket).take(8192);
        loop {
            let mut line = String::new();
            assert_ne!(request.read_line(&mut line).await.unwrap(), 0);
            if line == "\r\n" {
                break;
            }
        }
        let mut socket = request.into_inner().into_inner();
        socket.write_all(header.as_bytes()).await.unwrap();
        socket.write_all(&body).await.unwrap();
    }
}

async fn handshake(listener: &TcpListener, info: &[u8], metadata: bool) -> TcpStream {
    let (mut socket, _) = listener.accept().await.unwrap();
    socket.set_nodelay(true).unwrap();
    let mut handshake = [0; 68];
    socket.read_exact(&mut handshake).await.unwrap();
    assert_eq!(&handshake[..20], b"\x13BitTorrent protocol");
    assert_eq!(&handshake[28..48], &Sha1::digest(info)[..]);
    assert_eq!(handshake[25] & 0x10, 0x10, "BEP 10 bit");
    // Only the download connection offers BEP 6.
    assert_eq!(handshake[27] & 0x04 != 0, !metadata);
    if !metadata {
        handshake[20..28].fill(0); // plain BitTorrent peer
    }
    handshake[48..].fill(b'S');
    socket.write_all(&handshake).await.unwrap();
    socket
}

async fn message(socket: &mut TcpStream, expected: &[u8]) {
    assert_eq!(socket.read_u32().await.unwrap() as usize, expected.len());
    let mut received = vec![0; expected.len()];
    socket.read_exact(&mut received).await.unwrap();
    assert_eq!(received, expected);
}

async fn send(socket: &mut TcpStream, message: &[u8]) {
    socket.write_u32(message.len() as u32).await.unwrap();
    socket.write_all(message).await.unwrap();
}

async fn seed(listener: TcpListener, info: &[u8], metadata: bool) {
    if metadata {
        let mut socket = handshake(&listener, info, true).await;
        message(&mut socket, b"\x14\x00d1:md11:ut_metadatai1eee").await;
        let hello = format!(
            "\x14\x00d1:md11:ut_metadatai3ee13:metadata_sizei{}ee",
            info.len()
        );
        send(&mut socket, hello.as_bytes()).await;
        message(&mut socket, b"\x14\x03d8:msg_typei0e5:piecei0ee").await;
        let mut reply = format!(
            "\x14\x01d8:msg_typei1e5:piecei0e10:total_sizei{}ee",
            info.len()
        )
        .into_bytes();
        reply.extend_from_slice(info);
        send(&mut socket, &reply).await;
    }
    let mut socket = handshake(&listener, info, false).await;
    send(&mut socket, &[5, 0x80]).await; // Bitfield with piece 0
    message(&mut socket, &[2]).await; // Interested
    send(&mut socket, &[1]).await; // Unchoke
    let mut request = vec![6];
    request.extend_from_slice(&[0; 8]);
    request.extend_from_slice(&(DATA.len() as u32).to_be_bytes());
    message(&mut socket, &request).await;
    let mut piece = vec![7];
    piece.extend_from_slice(&[0; 8]);
    piece.extend_from_slice(DATA);
    send(&mut socket, &piece).await;
}

async fn download(metadata: bool) {
    let dir = TestDir::new();
    timeout(DEADLINE, async {
        let (mut daemon, address) = daemon(&dir.0, true, None).await;
        let mut connection = BufReader::new(TcpStream::connect(address).await.unwrap());
        let tracker_socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seeder_socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let SocketAddr::V4(peer) = seeder_socket.local_addr().unwrap() else {
            panic!("expected an IPv4 peer");
        };
        let url = format!("http://{}/announce", tracker_socket.local_addr().unwrap());
        let info = info();
        let params = if metadata {
            let query = url::form_urlencoded::Serializer::new(String::new())
                .append_pair("tr", &url)
                .finish();
            json!({ "magnet": format!("{}&{query}", magnet(&info)), "output": "swarm" })
        } else {
            json!({ "torrent": STANDARD.encode(torrent(&info, Some(&url))), "output": "swarm" })
        };

        tokio::join!(
            tracker(tracker_socket, peer, if metadata { 3 } else { 2 }),
            seed(seeder_socket, &info, metadata),
            async {
                let added = rpc(&mut connection, "add_job", params).await;
                let id = added["result"]["id"]
                    .as_str()
                    .unwrap_or_else(|| panic!("{added}"));
                loop {
                    let job = rpc(&mut connection, "get_job", json!({ "id": id })).await;
                    match job["result"]["state"].as_str() {
                        Some("Waiting" | "Active") => sleep(Duration::from_millis(50)).await,
                        Some("Complete") => break,
                        _ => panic!("unexpected job state: {job}"),
                    }
                }
                assert_eq!(
                    fs::read(dir.0.join("downloads/swarm/file.bin")).unwrap(),
                    DATA
                );
            }
        );
        daemon.kill().await.unwrap();
    })
    .await
    .expect("RPC torrent download timed out");
}

#[tokio::test]
async fn downloads_torrent_via_rpc() {
    download(false).await;
}

#[tokio::test]
async fn downloads_magnet_via_rpc() {
    download(true).await;
}

#[tokio::test]
async fn rejects_invalid_jobs_without_registering_them() {
    let dir = TestDir::new();
    timeout(DEADLINE, async {
        let (mut daemon, address) = daemon(&dir.0, false, None).await;
        let mut connection = BufReader::new(TcpStream::connect(address).await.unwrap());
        let info = info();
        let magnet = magnet(&info);
        let valid = STANDARD.encode(torrent(&info, Some("udp://localhost:6881")));
        let mut oversized = torrent(&info, Some("udp://localhost:6881"));
        // An ignored top-level bencode field makes the torrent valid even without the size check.
        oversized.pop();
        let padding = 4 * MIB + 1 - oversized.len() - b"1:z0000000:e".len();
        oversized.extend_from_slice(format!("1:z{padding}:").as_bytes());
        oversized.resize(oversized.len() + padding, b'x');
        oversized.push(b'e');
        assert_eq!(oversized.len(), 4 * MIB + 1);

        for (case, params, expected) in [
            ("base64", json!({"torrent": "*", "output": "out"}), "invalid torrent base64"),
            ("size", json!({"torrent": STANDARD.encode(oversized), "output": "out"}), ".torrent exceeds 4 MiB"),
            ("metainfo", json!({"torrent": STANDARD.encode(b"de"), "output": "out"}), "Missing info dictionary"),
            ("no tracker", json!({"torrent": STANDARD.encode(torrent(&info, None)), "output": "out"}), "no usable tracker"),
            ("HTTP loopback", json!({"torrent": STANDARD.encode(torrent(&info, Some("http://127.0.0.1/announce"))), "output": "out"}), "no usable tracker"),
            ("UDP loopback", json!({"torrent": STANDARD.encode(torrent(&info, Some("udp://127.0.0.1:6881"))), "output": "out"}), "no usable tracker"),
            ("magnet size", json!({"magnet": format!("{magnet}&dn={}", "x".repeat(16 * 1024)), "output": "out"}), "Magnet link exceeds 16 KiB"),
            ("magnet without tracker", json!({"magnet": magnet, "output": "out"}), "no usable tracker"),
            ("peers", json!({"torrent": valid, "peers": [], "output": "out"}), "peers are not accepted over RPC"),
            ("multiple sources", json!({"torrent": valid, "magnet": magnet, "output": "out"}), "exactly one of 'urls', 'torrent' or 'magnet' is required"),
            ("no source", json!({"output": "out"}), "exactly one of 'urls', 'torrent' or 'magnet' is required"),
            ("output", json!({"torrent": valid, "output": "../x"}), "output must be a relative path inside the download directory"),
        ] {
            let response = rpc(&mut connection, "add_job", params).await;
            assert_eq!(
                response["error"],
                json!({ "code": -32000, "message": expected }),
                "{case}: {response}"
            );
            assert!(response.get("result").is_none(), "{case}: {response}");
            let jobs = rpc(&mut connection, "list_jobs", Value::Null).await;
            assert_eq!(jobs["result"]["jobs"], json!([]), "{case}: {jobs}");
        }
        daemon.kill().await.unwrap();
    })
    .await
    .expect("RPC rejection tests timed out");
}

#[tokio::test]
async fn trackerless_rpc_jobs_respect_private_torrents_and_protected_dht() {
    let dir = TestDir::new();
    timeout(DEADLINE, async {
        let bootstrap = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let (mut daemon, address) =
            daemon(&dir.0, false, Some(bootstrap.local_addr().unwrap())).await;
        let mut connection = BufReader::new(TcpStream::connect(address).await.unwrap());
        let mut info = info();
        let added = rpc(
            &mut connection,
            "add_job",
            json!({"torrent": STANDARD.encode(torrent(&info, None)), "output": "torrent"}),
        )
        .await;
        let torrent_id = added["result"]["id"].as_str().unwrap_or_else(|| panic!("{added}"));
        let added = rpc(
            &mut connection,
            "add_job",
            json!({"magnet": magnet(&info), "output": "magnet"}),
        )
        .await;
        let magnet_id = added["result"]["id"].as_str().unwrap_or_else(|| panic!("{added}"));

        info.pop();
        info.extend_from_slice(b"7:privatei1ee");
        let rejected = rpc(
            &mut connection,
            "add_job",
            json!({"torrent": STANDARD.encode(torrent(&info, None)), "output": "private"}),
        )
        .await;
        assert_eq!(
            rejected["error"],
            json!({"code": -32000, "message": "private .torrent cannot use DHT and requires a tracker"}),
            "{rejected}"
        );
        let jobs = rpc(&mut connection, "list_jobs", Value::Null).await;
        let jobs = jobs["result"]["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), 2, "{jobs:?}");
        for id in [torrent_id, magnet_id] {
            assert!(jobs.iter().any(|job| job["id"] == id), "{jobs:?}");
            loop {
                let job = rpc(&mut connection, "get_job", json!({"id": id})).await;
                match job["result"]["state"].as_str() {
                    Some("Waiting") => sleep(Duration::from_millis(50)).await,
                    Some("Active") => break,
                    _ if job["result"]["state"].get("Failed").is_some() => break,
                    _ => panic!("unexpected job state: {job}"),
                }
            }
        }
        assert!(
            timeout(Duration::from_secs(1), bootstrap.recv_from(&mut [0; 2048]))
                .await
                .is_err(),
            "protected RPC job reached the loopback DHT node"
        );
        daemon.kill().await.unwrap();
    })
    .await
    .expect("protected RPC discovery timed out");
}

#[tokio::test]
async fn downloads_trackerless_torrent_via_rpc_and_dht() {
    let dir = TestDir::new();
    timeout(DEADLINE, async {
        let bootstrap = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let (mut daemon, address) =
            daemon(&dir.0, true, Some(bootstrap.local_addr().unwrap())).await;
        let mut connection = BufReader::new(TcpStream::connect(address).await.unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let SocketAddr::V4(peer) = listener.local_addr().unwrap() else {
            panic!("expected an IPv4 peer");
        };
        let info = info();
        let added = rpc(
            &mut connection,
            "add_job",
            json!({"torrent": STANDARD.encode(torrent(&info, None)), "output": "swarm"}),
        )
        .await;
        let id = added["result"]["id"]
            .as_str()
            .unwrap_or_else(|| panic!("{added}"));
        let mut packet = [0; 2048];
        let (n, source) = bootstrap.recv_from(&mut packet).await.unwrap();
        let lookup = [
            b"9:info_hash20:".as_slice(),
            &Sha1::digest(&info),
            b"e1:q9:get_peers2:roi1e1:t2:",
        ]
        .concat();
        let transaction = packet[..n]
            .strip_suffix(b"1:y1:qe")
            .and_then(|prefix| prefix.split_last_chunk::<2>())
            .and_then(|(prefix, transaction)| prefix.ends_with(&lookup).then_some(transaction))
            .expect("expected a read-only DHT lookup for the torrent");
        let response = [
            b"d1:rd2:id20:".as_slice(),
            &[2; 20],
            b"6:valuesl6:",
            &peer.ip().octets(),
            &peer.port().to_be_bytes(),
            b"ee1:t2:",
            transaction,
            b"1:y1:re",
        ]
        .concat();
        bootstrap.send_to(&response, source).await.unwrap();
        seed(listener, &info, false).await;

        loop {
            let job = rpc(&mut connection, "get_job", json!({"id": id})).await;
            match job["result"]["state"].as_str() {
                Some("Waiting" | "Active") => sleep(Duration::from_millis(50)).await,
                Some("Complete") => break,
                _ => panic!("unexpected job state: {job}"),
            }
        }
        assert_eq!(
            fs::read(dir.0.join("downloads/swarm/file.bin")).unwrap(),
            DATA
        );
        daemon.kill().await.unwrap();
    })
    .await
    .expect("trackerless RPC torrent download timed out");
}

#[tokio::test]
async fn closes_oversized_ndjson_without_waiting_for_newline() {
    let dir = TestDir::new();
    timeout(DEADLINE, async {
        let (mut daemon, address) = daemon(&dir.0, false, None).await;
        let mut socket = TcpStream::connect(address).await.unwrap();
        let mut line = vec![b' '; 9 * MIB];
        line[0] = b'{';
        timeout(Duration::from_secs(2), async {
            let (mut reader, mut writer) = socket.split();
            // Keep the write half open. EOF here must come from the server.
            let (sent, received) = tokio::join!(writer.write_all(&line), async {
                let mut reply = Vec::new();
                reader.read_to_end(&mut reply).await
            });
            for result in [sent, received.map(|_| ())] {
                if let Err(error) = result {
                    assert!(
                        matches!(
                            error.kind(),
                            ErrorKind::ConnectionReset
                                | ErrorKind::ConnectionAborted
                                | ErrorKind::BrokenPipe
                        ),
                        "{error}"
                    );
                }
            }
        })
        .await
        .expect("server did not close the oversized NDJSON connection within 2 seconds");

        let mut connection = BufReader::new(TcpStream::connect(address).await.unwrap());
        let response = rpc(&mut connection, "list_jobs", Value::Null).await;
        assert_eq!(response["result"]["jobs"], json!([]));
        daemon.kill().await.unwrap();
    })
    .await
    .expect("NDJSON test timed out");
}

#[tokio::test]
async fn help_lists_udp_for_torrent_and_magnet() {
    let output = timeout(
        DEADLINE,
        Command::new(env!("CARGO_BIN_EXE_Chorus"))
            .arg("--help")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("--help timed out")
    .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    for option in ["--torrent", "--magnet"] {
        let mut lines = help.lines();
        lines
            .find(|line| line.trim_start().starts_with(&format!("{option} <")))
            .expect("missing option");
        let description = lines.next().unwrap();
        assert!(
            description.contains("HTTP(S) and UDP trackers"),
            "{option}: {description}"
        );
    }
}
