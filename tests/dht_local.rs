use serde_json::Value;
use sha1::{Digest, Sha1};
use std::fs;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const PAYLOAD: &[u8] = b"discovered through DHT";
const DEADLINE: Duration = Duration::from_secs(10);

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let mut nonce = [0; 8];
        getrandom::fill(&mut nonce).unwrap();
        let dir = std::env::temp_dir().join(format!(
            "chorus-dht-local-{:016x}",
            u64::from_ne_bytes(nonce)
        ));
        fs::create_dir(&dir).unwrap();
        Self(dir)
    }

    fn command(&self, bootstrap: SocketAddr) -> Command {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_Chorus"));
        cmd.args(["--job-dir"])
            .arg(self.0.join("jobs"))
            .args(["--output"])
            .arg(self.0.join("output"))
            .args(["--dht-bootstrap", &bootstrap.to_string()])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        cmd
    }

    fn torrent(&self, private: bool) -> (PathBuf, [u8; 20]) {
        let info = info(private);
        let mut torrent = b"d4:info".to_vec();
        torrent.extend_from_slice(&info);
        torrent.push(b'e');
        let path = self.0.join("input.torrent");
        fs::write(&path, torrent).unwrap();
        (path, Sha1::digest(info).into())
    }

    fn assert_complete(&self, output: Output) {
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let registry: Value =
            serde_json::from_slice(&fs::read(self.0.join("jobs/registry.chorus.json")).unwrap())
                .unwrap();
        assert_eq!(registry["jobs"][0]["state"], "Complete");
        assert_eq!(
            fs::read(self.0.join("output/payload.bin")).unwrap(),
            PAYLOAD
        );
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn info(private: bool) -> Vec<u8> {
    let mut info = format!(
        "d6:lengthi{}e4:name11:payload.bin12:piece lengthi16384e6:pieces20:",
        PAYLOAD.len()
    )
    .into_bytes();
    info.extend_from_slice(&Sha1::digest(PAYLOAD));
    if private {
        info.extend_from_slice(b"7:privatei1e");
    }
    info.push(b'e');
    info
}

fn wait(mut child: Child) -> Output {
    let deadline = Instant::now() + DEADLINE;
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            child.kill().unwrap();
            let output = child.wait_with_output().unwrap();
            panic!(
                "Chorus exceeded the deadline: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        thread::sleep(Duration::from_millis(10));
    }
    child.wait_with_output().unwrap()
}

fn udp() -> UdpSocket {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    socket.set_read_timeout(Some(DEADLINE)).unwrap();
    socket
}

fn read_lookup(socket: &UdpSocket, hash: [u8; 20]) -> (SocketAddr, [u8; 2], [u8; 20]) {
    let mut buffer = [0; 2048];
    let (n, source) = socket.recv_from(&mut buffer).unwrap();
    let (prefix, rest) = buffer[..n].split_at(b"d1:ad2:id20:".len());
    assert_eq!(prefix, b"d1:ad2:id20:");
    let (id, rest) = rest.split_at(20);
    let rest = rest.strip_prefix(b"9:info_hash20:").unwrap();
    let (info_hash, rest) = rest.split_at(20);
    assert_eq!(info_hash, hash);
    let rest = rest.strip_prefix(b"e1:q9:get_peers2:roi1e1:t2:").unwrap();
    let (transaction, suffix) = rest.split_at(2);
    assert_eq!(suffix, b"1:y1:qe");
    (
        source,
        transaction.try_into().unwrap(),
        id.try_into().unwrap(),
    )
}

fn compact(addr: SocketAddr) -> [u8; 6] {
    let SocketAddr::V4(addr) = addr else {
        unreachable!()
    };
    let [a, b, c, d] = addr.ip().octets();
    let [hi, lo] = addr.port().to_be_bytes();
    [a, b, c, d, hi, lo]
}

fn reply(socket: &UdpSocket, to: SocketAddr, transaction: [u8; 2], id: [u8; 20], fields: &[u8]) {
    let packet = [
        b"d1:rd2:id20:".as_slice(),
        &id,
        fields,
        b"e1:t2:",
        &transaction,
        b"1:y1:re",
    ]
    .concat();
    socket.send_to(&packet, to).unwrap();
}

fn start_seeder() -> TcpListener {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    listener
}

fn handshake(listener: &TcpListener, hash: [u8; 20], metadata: bool) -> TcpStream {
    let deadline = Instant::now() + DEADLINE;
    let mut stream = loop {
        match listener.accept() {
            Ok((stream, _)) => break stream,
            Err(err) if err.kind() == io::ErrorKind::WouldBlock && Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(err) => panic!("seeder could not accept before deadline: {err}"),
        }
    };
    stream.set_nonblocking(false).unwrap();
    stream.set_read_timeout(Some(DEADLINE)).unwrap();
    stream.set_write_timeout(Some(DEADLINE)).unwrap();
    let mut handshake = [0; 68];
    stream.read_exact(&mut handshake).unwrap();
    assert_eq!(&handshake[..20], b"\x13BitTorrent protocol");
    assert_eq!(&handshake[28..48], &hash);
    assert_eq!(handshake[25] & 0x10 != 0, metadata);
    handshake[48..].fill(b'S');
    stream.write_all(&handshake).unwrap();
    stream
}

fn message(stream: &mut TcpStream, id: u8, body: &[u8]) {
    stream
        .write_all(&((body.len() + 1) as u32).to_be_bytes())
        .unwrap();
    stream.write_all(&[id]).unwrap();
    stream.write_all(body).unwrap();
}

fn expect_message(stream: &mut TcpStream, expected: &[u8]) {
    let mut received = vec![0; 4 + expected.len()];
    stream.read_exact(&mut received).unwrap();
    assert_eq!(
        u32::from_be_bytes(received[..4].try_into().unwrap()) as usize,
        expected.len()
    );
    assert_eq!(&received[4..], expected);
}

fn seed(listener: &TcpListener, hash: [u8; 20]) {
    let mut stream = handshake(listener, hash, false);
    message(&mut stream, 5, &[0x80]);
    expect_message(&mut stream, &[2]);
    message(&mut stream, 1, &[]);
    expect_message(
        &mut stream,
        &[
            &[6, 0, 0, 0, 0, 0, 0, 0, 0][..],
            &(PAYLOAD.len() as u32).to_be_bytes(),
        ]
        .concat(),
    );
    message(&mut stream, 7, &[&[0; 8][..], PAYLOAD].concat());
}

fn assert_silent(socket: &UdpSocket) {
    socket
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let err = socket
        .recv_from(&mut [0; 2048])
        .expect_err("unexpected DHT packet");
    assert!(matches!(
        err.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
    ));
}

#[test]
fn trackerless_torrent_follows_dht_nodes_and_ignores_ping() {
    let dir = TestDir::new();
    let (torrent, hash) = dir.torrent(false);
    let (a, b) = (udp(), udp());
    let seeder = start_seeder();
    let child = dir
        .command(a.local_addr().unwrap())
        .arg("--torrent")
        .arg(torrent)
        .spawn()
        .unwrap();

    let (source, transaction, id) = read_lookup(&a, hash);
    let ping = [
        b"d1:ad2:id20:".as_slice(),
        &[2; 20],
        b"e1:q4:ping1:t2:hi1:y1:qe",
    ]
    .concat();
    a.send_to(&ping, source).unwrap();
    assert_silent(&a);
    let nodes = [
        b"5:nodes26:".as_slice(),
        &[3; 20],
        &compact(b.local_addr().unwrap()),
    ]
    .concat();
    reply(&a, source, transaction, [2; 20], &nodes);
    let (next_source, next_transaction, next_id) = read_lookup(&b, hash);
    assert_eq!(next_source, source);
    assert_eq!(next_id, id);
    let values = [
        b"6:valuesl6:".as_slice(),
        &compact(seeder.local_addr().unwrap()),
        b"e",
    ]
    .concat();
    reply(&b, source, next_transaction, [3; 20], &values);
    seed(&seeder, hash);
    dir.assert_complete(wait(child));
}

#[test]
fn private_torrent_without_other_discovery_sends_no_dht_queries() {
    let dir = TestDir::new();
    let (torrent, _) = dir.torrent(true);
    let a = udp();
    let child = dir
        .command(a.local_addr().unwrap())
        .arg("--torrent")
        .arg(torrent)
        .spawn()
        .unwrap();
    let output = wait(child);
    assert_silent(&a);
    assert_eq!(output.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("private .torrent cannot use DHT"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!dir.0.join("jobs/dht-nodes.dat").exists());
}

#[test]
fn trackerless_magnet_discovers_metadata_and_downloads() {
    let dir = TestDir::new();
    let info = info(false);
    let hash: [u8; 20] = Sha1::digest(&info).into();
    let hex: String = hash.iter().map(|byte| format!("{byte:02x}")).collect();
    let a = udp();
    let seeder = start_seeder();
    let child = dir
        .command(a.local_addr().unwrap())
        .args(["--magnet", &format!("magnet:?xt=urn:btih:{hex}")])
        .spawn()
        .unwrap();
    let (source, transaction, _) = read_lookup(&a, hash);
    let values = [
        b"6:valuesl6:".as_slice(),
        &compact(seeder.local_addr().unwrap()),
        b"e",
    ]
    .concat();
    reply(&a, source, transaction, [2; 20], &values);
    let mut stream = handshake(&seeder, hash, true);
    expect_message(&mut stream, b"\x14\x00d1:md11:ut_metadatai1eee");
    let hello = format!("\0d1:md11:ut_metadatai7ee13:metadata_sizei{}ee", info.len());
    message(&mut stream, 20, hello.as_bytes());
    expect_message(&mut stream, b"\x14\x07d8:msg_typei0e5:piecei0ee");
    let header = format!("\x01d8:msg_typei1e5:piecei0e10:total_sizei{}ee", info.len());
    message(&mut stream, 20, &[header.as_bytes(), &info].concat());
    seed(&seeder, hash);
    dir.assert_complete(wait(child));
}

#[cfg(unix)]
#[test]
fn cache_stays_in_job_dir_and_never_follows_old_temp_symlink() {
    let dir = TestDir::new();
    let (torrent, hash) = dir.torrent(false);
    let temp = dir.0.join("temp");
    let target = dir.0.join("target");
    fs::create_dir(&temp).unwrap();
    fs::write(&target, b"untouched").unwrap();
    std::os::unix::fs::symlink(&target, temp.join("chorus_dht_cache.bin")).unwrap();
    let a = udp();
    let seeder = start_seeder();
    let child = dir
        .command(a.local_addr().unwrap())
        .arg("--torrent")
        .arg(torrent)
        .env("TMPDIR", &temp)
        .spawn()
        .unwrap();
    let (source, transaction, _) = read_lookup(&a, hash);
    let values = [
        b"6:valuesl6:".as_slice(),
        &compact(seeder.local_addr().unwrap()),
        b"e",
    ]
    .concat();
    reply(&a, source, transaction, [2; 20], &values);
    seed(&seeder, hash);
    dir.assert_complete(wait(child));
    assert_eq!(fs::read(&target).unwrap(), b"untouched");
    assert_eq!(
        fs::read(dir.0.join("jobs/dht-nodes.dat")).unwrap(),
        [&[2; 20][..], &compact(a.local_addr().unwrap())].concat()
    );
}
