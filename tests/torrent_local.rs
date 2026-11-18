use serde_json::{Value, json};
use sha1::{Digest, Sha1};
use sha2::Sha256;
use std::fs;
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const PIECE: usize = 16 * 1024;
const DEADLINE: Duration = Duration::from_secs(20);

#[derive(Clone, Copy, PartialEq)]
enum Seeder {
    Clean,
    CorruptOnce,
    CorruptAlways,
    WrongHash,
    StallAfterTwo,
    LateFirstPiece,
    HaveOnly,
}

fn send_message(stream: &mut TcpStream, id: u8, body: &[u8]) {
    stream
        .write_all(&((body.len() + 1) as u32).to_be_bytes())
        .unwrap();
    stream.write_all(&[id]).unwrap();
    stream.write_all(body).unwrap();
}

fn read_message(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut length = [0; 4];
    stream.read_exact(&mut length)?;
    let length = u32::from_be_bytes(length) as usize;
    assert!(length <= 128);
    let mut message = vec![0; length];
    stream.read_exact(&mut message)?;
    Ok(message)
}

fn request_index(message: &[u8]) -> usize {
    assert_eq!(message[0], 6);
    u32::from_be_bytes(message[1..5].try_into().unwrap()) as usize
}

fn assert_requests(mut requested: Vec<usize>, expected: &[usize]) {
    requested.sort_unstable();
    assert_eq!(requested, expected);
}

fn checkpoint_pieces(map: &Path) -> Vec<usize> {
    let bits = read_json(map.to_path_buf())["bitfield"][0]
        .as_u64()
        .unwrap();
    (0..8).filter(|index| bits & (0x80 >> index) != 0).collect()
}

fn accept_before(listener: &TcpListener, deadline: Instant) -> TcpStream {
    listener.set_nonblocking(true).unwrap();
    let stream = loop {
        match listener.accept() {
            Ok((stream, _)) => break stream,
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < deadline, "no connection before deadline");
                thread::sleep(Duration::from_millis(10));
            }
            Err(err) => panic!("accept failed: {err}"),
        }
    };
    stream.set_nonblocking(false).unwrap();
    stream
}

fn seed(listener: &TcpListener, data: &[u8], hash: [u8; 20], mode: Seeder) -> Vec<usize> {
    let deadline = Instant::now() + DEADLINE;
    let stream = accept_before(listener, deadline);
    seed_connected(stream, data, hash, mode, deadline)
}

fn seed_connected(
    mut stream: TcpStream,
    data: &[u8],
    hash: [u8; 20],
    mode: Seeder,
    deadline: Instant,
) -> Vec<usize> {
    stream.set_nonblocking(false).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.set_nodelay(true).unwrap();
    let mut handshake = [0; 68];
    stream.read_exact(&mut handshake).unwrap();
    assert_eq!(&handshake[..20], b"\x13BitTorrent protocol");
    assert_eq!(
        handshake[27] & 0x04,
        0x04,
        "download handshake must offer BEP 6"
    );
    assert_eq!(&handshake[28..48], &hash);
    assert_eq!(&handshake[48..56], b"-CH0400-");
    if mode == Seeder::WrongHash {
        handshake[28] ^= 0xff;
    }
    // Answer as a plain BitTorrent peer: no BEP 6 or BEP 10 on this connection.
    handshake[20..28].fill(0);
    handshake[48..].fill(b'S');
    stream.write_all(&handshake).unwrap();
    if mode == Seeder::WrongHash {
        return Vec::new();
    }

    let pieces = data.len().div_ceil(PIECE);
    let mut bitfield = vec![0; pieces.div_ceil(8)];
    for index in 0..pieces {
        bitfield[index / 8] |= 0x80 >> (index % 8);
    }
    if mode == Seeder::LateFirstPiece {
        bitfield[0] &= 0x7f;
    }
    if mode == Seeder::HaveOnly {
        for index in 1..pieces as u32 {
            send_message(&mut stream, 4, &index.to_be_bytes());
        }
    } else {
        send_message(&mut stream, 5, &bitfield);
    }
    assert_eq!(read_message(&mut stream).unwrap(), [2]);
    send_message(&mut stream, 1, &[]);

    let mut requests = Vec::new();
    loop {
        assert!(Instant::now() < deadline, "seeder exceeded its deadline");
        let message = match read_message(&mut stream) {
            Ok(message) => message,
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset
                ) =>
            {
                return requests;
            }
            Err(err) => panic!("read failed: {err}"),
        };
        if message[0] == 8 {
            continue;
        }
        assert_eq!(message[0], 6, "expected Request, got {message:?}");
        let index = u32::from_be_bytes(message[1..5].try_into().unwrap()) as usize;
        if mode == Seeder::HaveOnly && index == 0 && requests.len() < pieces - 1 {
            // Close on a request for a piece this peer has not announced yet.
            return requests;
        }
        if mode == Seeder::StallAfterTwo && requests.len() == 2 {
            continue;
        }
        if mode == Seeder::CorruptAlways && requests.first().is_some_and(|&first| first != index) {
            // With several pieces in flight, answer only retries of the first corrupt piece.
            // Each request for it still requires the previous bad piece to have been rejected.
            continue;
        }
        if mode == Seeder::StallAfterTwo && requests.len() == 1 {
            // Let the checkpoint interval elapse before the seeder goes quiet.
            thread::sleep(Duration::from_millis(1100));
        }
        let begin = u32::from_be_bytes(message[5..9].try_into().unwrap()) as usize;
        let length = u32::from_be_bytes(message[9..13].try_into().unwrap()) as usize;
        let offset = index * PIECE + begin;
        let mut body = message[1..9].to_vec();
        body.extend_from_slice(&data[offset..offset + length]);
        if mode == Seeder::CorruptOnce && requests.is_empty() {
            // Keep the request outstanding after a wrong index, misaligned offset,
            // or short block. Only the subsequent full block counts as a bad piece.
            body[3] ^= 1;
            send_message(&mut stream, 7, &body);
            body[3] ^= 1;
            body[7] ^= 1;
            send_message(&mut stream, 7, &body[..body.len() - 1]);
            body[7] ^= 1;
            send_message(&mut stream, 7, &body[..body.len() - 1]);
            body[8] ^= 1;
        } else if mode == Seeder::CorruptAlways {
            body[8] ^= 1;
        }
        send_message(&mut stream, 7, &body);
        requests.push(index);
        if matches!(mode, Seeder::LateFirstPiece | Seeder::HaveOnly) && requests.len() == pieces - 1
        {
            send_message(&mut stream, 4, &0_u32.to_be_bytes());
        }
    }
}

fn make_torrent(data: &[u8], files: &[(&[&str], usize)], bad_count: bool) -> (Vec<u8>, [u8; 20]) {
    make_torrent_with_piece_length(data, files, bad_count, PIECE)
}

fn make_torrent_with_piece_length(
    data: &[u8],
    files: &[(&[&str], usize)],
    bad_count: bool,
    piece_length: usize,
) -> (Vec<u8>, [u8; 20]) {
    let mut info = if files.is_empty() {
        format!("d6:lengthi{}e4:name11:payload.bin", data.len()).into_bytes()
    } else {
        let mut info = String::from("d5:filesl");
        for (path, length) in files {
            info.push_str(&format!("d6:lengthi{length}e4:pathl"));
            for component in *path {
                info.push_str(&format!("{}:{component}", component.len()));
            }
            info.push_str("ee");
        }
        info.push_str("e4:name7:payload");
        info.into_bytes()
    };
    let mut hashes: Vec<_> = data
        .chunks(piece_length)
        .flat_map(|piece| Sha1::digest(piece).to_vec())
        .collect();
    if bad_count {
        hashes.truncate(hashes.len() - 20);
    }
    write!(
        info,
        "12:piece lengthi{piece_length}e6:pieces{}:",
        hashes.len()
    )
    .unwrap();
    info.extend_from_slice(&hashes);
    info.push(b'e');
    let hash = Sha1::digest(&info).into();
    let mut torrent = b"d4:info".to_vec();
    torrent.extend_from_slice(&info);
    torrent.push(b'e');
    (torrent, hash)
}

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path = std::env::temp_dir().join(format!(
            "chorus-torrent-local-{:032x}",
            u128::from_ne_bytes(nonce)
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

fn job_command(dir: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_Chorus"));
    command
        .args(["--disable-dht", "--job-dir"])
        .arg(dir.join("jobs"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

fn download_command(dir: &Path, peers: &[SocketAddr]) -> Command {
    let mut command = job_command(dir);
    command
        .arg("--torrent")
        .arg(dir.join("input.torrent"))
        .arg("-o")
        .arg(dir.join("output"));
    for peer in peers {
        command.arg("--peer").arg(peer.to_string());
    }
    command
}

fn wait_for_download(mut child: Child) -> Output {
    let deadline = Instant::now() + DEADLINE;
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            child.kill().unwrap();
            let output = child.wait_with_output().unwrap();
            panic!(
                "download timed out: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        thread::sleep(Duration::from_millis(10));
    }
    child.wait_with_output().unwrap()
}

fn read_json(path: PathBuf) -> Value {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}

fn run_case(mode: Seeder, files: &[(&[&str], usize)], total: usize, expected: Value, events: bool) {
    let dir = TestDir::new();
    let data: Vec<_> = (0..total).map(|i| (i % 251) as u8).collect();
    let (torrent, hash) = make_torrent(&data, files, false);
    fs::write(dir.0.join("input.torrent"), &torrent).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let peer = listener.local_addr().unwrap();
    let seeder_data = data.clone();
    let seeder = thread::spawn(move || seed(&listener, &seeder_data, hash, mode));
    let mut command = download_command(&dir.0, &[peer]);
    if events {
        command.arg("--events");
    }
    let output = wait_for_download(command.spawn().unwrap());
    let count = seeder.join().unwrap().len();
    match mode {
        Seeder::Clean => assert_eq!(count, total.div_ceil(PIECE)),
        Seeder::CorruptOnce => assert_eq!(count, total.div_ceil(PIECE) + 1),
        Seeder::CorruptAlways => assert_eq!(count, 3),
        Seeder::WrongHash => assert_eq!(count, 0),
        Seeder::StallAfterTwo | Seeder::LateFirstPiece | Seeder::HaveOnly => unreachable!(),
    }

    let jobs = dir.0.join("jobs");
    let registry = read_json(jobs.join("registry.chorus.json"));
    let record = &registry["jobs"][0];
    assert_eq!(registry["jobs"].as_array().unwrap().len(), 1);
    assert_eq!(
        record["state"],
        expected,
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let id = record["id"].as_str().unwrap();
    let manifest = read_json(jobs.join(format!("{id}.chorus.json")));
    let source = format!(
        "torrent:{}",
        hash.iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
    assert_eq!(manifest["sources"], json!([source]));
    assert_eq!(manifest["version"], 2);
    assert_eq!(manifest["state"], expected);
    let private = read_json(jobs.join(format!("{id}.chorus.private.json")));
    let filename = format!("{id}.torrent");
    assert_eq!(private["torrent"], filename);
    assert_eq!(private["peers"], json!([peer.to_string()]));
    assert_eq!(private["original_urls"], json!([]));
    assert_eq!(fs::read(jobs.join(filename)).unwrap(), torrent);

    assert_eq!(output.status.success(), expected == "Complete");
    if expected == "Complete" {
        let output_dir = dir.0.join("output");
        if files.is_empty() {
            assert_eq!(fs::read(output_dir.join("payload.bin")).unwrap(), data);
        } else {
            let mut offset = 0;
            for &(path, length) in files {
                let path = path
                    .iter()
                    .fold(output_dir.join("payload"), |p, component| p.join(component));
                assert_eq!(fs::read(path).unwrap(), data[offset..offset + length]);
                offset += length;
            }
            assert_eq!(offset, total);
        }
    }
    if events {
        let events: Vec<Value> = String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let progress = events
            .iter()
            .position(|event| {
                event["type"] == "Progress"
                    && event["data"]["id"] == id
                    && event["data"]["bytes_total"] == total
                    && event["data"]["bytes_downloaded"]
                        .as_u64()
                        .is_some_and(|bytes| bytes > 0)
            })
            .expect("missing Progress");
        let complete = events
            .iter()
            .position(|event| {
                event["type"] == "StateChanged"
                    && event["data"]["id"] == id
                    && event["data"]["to"] == "Complete"
            })
            .expect("missing Complete");
        assert!(progress < complete);
    }
}

#[test]
fn single_file() {
    run_case(Seeder::Clean, &[], 300 * 1024, json!("Complete"), false);
}

#[test]
fn never_uploads_after_interested_and_request() {
    let dir = TestDir::new();
    let (data, hash) = make_single_file(&dir.0, PIECE + 1);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let peer = listener.local_addr().unwrap();
    let child = download_command(&dir.0, &[peer]).spawn().unwrap();
    let mut stream = accept_before(&listener, Instant::now() + DEADLINE);
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let mut handshake = [0; 68];
    stream.read_exact(&mut handshake).unwrap();
    assert_eq!(&handshake[28..48], &hash);
    // Answer as a plain BitTorrent peer: no BEP 6 or BEP 10 on this connection.
    handshake[20..28].fill(0);
    handshake[48..].fill(b'S');
    stream.write_all(&handshake).unwrap();
    send_message(&mut stream, 5, &[0xc0]);
    assert_eq!(read_message(&mut stream).unwrap(), [2]);
    send_message(&mut stream, 1, &[]);

    let request = read_message(&mut stream).unwrap();
    let first = request_index(&request);
    assert!(first < 2);
    assert_eq!(&request[5..9], &[0; 4]);
    let (offset, end) = (first * PIECE, ((first + 1) * PIECE).min(data.len()));
    let mut first_piece = request[1..9].to_vec();
    first_piece.extend_from_slice(&data[offset..end]);
    send_message(&mut stream, 7, &first_piece);
    let request = read_message(&mut stream).unwrap();
    let second = request_index(&request);
    assert_eq!(second, 1 - first);
    assert_eq!(&request[5..9], &[0; 4]);

    send_message(&mut stream, 2, &[]);
    send_message(&mut stream, 6, &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let mut byte = [0];
    let err = stream
        .read(&mut byte)
        .expect_err("Chorus sent a message after Interested and Request");
    assert!(matches!(
        err.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
    ));
    let mut last_piece = request[1..9].to_vec();
    last_piece.extend_from_slice(&data[second * PIECE..((second + 1) * PIECE).min(data.len())]);
    send_message(&mut stream, 7, &last_piece);
    let output = wait_for_download(child);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn multi_file_with_subdirectory() {
    let files: &[(&[&str], usize)] = &[
        (&["a.bin"], 12345),
        (&["sub", "b.bin"], 31000),
        (&["sub", "c.bin"], 9),
    ];
    run_case(Seeder::Clean, files, 43354, json!("Complete"), false);
}

#[test]
fn retries_a_corrupt_piece() {
    run_case(
        Seeder::CorruptOnce,
        &[],
        PIECE + 19,
        json!("Complete"),
        false,
    );
}

#[test]
fn abandons_peer_after_three_bad_pieces() {
    run_case(
        Seeder::CorruptAlways,
        &[],
        PIECE + 19,
        json!({"Failed":"Network"}),
        false,
    );
}

#[test]
fn rejects_wrong_info_hash() {
    run_case(
        Seeder::WrongHash,
        &[],
        PIECE,
        json!({"Failed":"Network"}),
        false,
    );
}

#[test]
fn rejects_wrong_piece_count_before_connecting() {
    let dir = TestDir::new();
    let (torrent, _) = make_torrent(&vec![0; PIECE + 19], &[], true);
    fs::write(dir.0.join("input.torrent"), torrent).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let output = wait_for_download(
        download_command(&dir.0, &[listener.local_addr().unwrap()])
            .spawn()
            .unwrap(),
    );
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("pieces count mismatch"));
    let jobs = dir.0.join("jobs");
    if jobs.exists() {
        assert_eq!(fs::read_dir(jobs).unwrap().count(), 0);
    }
    assert!(!dir.0.join("output").exists());
    listener.set_nonblocking(true).unwrap();
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}

#[test]
fn emits_progress_before_complete() {
    run_case(Seeder::Clean, &[], 300 * 1024, json!("Complete"), true);
}

fn make_single_file(dir: &Path, total: usize) -> (Vec<u8>, [u8; 20]) {
    let data: Vec<_> = (0..total).map(|i| (i % 251) as u8).collect();
    let (torrent, hash) = make_torrent(&data, &[], false);
    fs::write(dir.join("input.torrent"), torrent).unwrap();
    (data, hash)
}

fn interrupt_download(dir: &Path, peer: SocketAddr, no_overwrite: bool) -> (String, PathBuf) {
    let mut command = download_command(dir, &[peer]);
    if no_overwrite {
        command.arg("--no-overwrite");
    }
    interrupt_at_checkpoint(dir, command.spawn().unwrap(), false)
}

fn interrupt_at_checkpoint(
    dir: &Path,
    mut child: Child,
    require_torrent: bool,
) -> (String, PathBuf) {
    let jobs = dir.join("jobs");
    let deadline = Instant::now() + DEADLINE;
    while Instant::now() < deadline && child.try_wait().unwrap().is_none() {
        let checkpoint = (|| {
            let registry: Value =
                serde_json::from_slice(&fs::read(jobs.join("registry.chorus.json")).ok()?).ok()?;
            let id = registry["jobs"][0]["id"].as_str()?;
            let path = jobs.join(format!("{id}.chorus.piecemap.json"));
            let map: Value = serde_json::from_slice(&fs::read(&path).ok()?).ok()?;
            (map["bitfield"][0].as_u64()?.count_ones() == 2
                && (!require_torrent || jobs.join(format!("{id}.torrent")).is_file()))
            .then(|| (id.to_owned(), path))
        })();
        if let Some(checkpoint) = checkpoint {
            child.kill().unwrap();
            assert!(!child.wait().unwrap().success());
            return checkpoint;
        }
        thread::sleep(Duration::from_millis(10));
    }
    if child.try_wait().unwrap().is_none() {
        child.kill().unwrap();
    }
    let output = child.wait_with_output().unwrap();
    panic!(
        "download never saved its partial map: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_complete(dir: &Path, output: &Output, data: &[u8]) {
    assert!(
        output.status.success(),
        "download failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let registry = read_json(dir.join("jobs/registry.chorus.json"));
    assert_eq!(registry["jobs"][0]["state"], "Complete");
    assert_eq!(fs::read(dir.join("output/payload.bin")).unwrap(), data);
}

fn resume_case(no_overwrite: bool, modify_partial: impl FnOnce(&Path, &Path, &mut Vec<usize>)) {
    let dir = TestDir::new();
    let (data, hash) = make_single_file(&dir.0, 3 * PIECE + 19);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let peer = listener.local_addr().unwrap();
    thread::scope(|scope| {
        let seeder = scope.spawn(|| {
            let first = seed(&listener, &data, hash, Seeder::StallAfterTwo);
            (first, seed(&listener, &data, hash, Seeder::Clean))
        });
        let (id, map) = interrupt_download(&dir.0, peer, no_overwrite);
        let checkpoint = checkpoint_pieces(&map);
        let mut verified = checkpoint.clone();
        modify_partial(&dir.0.join("output/payload.bin"), &map, &mut verified);
        let expected_requests: Vec<_> = (0..4).filter(|index| !verified.contains(index)).collect();
        let expected_bytes: usize = verified
            .iter()
            .map(|index| (data.len() - index * PIECE).min(PIECE))
            .sum();

        let mut command = job_command(&dir.0);
        command.args(["--resume-job", &id, "--events"]);
        if no_overwrite {
            command.arg("--no-overwrite");
        }
        let output = wait_for_download(command.spawn().unwrap());
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let (first, second) = seeder.join().unwrap();
        assert_requests(first, &checkpoint);
        assert_requests(second, &expected_requests);
        assert_complete(&dir.0, &output, &data);
        let first_progress = String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .find(|event| event["type"] == "Progress")
            .expect("missing resumed Progress");
        assert_eq!(first_progress["data"]["id"], id);
        assert_eq!(first_progress["data"]["bytes_downloaded"], expected_bytes);
        assert_eq!(first_progress["data"]["bytes_total"], data.len());
    });
}

#[test]
fn resumes_after_kill_without_requesting_verified_pieces() {
    resume_case(false, |_, _, _| {});
}

#[test]
fn requests_corrupt_piece_again_after_kill() {
    resume_case(false, |file, _, verified| {
        let corrupt = verified.remove(0);
        let mut partial = fs::OpenOptions::new().write(true).open(file).unwrap();
        partial
            .seek(SeekFrom::Start((corrupt * PIECE) as u64))
            .unwrap();
        partial.write_all(b"corrupt piece").unwrap();
    });
}

#[test]
fn restarts_from_zero_with_an_invalid_piece_map() {
    for map in [
        json!({"num_pieces": 5, "bitfield": [0xc0]}).to_string(),
        json!({"num_pieces": 4, "bitfield": []}).to_string(),
        json!({"num_pieces": 4, "bitfield": [0xc1]}).to_string(),
        "invalid JSON".to_owned(),
    ] {
        resume_case(false, |_, path, verified| {
            fs::write(path, map).unwrap();
            verified.clear();
        });
    }
}

#[test]
fn no_overwrite_allows_resume_and_protects_an_unrelated_file() {
    resume_case(true, |_, _, _| {});

    let dir = TestDir::new();
    make_single_file(&dir.0, PIECE);
    fs::create_dir(dir.0.join("output")).unwrap();
    let unrelated = dir.0.join("output/payload.bin");
    fs::write(&unrelated, b"unrelated").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let output = wait_for_download(
        download_command(&dir.0, &[listener.local_addr().unwrap()])
            .arg("--no-overwrite")
            .spawn()
            .unwrap(),
    );
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("Output already exists"));
    assert_eq!(fs::read(unrelated).unwrap(), b"unrelated");
    let registry = read_json(dir.0.join("jobs/registry.chorus.json"));
    let id = registry["jobs"][0]["id"].as_str().unwrap();
    assert!(
        !dir.0
            .join(format!("jobs/{id}.chorus.piecemap.json"))
            .exists()
    );
    listener.set_nonblocking(true).unwrap();
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}

#[test]
fn completes_from_a_verified_map_without_connecting() {
    let dir = TestDir::new();
    let (data, hash) = make_single_file(&dir.0, 2 * PIECE + 19);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let peer = listener.local_addr().unwrap();
    thread::scope(|scope| {
        let seeder = scope.spawn(|| seed(&listener, &data, hash, Seeder::StallAfterTwo));
        let (id, map) = interrupt_download(&dir.0, peer, false);
        assert_requests(seeder.join().unwrap(), &checkpoint_pieces(&map));
        fs::write(dir.0.join("output/payload.bin"), &data).unwrap();
        fs::write(
            map,
            json!({"num_pieces": 3, "bitfield": [0xe0]}).to_string(),
        )
        .unwrap();

        let output = wait_for_download(
            job_command(&dir.0)
                .args(["--resume-job", &id])
                .spawn()
                .unwrap(),
        );
        assert_complete(&dir.0, &output, &data);
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
    });
}

#[test]
fn honors_rate_limits_without_timing_out() {
    for (bytes, flag, rate, minimum) in [
        (
            4 * PIECE,
            "--limit-rate",
            "16384",
            Duration::from_millis(2500),
        ),
        (PIECE, "--limit-rate", "3000", Duration::from_secs(5)),
        (
            4 * PIECE,
            "--max-overall-download-limit",
            "16384",
            Duration::from_millis(2500),
        ),
    ] {
        let dir = TestDir::new();
        let (data, hash) = make_single_file(&dir.0, bytes);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let peer = listener.local_addr().unwrap();
        thread::scope(|scope| {
            let seeder = scope.spawn(|| seed(&listener, &data, hash, Seeder::Clean));
            let start = Instant::now();
            let output = wait_for_download(
                download_command(&dir.0, &[peer])
                    .args([flag, rate])
                    .spawn()
                    .unwrap(),
            );
            let elapsed = start.elapsed();
            assert_complete(&dir.0, &output, &data);
            assert_eq!(seeder.join().unwrap().len(), bytes / PIECE);
            assert!(
                elapsed >= minimum,
                "{flag} {rate}: {elapsed:?} < {minimum:?}"
            );
        });
    }
}

#[test]
fn downloads_a_piece_announced_late_by_the_same_peer() {
    let dir = TestDir::new();
    let (data, hash) = make_single_file(&dir.0, 2 * PIECE + 19);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let peer = listener.local_addr().unwrap();
    thread::scope(|scope| {
        let seeder = scope.spawn(|| seed(&listener, &data, hash, Seeder::LateFirstPiece));
        let output = wait_for_download(download_command(&dir.0, &[peer]).spawn().unwrap());
        assert_complete(&dir.0, &output, &data);
        let requested = seeder.join().unwrap();
        assert_eq!(requested.last(), Some(&0));
        assert_requests(requested, &[0, 1, 2]);
    });
}

fn make_tracked_torrent(dir: &Path, data: &[u8], tracker: SocketAddr) -> [u8; 20] {
    let url = format!("http://{tracker}/announce?passkey=local");
    torrent_with_trackers(dir, data, &[&url, &url])
}

fn torrent_with_trackers(dir: &Path, data: &[u8], trackers: &[&str]) -> [u8; 20] {
    let (torrent, hash) = make_torrent(data, &[], false);
    let first = trackers[0];
    let mut tracked = format!("d8:announce{}:{first}13:announce-listl", first.len()).into_bytes();
    for url in trackers {
        write!(tracked, "l{}:{url}e", url.len()).unwrap();
    }
    tracked.push(b'e');
    tracked.extend_from_slice(&torrent[1..]);
    fs::write(dir.join("input.torrent"), tracked).unwrap();
    hash
}

fn udp_tracker_download(wrong_transaction: bool) {
    let dir = TestDir::new();
    let data = vec![42; PIECE + 19];
    let tracker = UdpSocket::bind("127.0.0.1:0").unwrap();
    tracker.set_write_timeout(Some(DEADLINE)).unwrap();
    let seeder = TcpListener::bind("127.0.0.1:0").unwrap();
    let SocketAddr::V4(peer) = seeder.local_addr().unwrap() else {
        unreachable!();
    };
    let url = format!("udp://{}/ignored", tracker.local_addr().unwrap());
    let hash = torrent_with_trackers(&dir.0, &data, &[&url]);
    thread::scope(|scope| {
        let udp = scope.spawn(|| {
            let mut request = [0; 128];
            let id = 12345678u64.to_be_bytes();
            let mut events = Vec::new();
            let deadline = Instant::now() + DEADLINE;
            while let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
                tracker.set_read_timeout(Some(remaining)).unwrap();
                let Ok((len, sender)) = tracker.recv_from(&mut request) else {
                    break;
                };
                match len {
                    16 => {
                        assert_eq!(request[..8], 0x41727101980u64.to_be_bytes());
                        assert_eq!(request[8..12], [0; 4]);
                        let mut reply = [0; 16];
                        reply[4..8].copy_from_slice(&request[12..16]);
                        reply[8..16].copy_from_slice(&id);
                        if wrong_transaction {
                            let mut wrong = reply;
                            wrong[4] ^= 1;
                            wrong[8] ^= 1;
                            tracker.send_to(&wrong, sender).unwrap();
                        }
                        tracker.send_to(&reply, sender).unwrap();
                    }
                    98 if request[..8] == id => {
                        assert_eq!(request[8..12], 1u32.to_be_bytes());
                        assert_eq!(request[16..36], hash);
                        assert_eq!(request[84..88], [0; 4]);
                        assert_eq!(request[92..96], 200u32.to_be_bytes());
                        assert_eq!(request[96..98], 6881u16.to_be_bytes());
                        let event = u32::from_be_bytes(request[80..84].try_into().unwrap());
                        events.push(event);
                        let mut reply = [0; 26];
                        reply[..4].copy_from_slice(&1u32.to_be_bytes());
                        reply[4..8].copy_from_slice(&request[12..16]);
                        reply[8..12].copy_from_slice(&1u32.to_be_bytes());
                        reply[20..24].copy_from_slice(&peer.ip().octets());
                        reply[24..26].copy_from_slice(&peer.port().to_be_bytes());
                        tracker.send_to(&reply, sender).unwrap();
                        if event == 1 {
                            break;
                        }
                    }
                    98 => {} // A tracker silently drops announces with unknown connection IDs.
                    _ => panic!("unexpected UDP request size: {len}"),
                }
            }
            events
        });
        let peer = scope.spawn(|| seed(&seeder, &data, hash, Seeder::Clean));
        let output = wait_for_download(download_command(&dir.0, &[]).spawn().unwrap());
        assert_complete(&dir.0, &output, &data);
        assert_requests(peer.join().unwrap(), &[0, 1]);
        assert_eq!(udp.join().unwrap(), [2, 1]);
    });
}

#[test]
fn downloads_from_udp_tracker_and_announces_completion() {
    udp_tracker_download(false);
}

#[test]
fn ignores_udp_tracker_reply_with_wrong_transaction_id() {
    udp_tracker_download(true);
}

#[test]
fn slow_udp_tracker_does_not_delay_http_peer() {
    let dir = TestDir::new();
    let data = vec![42; PIECE + 19];
    let silent = UdpSocket::bind("127.0.0.1:0").unwrap();
    let tracker = TcpListener::bind("127.0.0.1:0").unwrap();
    let seeder = TcpListener::bind("127.0.0.1:0").unwrap();
    let udp_url = format!("udp://{}", silent.local_addr().unwrap());
    let http_url = format!("http://{}/announce", tracker.local_addr().unwrap());
    let hash = torrent_with_trackers(&dir.0, &data, &[&udp_url, &http_url]);
    let response = compact_response(seeder.local_addr().unwrap());
    thread::scope(|scope| {
        scope.spawn(|| serve_tracker(&tracker, &[&response, &response], true));
        scope.spawn(|| seed(&seeder, &data, hash, Seeder::Clean));
        let start = Instant::now();
        let output = wait_for_download(download_command(&dir.0, &[]).spawn().unwrap());
        assert_complete(&dir.0, &output, &data);
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(10),
            "silent tracker delayed completion: {elapsed:?}"
        );
        silent
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let mut packet = [0; 16];
        assert_eq!(silent.recv(&mut packet).unwrap(), 16);
        assert_eq!(packet[..8], 0x41727101980u64.to_be_bytes());
    });
}

fn serve_tracker(listener: &TcpListener, responses: &[&[u8]], content_length: bool) -> Vec<String> {
    let deadline = Instant::now() + DEADLINE;
    responses
        .iter()
        .map(|body| {
            let mut stream = accept_before(listener, deadline);
            let timeout = Some(Duration::from_secs(5));
            stream.set_read_timeout(timeout).unwrap();
            stream.set_write_timeout(timeout).unwrap();
            let mut headers = BufReader::new(&mut stream).take(8192);
            let mut request = String::new();
            headers.read_line(&mut request).unwrap();
            loop {
                let mut line = String::new();
                let len = headers.read_line(&mut line).unwrap();
                assert_ne!(len, 0, "incomplete tracker request");
                if line == "\r\n" {
                    break;
                }
            }
            let mut response = b"HTTP/1.1 200 OK\r\nConnection: close\r\n".to_vec();
            if content_length {
                write!(response, "Content-Length: {}\r\n", body.len()).unwrap();
            }
            response.extend_from_slice(b"\r\n");
            stream.write_all(&response).unwrap();
            // Chorus can close before the tracker finishes sending an oversized body.
            let _ = stream.write_all(body);
            request
                .split_whitespace()
                .nth(1)
                .expect("missing request target")
                .to_owned()
        })
        .collect()
}

fn compact_response(peer: SocketAddr) -> Vec<u8> {
    let SocketAddr::V4(peer) = peer else {
        panic!("expected a local IPv4 peer");
    };
    let mut response = b"d8:intervali1e5:peers6:".to_vec();
    response.extend_from_slice(&peer.ip().octets());
    response.extend_from_slice(&peer.port().to_be_bytes());
    response.push(b'e');
    response
}

#[test]
fn downloads_from_http_tracker_and_announces_completion() {
    let dir = TestDir::new();
    let data = vec![42; PIECE + 19];
    let tracker = TcpListener::bind("127.0.0.1:0").unwrap();
    let seeder = TcpListener::bind("127.0.0.1:0").unwrap();
    let response = compact_response(seeder.local_addr().unwrap());
    let hash = make_tracked_torrent(&dir.0, &data, tracker.local_addr().unwrap());
    thread::scope(|scope| {
        let http = scope.spawn(|| {
            serve_tracker(
                &tracker,
                &[b"d8:intervali1e5:peers0:e", &response, &response],
                true,
            )
        });
        let peer = scope.spawn(|| seed(&seeder, &data, hash, Seeder::Clean));
        let output = wait_for_download(download_command(&dir.0, &[]).spawn().unwrap());
        assert_complete(&dir.0, &output, &data);
        assert_requests(peer.join().unwrap(), &[0, 1]);
        let requests = http.join().unwrap();
        assert!(!requests[1].contains("event="));
        for (request, event, left, downloaded) in [
            (&requests[0], "started", data.len(), 0),
            (&requests[2], "completed", 0, data.len()),
        ] {
            let query = request.split_once('?').unwrap().1;
            let params: std::collections::HashMap<_, _> =
                url::form_urlencoded::parse(query.as_bytes()).collect();
            // Check the raw 20 bytes as the form parser replaces invalid UTF-8.
            let hash_param = query
                .split('&')
                .find_map(|part| part.strip_prefix("info_hash="))
                .unwrap();
            let mut bytes = hash_param.as_bytes().iter();
            let mut decoded = Vec::new();
            while let Some(&byte) = bytes.next() {
                decoded.push(if byte == b'%' {
                    let hex = [*bytes.next().unwrap(), *bytes.next().unwrap()];
                    u8::from_str_radix(std::str::from_utf8(&hex).unwrap(), 16).unwrap()
                } else {
                    byte
                });
            }
            assert_eq!(decoded, hash);
            assert_eq!(params["passkey"], "local");
            assert_eq!(params["event"], event);
            assert_eq!(params["compact"], "1");
            assert_eq!(params["port"], "6881");
            assert_eq!(params["uploaded"], "0");
            assert_eq!(params["left"], left.to_string());
            assert_eq!(params["downloaded"], downloaded.to_string());
        }
    });
}

fn rejects_oversized_tracker_response(content_length: bool) {
    let dir = TestDir::new();
    let data = vec![42; PIECE];
    let tracker = TcpListener::bind("127.0.0.1:0").unwrap();
    let seeder = TcpListener::bind("127.0.0.1:0").unwrap();
    let hash = make_tracked_torrent(&dir.0, &data, tracker.local_addr().unwrap());
    let mut response = b"d8:intervali1e7:padding1048576:".to_vec();
    response.resize(response.len() + 1024 * 1024, b'x');
    let compact = compact_response(seeder.local_addr().unwrap());
    response.extend_from_slice(&compact[b"d8:intervali1e".len()..]);
    let stop_response = b"d8:intervali1e5:peers0:e";
    let finished = AtomicBool::new(false);
    thread::scope(|scope| {
        let http = scope
            .spawn(move || serve_tracker(&tracker, &[&response, stop_response], content_length));
        let peer = scope.spawn(|| {
            seeder.set_nonblocking(true).unwrap();
            let deadline = Instant::now() + DEADLINE;
            while !finished.load(Ordering::Relaxed) && Instant::now() < deadline {
                match seeder.accept() {
                    Ok((stream, _)) => {
                        return seed_connected(stream, &data, hash, Seeder::Clean, deadline);
                    }
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
                    Err(err) => panic!("accept failed: {err}"),
                }
                thread::sleep(Duration::from_millis(10));
            }
            Vec::new()
        });
        let output = wait_for_download(download_command(&dir.0, &[]).spawn().unwrap());
        finished.store(true, Ordering::Relaxed);
        let requests = http.join().unwrap();
        let downloaded_pieces = peer.join().unwrap();
        assert!(
            !output.status.success(),
            "Content-Length={content_length}: oversized response completed the download ({downloaded_pieces:?})"
        );
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("Tracker response exceeds 1 MiB"),
            "Content-Length={content_length}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            read_json(dir.0.join("jobs/registry.chorus.json"))["jobs"][0]["state"],
            json!({"Failed": "Network"})
        );
        assert!(requests[1].contains("event=stopped"));
        assert!(downloaded_pieces.is_empty());
        assert_eq!(
            seeder.accept().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
    });
}

#[test]
fn rejects_oversized_tracker_response_with_content_length() {
    rejects_oversized_tracker_response(true);
}

#[test]
fn rejects_oversized_tracker_response_without_content_length() {
    rejects_oversized_tracker_response(false);
}

#[test]
fn manual_peer_works_when_tracker_rejects_announce() {
    let dir = TestDir::new();
    let data = vec![42; PIECE];
    let tracker = TcpListener::bind("127.0.0.1:0").unwrap();
    let seeder = TcpListener::bind("127.0.0.1:0").unwrap();
    let peer = seeder.local_addr().unwrap();
    let hash = make_tracked_torrent(&dir.0, &data, tracker.local_addr().unwrap());
    let response = b"d14:failure reason6:deniede";
    thread::scope(|scope| {
        let http = scope.spawn(|| serve_tracker(&tracker, &[response, response], true));
        let seed = scope.spawn(|| seed(&seeder, &data, hash, Seeder::Clean));
        let output = wait_for_download(download_command(&dir.0, &[peer]).spawn().unwrap());
        assert_complete(&dir.0, &output, &data);
        assert_eq!(seed.join().unwrap(), [0]);
        let requests = http.join().unwrap();
        assert!(requests[0].contains("event=started"));
        assert!(requests[1].contains("event=completed"));
    });
}

#[test]
fn downloads_from_peer_with_have_and_no_bitfield() {
    let dir = TestDir::new();
    let (data, hash) = make_single_file(&dir.0, 2 * PIECE + 19);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let peer = listener.local_addr().unwrap();
    thread::scope(|scope| {
        let seeder = scope.spawn(|| seed(&listener, &data, hash, Seeder::HaveOnly));
        let output = wait_for_download(download_command(&dir.0, &[peer]).spawn().unwrap());
        assert_complete(&dir.0, &output, &data);
        let requested = seeder.join().unwrap();
        assert_eq!(requested.last(), Some(&0));
        assert_requests(requested, &[0, 1, 2]);
    });
}

fn magnet_metadata(data: &[u8]) -> (Vec<u8>, [u8; 20]) {
    let (torrent, _) = make_torrent(data, &[], false);
    let mut info = torrent[7..torrent.len() - 2].to_vec();
    write!(info, "1:z{}:", 20 * 1024).unwrap();
    info.extend_from_slice(&[b'x'; 20 * 1024]);
    info.push(b'e');
    let hash = Sha1::digest(&info).into();
    (info, hash)
}

fn magnet_link(hash: [u8; 20]) -> String {
    let hash: String = hash.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("magnet:?xt=urn:btih:{hash}&dn=..%2Fignored.bin")
}

fn magnet_command(dir: &Path, link: &str, peers: &[SocketAddr]) -> Command {
    let mut command = job_command(dir);
    command
        .args(["--magnet", link, "-o"])
        .arg(dir.join("output"));
    for peer in peers {
        command.arg("--peer").arg(peer.to_string());
    }
    command
}

fn seed_metadata(
    listener: &TcpListener,
    hash: [u8; 20],
    extension_id: u8,
    advertised_size: usize,
    info: &[u8],
) {
    let mut stream = accept_before(listener, Instant::now() + DEADLINE);
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.set_nodelay(true).unwrap();
    let mut handshake = [0; 68];
    stream.read_exact(&mut handshake).unwrap();
    assert_eq!(&handshake[..20], b"\x13BitTorrent protocol");
    assert_eq!(&handshake[28..48], &hash);
    assert_eq!(handshake[25] & 0x10, 0x10, "missing BEP 10 support");
    assert_eq!(handshake[27] & 0x04, 0, "metadata handshake without BEP 6");
    handshake[48..].fill(b'S');
    stream.write_all(&handshake).unwrap();
    assert_eq!(
        read_message(&mut stream).unwrap(),
        b"\x14\x00d1:md11:ut_metadatai1eee"
    );
    let mut hello = vec![0];
    write!(
        hello,
        "d1:md11:ut_metadatai{extension_id}ee13:metadata_sizei{advertised_size}ee"
    )
    .unwrap();
    send_message(&mut stream, 20, &hello);
    for (index, piece) in info.chunks(PIECE).enumerate() {
        let mut request = vec![20, extension_id];
        write!(request, "d8:msg_typei0e5:piecei{index}ee").unwrap();
        assert_eq!(read_message(&mut stream).unwrap(), request);
        let mut reply = vec![1];
        write!(
            reply,
            "d8:msg_typei1e5:piecei{index}e10:total_sizei{advertised_size}ee"
        )
        .unwrap();
        reply.extend_from_slice(piece);
        send_message(&mut stream, 20, &reply);
    }
    let error = read_message(&mut stream).expect_err("unexpected additional metadata request");
    assert!(matches!(
        error.kind(),
        io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset
    ));
}

#[test]
fn magnet_downloads_two_metadata_pieces_with_distinct_extension_ids() {
    let dir = TestDir::new();
    let data = vec![42; PIECE + 19];
    let (info, hash) = magnet_metadata(&data);
    let link = magnet_link(hash);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    thread::scope(|scope| {
        let peer = scope.spawn(|| {
            assert_eq!(info.chunks(PIECE).count(), 2);
            seed_metadata(&listener, hash, 3, info.len(), &info);
            seed(&listener, &data, hash, Seeder::Clean)
        });
        let output = wait_for_download(
            magnet_command(&dir.0, &link, &[listener.local_addr().unwrap()])
                .spawn()
                .unwrap(),
        );
        assert_complete(&dir.0, &output, &data);
        assert_requests(peer.join().unwrap(), &[0, 1]);
    });
    let jobs = dir.0.join("jobs");
    let registry = read_json(jobs.join("registry.chorus.json"));
    let id = registry["jobs"][0]["id"].as_str().unwrap();
    let filename = format!("{id}.torrent");
    let private = read_json(jobs.join(format!("{id}.chorus.private.json")));
    assert_eq!(private["torrent"], filename);
    assert_eq!(private["magnet"], link);
    let manifest = read_json(jobs.join(format!("{id}.chorus.json")));
    let hash_hex: String = hash.iter().map(|byte| format!("{byte:02x}")).collect();
    assert_eq!(manifest["sources"], json!([format!("torrent:{hash_hex}")]));
    let saved = fs::read(jobs.join(filename)).unwrap();
    let saved_info = saved
        .strip_prefix(b"d13:announce-listle4:info")
        .unwrap()
        .strip_suffix(b"e")
        .unwrap();
    assert_eq!(saved_info, info);
    assert_eq!(<[u8; 20]>::from(Sha1::digest(saved_info)), hash);
}

#[test]
fn magnet_skips_peer_advertising_one_gibibyte() {
    let dir = TestDir::new();
    let data = vec![42; PIECE + 19];
    let (info, hash) = magnet_metadata(&data);
    let oversized = TcpListener::bind("127.0.0.1:0").unwrap();
    let honest = TcpListener::bind("127.0.0.1:0").unwrap();
    let peers = [
        oversized.local_addr().unwrap(),
        honest.local_addr().unwrap(),
    ];
    thread::scope(|scope| {
        scope.spawn(|| {
            seed_metadata(&oversized, hash, 3, 1024 * 1024 * 1024, &[]);
            drop(oversized);
            seed_metadata(&honest, hash, 9, info.len(), &info);
            seed(&honest, &data, hash, Seeder::Clean)
        });
        let output = wait_for_download(
            magnet_command(&dir.0, &magnet_link(hash), &peers)
                .spawn()
                .unwrap(),
        );
        assert_complete(&dir.0, &output, &data);
    });
}

#[test]
fn magnet_rejects_metadata_with_wrong_sha1() {
    let data = vec![42; PIECE + 19];
    let (info, hash) = magnet_metadata(&data);
    let mut wrong = info.clone();
    wrong[info.len() - 2] ^= 1;
    for fallback in [true, false] {
        let dir = TestDir::new();
        let liar = TcpListener::bind("127.0.0.1:0").unwrap();
        let honest = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut peers = vec![liar.local_addr().unwrap()];
        if fallback {
            peers.push(honest.local_addr().unwrap());
        }
        thread::scope(|scope| {
            scope.spawn(|| {
                seed_metadata(&liar, hash, 3, wrong.len(), &wrong);
                drop(liar);
                if fallback {
                    seed_metadata(&honest, hash, 9, info.len(), &info);
                    assert_requests(seed(&honest, &data, hash, Seeder::Clean), &[0, 1]);
                }
            });
            let output = wait_for_download(
                magnet_command(&dir.0, &magnet_link(hash), &peers)
                    .spawn()
                    .unwrap(),
            );
            if fallback {
                assert_complete(&dir.0, &output, &data);
            } else {
                assert!(!output.status.success());
                assert!(String::from_utf8_lossy(&output.stderr).contains("info hash mismatch"));
                let jobs = dir.0.join("jobs");
                let registry = read_json(jobs.join("registry.chorus.json"));
                assert_eq!(registry["jobs"][0]["state"], json!({"Failed": "Network"}));
                let id = registry["jobs"][0]["id"].as_str().unwrap();
                let private = read_json(jobs.join(format!("{id}.chorus.private.json")));
                assert!(private["torrent"].is_null());
                assert!(!jobs.join(format!("{id}.torrent")).exists());
            }
        });
    }
}

#[test]
fn magnet_uses_http_tracker_with_positive_left_for_metadata() {
    let dir = TestDir::new();
    let data = vec![42; PIECE + 19];
    let (info, hash) = magnet_metadata(&data);
    let tracker = TcpListener::bind("127.0.0.1:0").unwrap();
    let seeder = TcpListener::bind("127.0.0.1:0").unwrap();
    let tracker_url = format!("http://{}/announce", tracker.local_addr().unwrap());
    let link = magnet_link(hash)
        + "&"
        + &url::form_urlencoded::Serializer::new(String::new())
            .append_pair("tr", &tracker_url)
            .finish();
    let response = compact_response(seeder.local_addr().unwrap());
    thread::scope(|scope| {
        let http =
            scope.spawn(|| serve_tracker(&tracker, &[&response, &response, &response], true));
        scope.spawn(|| {
            seed_metadata(&seeder, hash, 3, info.len(), &info);
            seed(&seeder, &data, hash, Seeder::Clean)
        });
        let output = wait_for_download(magnet_command(&dir.0, &link, &[]).spawn().unwrap());
        assert_complete(&dir.0, &output, &data);
        let requests = http.join().unwrap();
        let query = requests[0].split_once('?').unwrap().1;
        let params: std::collections::HashMap<_, _> =
            url::form_urlencoded::parse(query.as_bytes()).collect();
        // Check the raw bytes since the form parser replaces invalid UTF-8.
        let expected: String = hash
            .iter()
            .map(|byte| {
                if byte.is_ascii_alphanumeric() || b"-._~".contains(byte) {
                    char::from(*byte).to_string()
                } else {
                    format!("%{byte:02X}")
                }
            })
            .collect();
        assert!(
            query
                .split('&')
                .any(|pair| pair.strip_prefix("info_hash=") == Some(&expected))
        );
        assert_eq!(params["event"], "started");
        assert!(params["left"].parse::<u64>().unwrap() > 0);
    });
}

#[test]
fn magnet_resumes_from_saved_torrent_without_requesting_metadata() {
    let dir = TestDir::new();
    let data = vec![42; 3 * PIECE + 19];
    let (info, hash) = magnet_metadata(&data);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let peers = [listener.local_addr().unwrap()];
    thread::scope(|scope| {
        let peer = scope.spawn(|| {
            seed_metadata(&listener, hash, 3, info.len(), &info);
            let first = seed(&listener, &data, hash, Seeder::StallAfterTwo);
            (first, seed(&listener, &data, hash, Seeder::Clean))
        });
        let child = magnet_command(&dir.0, &magnet_link(hash), &peers)
            .spawn()
            .unwrap();
        let (id, map) = interrupt_at_checkpoint(&dir.0, child, true);
        let checkpoint = checkpoint_pieces(&map);
        let output = wait_for_download(
            job_command(&dir.0)
                .args(["--resume-job", &id])
                .spawn()
                .unwrap(),
        );
        assert_complete(&dir.0, &output, &data);
        let (first, second) = peer.join().unwrap();
        assert_requests(first, &checkpoint);
        let missing: Vec<_> = (0..4).filter(|index| !checkpoint.contains(index)).collect();
        assert_requests(second, &missing);
    });
}

#[test]
fn waits_for_slow_started_before_announcing_completion() {
    let dir = TestDir::new();
    let data = vec![42; PIECE];
    let tracker = TcpListener::bind("127.0.0.1:0").unwrap();
    let seeder = TcpListener::bind("127.0.0.1:0").unwrap();
    let hash = make_tracked_torrent(&dir.0, &data, tracker.local_addr().unwrap());
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (downloaded_tx, downloaded_rx) = std::sync::mpsc::channel();
    thread::scope(|scope| {
        let http = scope.spawn(move || {
            let mut stream = accept_before(&tracker, Instant::now() + DEADLINE);
            stream.set_read_timeout(Some(DEADLINE)).unwrap();
            let mut headers = BufReader::new(&mut stream).take(8192);
            let mut request = String::new();
            headers.read_line(&mut request).unwrap();
            assert!(request.contains("event=started"), "{request}");
            loop {
                let mut line = String::new();
                assert_ne!(headers.read_line(&mut line).unwrap(), 0, "incomplete started");
                if line == "\r\n" {
                    break;
                }
            }
            started_tx.send(()).unwrap();
            downloaded_rx.recv_timeout(DEADLINE).unwrap();
            stream.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
            let error = stream.read(&mut [0]).expect_err("started was dropped while awaiting its reply");
            assert!(matches!(error.kind(), io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock), "{error}");
            stream.write_all(b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 24\r\n\r\nd8:intervali1e5:peers0:e").unwrap();
            let completed = serve_tracker(&tracker, &[b"d8:intervali1e5:peers0:e"], true);
            assert!(completed[0].contains("event=completed"), "{completed:?}");
        });
        let peer = seeder.local_addr().unwrap();
        let data = &data;
        let seeded = scope.spawn(move || {
            started_rx.recv_timeout(DEADLINE).unwrap();
            assert_eq!(seed(&seeder, data, hash, Seeder::Clean), [0]);
            let finished = Instant::now();
            downloaded_tx.send(()).unwrap();
            finished
        });
        let output = wait_for_download(download_command(&dir.0, &[peer]).spawn().unwrap());
        let finished = Instant::now();
        assert_complete(&dir.0, &output, data);
        assert!(
            finished
                .checked_duration_since(seeded.join().unwrap())
                .unwrap_or_default()
                < Duration::from_secs(5)
        );
        http.join().unwrap();
    });
}

fn accept_payload_peer(listener: &TcpListener, hash: [u8; 20]) -> TcpStream {
    payload_handshake(accept_before(listener, Instant::now() + DEADLINE), hash)
}

fn payload_handshake(mut stream: TcpStream, hash: [u8; 20]) -> TcpStream {
    stream.set_read_timeout(Some(DEADLINE)).unwrap();
    stream.set_write_timeout(Some(DEADLINE)).unwrap();
    stream.set_nodelay(true).unwrap();
    let mut handshake = [0; 68];
    stream.read_exact(&mut handshake).unwrap();
    assert_eq!(&handshake[28..48], &hash);
    // Answer as a plain BitTorrent peer: no BEP 6 or BEP 10 on this connection.
    handshake[20..28].fill(0);
    handshake[48..].fill(b'S');
    stream.write_all(&handshake).unwrap();
    assert_eq!(read_message(&mut stream).unwrap(), [2]);
    stream
}

#[test]
fn rejects_giant_peer_frame_without_waiting_for_body() {
    let dir = TestDir::new();
    let (_, hash) = make_single_file(&dir.0, PIECE);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let peer = listener.local_addr().unwrap();
    let child = download_command(&dir.0, &[peer]).spawn().unwrap();
    let mut stream = accept_payload_peer(&listener, hash);
    let sent = Instant::now();
    stream.write_all(&u32::MAX.to_be_bytes()).unwrap();
    let output = wait_for_download(child);
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(error.contains("Message too large"), "{error}");
    assert!(sent.elapsed() < Duration::from_secs(1), "{error}");
}

#[test]
fn cancelling_a_silent_peer_saves_verified_pieces_promptly() {
    let dir = TestDir::new();
    let (data, hash) = make_single_file(&dir.0, 2 * PIECE);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let peer = listener.local_addr().unwrap();
    let child = download_command(&dir.0, &[peer]).spawn().unwrap();
    let mut stream = accept_payload_peer(&listener, hash);
    send_message(&mut stream, 5, &[0xc0]);
    send_message(&mut stream, 1, &[]);
    let first = read_message(&mut stream).unwrap();
    let index = request_index(&first);
    assert!(index < 2);
    let mut body = first[1..9].to_vec();
    body.extend_from_slice(&data[index * PIECE..(index + 1) * PIECE]);
    send_message(&mut stream, 7, &body);
    let second = read_message(&mut stream).unwrap();
    assert_eq!(request_index(&second), 1 - index);

    let jobs = dir.0.join("jobs");
    let registry = read_json(jobs.join("registry.chorus.json"));
    let id = registry["jobs"][0]["id"].as_str().unwrap();
    let map = jobs.join(format!("{id}.chorus.piecemap.json"));
    fs::remove_file(&map).unwrap();

    let cancelled = job_command(&dir.0)
        .args(["--cancel-job", id])
        .output()
        .unwrap();
    assert!(
        cancelled.status.success(),
        "{}",
        String::from_utf8_lossy(&cancelled.stderr)
    );
    let cancel = Instant::now();
    let _ = wait_for_download(child);
    assert!(
        cancel.elapsed() < Duration::from_secs(1),
        "cancellation took {:?}",
        cancel.elapsed()
    );
    assert_eq!(
        read_json(jobs.join("registry.chorus.json"))["jobs"][0]["state"],
        "Cancelled"
    );
    assert_eq!(read_json(map)["bitfield"], json!([0x80_u8 >> index]));
}

const SWARM_PIECE: usize = 2 * PIECE;

struct SwarmSeeder {
    delay: Duration,
    corrupt: bool,
    vanish: bool,
}

#[derive(Default)]
struct SwarmDelivery {
    requests: Vec<(Instant, Instant)>,
    pieces: usize,
}

fn seed_swarm(
    listener: TcpListener,
    data: &[u8],
    hash: [u8; 20],
    seeder: SwarmSeeder,
    ready: &AtomicBool,
    stopped: &AtomicBool,
) -> SwarmDelivery {
    let mut delivery = SwarmDelivery::default();
    listener.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + DEADLINE;
    let stream = loop {
        if stopped.load(Ordering::Relaxed) || Instant::now() >= deadline {
            return delivery;
        }
        if !ready.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(2));
            continue;
        }
        match listener.accept() {
            Ok((stream, _)) => break stream,
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(2));
            }
            Err(err) => panic!("accept failed: {err}"),
        }
    };
    stream.set_nonblocking(false).unwrap();
    let mut stream = payload_handshake(stream, hash);
    send_message(&mut stream, 5, &[0xff, 0xff]);
    send_message(&mut stream, 1, &[]);

    loop {
        let request = match read_message(&mut stream) {
            Ok(request) => request,
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::UnexpectedEof
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::ConnectionAborted
                ) =>
            {
                return delivery;
            }
            Err(err) => panic!("read failed: {err}"),
        };
        if request[0] == 8 {
            continue;
        }
        let index = request_index(&request);
        let start = Instant::now();
        let begin = u32::from_be_bytes(request[5..9].try_into().unwrap()) as usize;
        let length = u32::from_be_bytes(request[9..13].try_into().unwrap()) as usize;
        let offset = index * SWARM_PIECE + begin;
        let mut frame = ((9 + length) as u32).to_be_bytes().to_vec();
        frame.push(7);
        frame.extend_from_slice(&request[1..9]);
        frame.extend_from_slice(&data[offset..offset + length]);
        if seeder.corrupt {
            frame[13] ^= 1;
        }
        thread::sleep(seeder.delay);
        if let Err(err) = stream.write_all(&frame) {
            assert!(
                matches!(
                    err.kind(),
                    io::ErrorKind::BrokenPipe
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::ConnectionAborted
                ),
                "{err}"
            );
            return delivery;
        }
        delivery.requests.push((start, Instant::now()));
        delivery.pieces += usize::from(begin + length == SWARM_PIECE);
        if seeder.vanish {
            return delivery;
        }
    }
}

fn run_swarm<T>(
    seeders: [SwarmSeeder; 4],
    download: impl FnOnce(&Path, &[SocketAddr], &AtomicBool) -> T,
) -> (Vec<SwarmDelivery>, T, usize) {
    let dir = TestDir::new();
    let data: Vec<_> = (0..16 * SWARM_PIECE).map(|i| (i % 251) as u8).collect();
    let (torrent, hash) = make_torrent_with_piece_length(&data, &[], false, SWARM_PIECE);
    fs::write(dir.0.join("input.torrent"), torrent).unwrap();

    let listeners = seeders.map(|seeder| (TcpListener::bind("127.0.0.1:0").unwrap(), seeder));
    let peers: Vec<_> = listeners
        .iter()
        .map(|(socket, _)| socket.local_addr().unwrap())
        .collect();
    let ready = AtomicBool::new(false);
    let stopped = AtomicBool::new(false);
    let (observed, deliveries) = thread::scope(|scope| {
        let seeders: Vec<_> = listeners
            .into_iter()
            .map(|(listener, seeder)| {
                let (data, ready, stopped) = (&data, &ready, &stopped);
                scope.spawn(move || seed_swarm(listener, data, hash, seeder, ready, stopped))
            })
            .collect();
        let observed = download(&dir.0, &peers, &ready);
        stopped.store(true, Ordering::Relaxed);
        (
            observed,
            seeders
                .into_iter()
                .map(|seeder| seeder.join().unwrap())
                .collect(),
        )
    });
    let registry = read_json(dir.0.join("jobs/registry.chorus.json"));
    assert_eq!(registry["jobs"][0]["state"], "Complete");
    assert_eq!(
        Sha256::digest(fs::read(dir.0.join("output/payload.bin")).unwrap()),
        Sha256::digest(&data)
    );
    (deliveries, observed, data.len())
}

#[tokio::main(flavor = "current_thread")]
async fn observe_swarm_progress(dir: &Path, peers: &[SocketAddr], ready: &AtomicBool) -> Vec<u64> {
    use futures::StreamExt;
    use tokio::io::AsyncBufReadExt;

    tokio::time::timeout(DEADLINE, async {
        let mut daemon = tokio::process::Command::from(download_command(dir, peers))
            .args(["--rpc-bind", "127.0.0.1:0", "--allow-private-network"])
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut stdout = tokio::io::BufReader::new(daemon.stdout.take().unwrap()).lines();
        let address = loop {
            let line = stdout
                .next_line()
                .await
                .unwrap()
                .expect("RPC startup address");
            if let Some(address) = line.strip_prefix("Chorus: RPC/WebSocket Server listening on ") {
                break address.to_owned();
            }
        };
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{address}"))
            .await
            .unwrap();
        ready.store(true, Ordering::Relaxed);

        let mut progress = Vec::new();
        let mut previous_seq = None;
        loop {
            let message = ws
                .next()
                .await
                .expect("WebSocket closed before Complete")
                .unwrap();
            let event: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
            let seq = event["seq"].as_u64().unwrap();
            if let Some(previous) = previous_seq {
                assert_eq!(seq, previous + 1, "WebSocket missed an event");
            }
            previous_seq = Some(seq);
            match event["type"].as_str() {
                Some("Progress") => {
                    progress.push(event["data"]["bytes_downloaded"].as_u64().unwrap())
                }
                Some("StateChanged") if event["data"]["to"] == "Complete" => {
                    daemon.kill().await.unwrap();
                    return progress;
                }
                Some("StateChanged") if event["data"]["to"].get("Failed").is_some() => {
                    panic!("swarm job failed: {event}");
                }
                _ => {}
            }
        }
    })
    .await
    .expect("swarm timed out before Complete")
}

#[test]
fn f13a_enxame_honesto_baixa_em_paralelo() {
    let (deliveries, (), _) = run_swarm(
        std::array::from_fn(|_| SwarmSeeder {
            delay: Duration::from_millis(30),
            corrupt: false,
            vanish: false,
        }),
        |dir, peers, ready| {
            ready.store(true, Ordering::Relaxed);
            let output = wait_for_download(download_command(dir, peers).spawn().unwrap());
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
        },
    );
    let peak = deliveries
        .iter()
        .flat_map(|delivery| &delivery.requests)
        .map(|(start, _)| {
            deliveries
                .iter()
                .filter(|delivery| {
                    delivery
                        .requests
                        .iter()
                        .any(|(from, to)| from <= start && start < to)
                })
                .count()
        })
        .max()
        .unwrap_or(0);
    let pieces: Vec<_> = deliveries.iter().map(|delivery| delivery.pieces).collect();
    println!(
        "16 peças, SHA-256 ok, pico de aparelhos com pedido em aberto={peak}, entregas={pieces:?}"
    );
    assert!(peak >= 2, "expected simultaneous requests, peak={peak}");
    assert!(pieces.iter().all(|&count| count > 0), "{pieces:?}");
}

#[test]
fn f13a_enxame_misturado_termina_com_progresso_monotono() {
    let (deliveries, progress, total) = run_swarm(
        [
            SwarmSeeder {
                delay: Duration::from_millis(30),
                corrupt: false,
                vanish: false,
            },
            SwarmSeeder {
                delay: Duration::from_millis(300),
                corrupt: false,
                vanish: false,
            },
            SwarmSeeder {
                delay: Duration::from_millis(15),
                corrupt: false,
                vanish: true,
            },
            SwarmSeeder {
                delay: Duration::from_millis(10),
                corrupt: true,
                vanish: false,
            },
        ],
        observe_swarm_progress,
    );
    let pieces: Vec<_> = deliveries.iter().map(|delivery| delivery.pieces).collect();
    let blocks: Vec<_> = deliveries
        .iter()
        .map(|delivery| delivery.requests.len())
        .collect();
    println!("SHA-256 ok, peças={pieces:?}, blocos={blocks:?}, Progress={progress:?}");
    assert!(blocks.iter().all(|&count| count > 0), "{blocks:?}");
    assert_eq!(blocks[2], 1);
    assert!(
        (1..=3).contains(&pieces[3]),
        "hostile peer received {} pieces",
        pieces[3]
    );
    assert_eq!(progress.len(), 16, "WebSocket missed a piece's Progress");
    assert!(progress.windows(2).all(|pair| pair[0] <= pair[1]));
    assert_eq!(progress.last(), Some(&(total as u64)));
}

#[test]
fn f13a_cancelar_tres_aparelhos_salva_a_peca() {
    let dir = TestDir::new();
    let (data, hash) = make_single_file(&dir.0, 2 * PIECE);
    let listeners: [_; 3] = std::array::from_fn(|_| TcpListener::bind("127.0.0.1:0").unwrap());
    let peers = listeners
        .each_ref()
        .map(|listener| listener.local_addr().unwrap());
    let child = download_command(&dir.0, &peers).spawn().unwrap();
    let [mut seeder, _silent1, _silent2] = listeners
        .each_ref()
        .map(|listener| accept_payload_peer(listener, hash));

    send_message(&mut seeder, 5, &[0xc0]);
    send_message(&mut seeder, 1, &[]);
    let first = read_message(&mut seeder).unwrap();
    let index = request_index(&first);
    assert!(index < 2);
    let mut body = first[1..9].to_vec();
    body.extend_from_slice(&data[index * PIECE..(index + 1) * PIECE]);
    send_message(&mut seeder, 7, &body);
    assert_eq!(
        request_index(&read_message(&mut seeder).unwrap()),
        1 - index
    );

    let jobs = dir.0.join("jobs");
    let registro = jobs.join("registry.chorus.json");
    let registry = read_json(registro.clone());
    let id = registry["jobs"][0]["id"].as_str().unwrap();
    let cancel = job_command(&dir.0)
        .args(["--cancel-job", id])
        .output()
        .unwrap();
    assert!(
        cancel.status.success(),
        "{}",
        String::from_utf8_lossy(&cancel.stderr)
    );
    let start = Instant::now();
    let _ = wait_for_download(child);
    let elapsed = start.elapsed();
    let saved = read_json(jobs.join(format!("{id}.chorus.piecemap.json")));
    println!(
        "3 aparelhos, cancelamento em {} ms, peça={index}, mapa={}",
        elapsed.as_millis(),
        saved["bitfield"]
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "cancellation took {elapsed:?}"
    );
    assert_eq!(saved["bitfield"], json!([0x80_u8 >> index]));
    assert_eq!(read_json(registro)["jobs"][0]["state"], "Cancelled");
}

fn borda_fim_normal(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::UnexpectedEof
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
    )
}

fn borda_resposta_tracker(peers: &[SocketAddr]) -> Vec<u8> {
    let mut reply = format!("d8:intervali1e5:peers{}:", 6 * peers.len()).into_bytes();
    for address in peers {
        let SocketAddr::V4(address) = address else {
            unreachable!()
        };
        reply.extend_from_slice(&address.ip().octets());
        reply.extend_from_slice(&address.port().to_be_bytes());
    }
    reply.push(b'e');
    reply
}

#[derive(Clone, Copy)]
enum BordaSeeder {
    Hostil,
    Fecha,
    Honesto,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct BordaPedido {
    index: usize,
    begin: usize,
    length: usize,
}

impl BordaPedido {
    fn from_wire(message: &[u8]) -> Self {
        assert_eq!(message.len(), 13);
        let field =
            |offset| u32::from_be_bytes(message[offset..offset + 4].try_into().unwrap()) as usize;
        Self {
            index: field(1),
            begin: field(5),
            length: field(9),
        }
    }

    fn responder(
        self,
        stream: &mut TcpStream,
        data: &[u8],
        piece_length: usize,
        corrupt: bool,
    ) -> io::Result<()> {
        let offset = self.index * piece_length + self.begin;
        let mut frame = Vec::with_capacity(13 + self.length);
        frame.extend_from_slice(&((self.length + 9) as u32).to_be_bytes());
        frame.push(7);
        frame.extend_from_slice(&(self.index as u32).to_be_bytes());
        frame.extend_from_slice(&(self.begin as u32).to_be_bytes());
        frame.extend_from_slice(&data[offset..offset + self.length]);
        if corrupt {
            frame[13] ^= 1;
        }
        stream.write_all(&frame)
    }
}

fn borda_semeador_rodadas(
    listener: &TcpListener,
    data: &[u8],
    hash: [u8; 20],
    papel: BordaSeeder,
    acabou: &AtomicBool,
) -> usize {
    listener.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + DEADLINE;
    let mut aceitos = 0;
    while !acabou.load(Ordering::Relaxed) && Instant::now() < deadline {
        let (stream, _) = match listener.accept() {
            Ok(accepted) => accepted,
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(2));
                continue;
            }
            Err(err) => panic!("accept failed: {err}"),
        };
        aceitos += 1;
        stream.set_nonblocking(false).unwrap();
        let mut stream = payload_handshake(stream, hash);
        if matches!(papel, BordaSeeder::Fecha) {
            continue;
        }
        if matches!(papel, BordaSeeder::Honesto) {
            // Keep the second round open long enough to observe any forbidden reconnect.
            thread::sleep(Duration::from_millis(150));
        }
        send_message(&mut stream, 5, &[0xf0]);
        send_message(&mut stream, 1, &[]);
        loop {
            let message = match read_message(&mut stream) {
                Ok(message) => message,
                Err(err) if borda_fim_normal(&err) => break,
                Err(err) => panic!("peer read failed: {err}"),
            };
            if message[0] == 8 {
                continue;
            }
            assert_eq!(message[0], 6);
            let request = BordaPedido::from_wire(&message);
            let corrupt = matches!(papel, BordaSeeder::Hostil);
            if let Err(err) = request.responder(&mut stream, data, PIECE, corrupt) {
                assert!(
                    borda_fim_normal(&err) || err.kind() == io::ErrorKind::BrokenPipe,
                    "{err}"
                );
                break;
            }
        }
    }
    aceitos
}

fn borda_provar_banidos(banir_ip: bool) {
    use BordaSeeder::{Fecha, Honesto, Hostil};

    let dir = TestDir::new();
    let data: Vec<_> = (0..4 * PIECE).map(|i| (i % 251) as u8).collect();
    let tracker = TcpListener::bind("127.0.0.1:0").unwrap();
    let hash = make_tracked_torrent(&dir.0, &data, tracker.local_addr().unwrap());
    let papeis = if banir_ip {
        vec![Hostil, Hostil, Hostil, Honesto]
    } else {
        vec![Hostil, Fecha, Honesto]
    };
    let listeners: Vec<_> = papeis
        .iter()
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let peers: Vec<_> = listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap())
        .collect();
    let (honesto, rodada1) = peers.split_last().unwrap();
    let rodada1 = borda_resposta_tracker(rodada1);
    let rodada2 = if banir_ip {
        borda_resposta_tracker(&[*honesto])
    } else {
        borda_resposta_tracker(&peers)
    };
    let vazio = borda_resposta_tracker(&[]);
    let acabou = AtomicBool::new(false);
    let (output, aceitos, pedidos) = thread::scope(|scope| {
        let tracker = scope.spawn(|| {
            let mut pedidos = Vec::new();
            for resposta in [&rodada1, &rodada2, &vazio, &vazio] {
                let request = serve_tracker(&tracker, &[resposta], true).pop().unwrap();
                let terminal =
                    request.contains("event=completed") || request.contains("event=stopped");
                pedidos.push(request);
                if terminal {
                    break;
                }
            }
            pedidos
        });
        let aparelhos: Vec<_> = listeners
            .iter()
            .zip(papeis)
            .map(|(listener, papel)| {
                let (data, acabou) = (&data, &acabou);
                scope.spawn(move || borda_semeador_rodadas(listener, data, hash, papel, acabou))
            })
            .collect();
        let output = wait_for_download(download_command(&dir.0, &[]).spawn().unwrap());
        acabou.store(true, Ordering::Relaxed);
        let aceitos: Vec<_> = aparelhos
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        (output, aceitos, tracker.join().unwrap())
    });
    println!(
        "Aceitos por aparelho={aceitos:?}, pedidos ao tracker={}",
        pedidos.len()
    );
    assert!(pedidos[0].contains("event=started"));
    assert!(!pedidos[1].contains("event="));
    if banir_ip {
        assert_eq!(
            aceitos,
            [1, 1, 1, 0],
            "o IP banido aceitou uma conexão nova"
        );
        assert_eq!(pedidos.len(), 4);
        assert!(pedidos[3].contains("event=stopped"));
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("peer sent bad pieces"), "{stderr}");
        let registry = read_json(dir.0.join("jobs/registry.chorus.json"));
        assert_eq!(registry["jobs"][0]["state"], json!({"Failed": "Network"}));
        println!("Network: peer sent bad pieces");
    } else {
        assert_eq!(aceitos, [1, 2, 1], "hostil, falha comum, honesto");
        assert_eq!(pedidos.len(), 3);
        assert!(pedidos[2].contains("event=completed"));
        assert_complete(&dir.0, &output, &data);
        assert_eq!(
            Sha256::digest(fs::read(dir.0.join("output/payload.bin")).unwrap()),
            Sha256::digest(&data)
        );
    }
}

#[test]
fn f13b_banido_nao_volta_e_falha_comum_volta() {
    borda_provar_banidos(false);
}

#[test]
fn f13b_tres_portas_banem_o_ip() {
    borda_provar_banidos(true);
}

fn borda_pico_conexoes(total: usize, esperado: usize, responder_handshake: bool) {
    let dir = TestDir::new();
    let tracker = TcpListener::bind("127.0.0.1:0").unwrap();
    let hash = make_tracked_torrent(&dir.0, &[0; PIECE], tracker.local_addr().unwrap());
    let mut listeners: Vec<_> = (0..total)
        .map(|_| {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            listener
        })
        .collect();
    let peers: Vec<_> = listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap())
        .collect();
    let reply = borda_resposta_tracker(&peers);
    thread::scope(|scope| {
        scope.spawn(|| serve_tracker(&tracker, &[&reply], true));
        let mut child = download_command(&dir.0, &[]).spawn().unwrap();
        let deadline = Instant::now() + DEADLINE;
        let mut ativos = Vec::new();
        let mut pico = 0;
        let mut encerrados = Vec::new();
        while !listeners.is_empty() && Instant::now() < deadline {
            let mut index = 0;
            while index < listeners.len() {
                match listeners[index].accept() {
                    Ok((mut stream, _)) => {
                        listeners.swap_remove(index);
                        stream.set_nonblocking(false).unwrap();
                        if responder_handshake {
                            stream = payload_handshake(stream, hash);
                            send_message(&mut stream, 5, &[0]);
                        } else {
                            stream.set_read_timeout(Some(DEADLINE)).unwrap();
                            let mut handshake = [0; 68];
                            stream.read_exact(&mut handshake).unwrap();
                            assert_eq!(&handshake[28..48], &hash);
                        }
                        stream.set_nonblocking(true).unwrap();
                        ativos.push((stream, Instant::now()));
                    }
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => index += 1,
                    Err(err) => panic!("accept failed: {err}"),
                }
            }
            ativos.retain(|(stream, inicio)| {
                if responder_handshake && inicio.elapsed() >= Duration::from_secs(5) {
                    encerrados.push((*inicio, Instant::now()));
                    return false;
                }
                match stream.peek(&mut [0; 1]) {
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => return true,
                    Err(err) => assert!(borda_fim_normal(&err), "{err}"),
                    Ok(bytes) => assert_eq!(bytes, 0, "mensagem inesperada"),
                }
                encerrados.push((*inicio, Instant::now()));
                false
            });
            pico = pico.max(ativos.len());
            thread::sleep(Duration::from_millis(2));
        }
        child.kill().unwrap();
        child.wait().unwrap();
        encerrados.extend(ativos.iter().map(|(_, inicio)| (*inicio, Instant::now())));
        assert!(encerrados.iter().all(|(inicio, fim)| inicio < fim));
        println!(
            "Handshake respondido={responder_handshake}, aceitos={}, pico={pico}",
            encerrados.len()
        );
        assert_eq!(encerrados.len(), total);
        assert_eq!(pico, esperado, "teto de conexões observado no fio");
    });
}

#[test]
fn f13b_teto_de_dez_tentativas() {
    borda_pico_conexoes(15, 10, false);
}

#[test]
fn f13b_teto_de_cinquenta_conectados() {
    borda_pico_conexoes(60, 50, true);
}

fn borda_enviar_peca(stream: &mut TcpStream, index: usize, data: &[u8], piece_length: usize) {
    for begin in (0..piece_length).step_by(PIECE) {
        let message = read_message(stream).unwrap();
        assert_eq!(message[0], 6);
        let pedido = BordaPedido::from_wire(&message);
        assert_eq!(
            pedido,
            BordaPedido {
                index,
                begin,
                length: PIECE
            }
        );
        pedido.responder(stream, data, piece_length, false).unwrap();
    }
}

#[test]
fn f13b_roubo_cancela_so_pedido_sem_resposta() {
    use std::sync::mpsc::sync_channel;

    let dir = TestDir::new();
    let piece_length = 4 * PIECE;
    let data: Vec<_> = (0..3 * piece_length).map(|i| (i % 251) as u8).collect();
    let (torrent, hash) = make_torrent_with_piece_length(&data, &[], false, piece_length);
    fs::write(dir.0.join("input.torrent"), torrent).unwrap();
    let lento = TcpListener::bind("127.0.0.1:0").unwrap();
    let rapido = TcpListener::bind("127.0.0.1:0").unwrap();
    let peers = [lento.local_addr().unwrap(), rapido.local_addr().unwrap()];
    let (abriu, aberto) = sync_channel::<Instant>(1);
    let (tomou, tomado) = sync_channel::<()>(1);
    let (inspecionou, inspecionado) = sync_channel::<()>(1);

    let inicio = Instant::now();
    let (output, pedidos, cancels) = thread::scope(|scope| {
        let bytes = &data;
        let lento = scope.spawn(move || {
            let mut stream = accept_payload_peer(&lento, hash);
            send_message(&mut stream, 5, &[0x80]);
            send_message(&mut stream, 1, &[]);
            let pedidos: Vec<_> = (0..4)
                .map(|n| {
                    let message = read_message(&mut stream).unwrap();
                    assert_eq!(message[0], 6);
                    let pedido = BordaPedido::from_wire(&message);
                    assert_eq!(
                        pedido,
                        BordaPedido {
                            index: 0,
                            begin: n * PIECE,
                            length: PIECE
                        }
                    );
                    pedido
                })
                .collect();
            let recebido = Instant::now();
            abriu.send(recebido).unwrap();
            tomado
                .recv_timeout(Duration::from_secs(2))
                .expect("o seeder rápido não tomou a peça");
            thread::sleep(
                (recebido + Duration::from_secs(1)).saturating_duration_since(Instant::now()),
            );
            pedidos[0]
                .responder(&mut stream, bytes, piece_length, false)
                .unwrap();
            stream
                .set_read_timeout(Some(Duration::from_millis(500)))
                .unwrap();
            let mut cancels = Vec::new();
            loop {
                match read_message(&mut stream) {
                    Ok(message) => {
                        assert_eq!(message[0], 8);
                        cancels.push(BordaPedido::from_wire(&message));
                    }
                    Err(err)
                        if borda_fim_normal(&err)
                            || matches!(
                                err.kind(),
                                io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                            ) =>
                    {
                        break;
                    }
                    Err(err) => panic!("Cancel read failed: {err}"),
                }
            }
            inspecionou.send(()).unwrap();
            (pedidos, cancels)
        });
        let rapido = scope.spawn(move || {
            let recebido = aberto.recv_timeout(DEADLINE).unwrap();
            thread::sleep(Duration::from_millis(300));
            let mut stream = accept_payload_peer(&rapido, hash);
            send_message(&mut stream, 5, &[0x40]);
            send_message(&mut stream, 1, &[]);
            borda_enviar_peca(&mut stream, 1, bytes, piece_length);
            thread::sleep(
                (recebido + Duration::from_millis(450)).saturating_duration_since(Instant::now()),
            );
            // A peça 2 continua na fila até o lento observar o fio. Portanto, (0) foi roubada.
            send_message(&mut stream, 4, &0_u32.to_be_bytes());
            borda_enviar_peca(&mut stream, 0, bytes, piece_length);
            tomou.send(()).unwrap();
            inspecionado.recv_timeout(DEADLINE).unwrap();
            send_message(&mut stream, 4, &2_u32.to_be_bytes());
            borda_enviar_peca(&mut stream, 2, bytes, piece_length);
        });
        let output = wait_for_download(download_command(&dir.0, &peers).spawn().unwrap());
        let (pedidos, cancels) = lento.join().unwrap();
        rapido.join().unwrap();
        (output, pedidos, cancels)
    });
    assert_complete(&dir.0, &output, &data);
    assert_eq!(
        Sha256::digest(fs::read(dir.0.join("output/payload.bin")).unwrap()),
        Sha256::digest(&data)
    );
    let elapsed = inicio.elapsed();
    println!(
        "Roubo, SHA-256 ok, {} ms (lento a 1 bloco/s: 12 s), entregue={:?}, Cancel={cancels:?}",
        elapsed.as_millis(),
        pedidos[0]
    );
    assert!(elapsed < Duration::from_secs(6), "{elapsed:?}");
    assert!(!cancels.is_empty(), "nenhum Cancel recebido");
    let mut sem_resposta: std::collections::HashSet<_> = pedidos[1..].iter().copied().collect();
    for cancel in cancels {
        // F1.4b.2: the 1 s rate tick notices the steal before the slow block arrives,
        // so the Cancel for pedidos[0] crosses its answer on the wire (BEP 3 allows it).
        if cancel == pedidos[0] {
            continue;
        }
        assert!(
            sem_resposta.remove(&cancel),
            "Cancel sem pedido pendente: {cancel:?}"
        );
    }
    assert!(
        sem_resposta.is_empty(),
        "pedidos sem resposta sem Cancel: {sem_resposta:?}"
    );
}

#[test]
fn f14a_atraso_pede_outra_peca_na_mesma_conexao_tcp() {
    let dir = TestDir::new();
    let (data, hash) = make_single_file(&dir.0, 2 * PIECE);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let peer = listener.local_addr().unwrap();
    let child = download_command(&dir.0, &[peer]).spawn().unwrap();
    let mut stream = accept_payload_peer(&listener, hash);
    stream
        .set_read_timeout(Some(Duration::from_secs(25)))
        .unwrap();
    send_message(&mut stream, 5, &[0x80]);
    send_message(&mut stream, 1, &[]);
    let request = read_message(&mut stream).unwrap();
    assert_eq!(request_index(&request), 0);
    let inicio = Instant::now();
    send_message(&mut stream, 4, &1_u32.to_be_bytes());
    let outra = read_message(&mut stream).unwrap();
    assert_eq!(request_index(&outra), 1);
    BordaPedido::from_wire(&outra)
        .responder(&mut stream, &data, PIECE, false)
        .unwrap();
    let cancel = read_message(&mut stream).unwrap();
    let elapsed = inicio.elapsed();
    assert_eq!(cancel[0], 8);
    assert_eq!(cancel[1..], request[1..]);
    assert!(elapsed >= Duration::from_secs(19), "Cancel em {elapsed:?}");
    let retry = read_message(&mut stream).unwrap();
    assert_eq!(retry, request, "Request(1)={outra:?}, Cancel(0)={cancel:?}");
    drop(stream);
    let output = wait_for_download(child);
    assert!(!output.status.success(), "a peça 0 ficou sem fonte");
    let registry = read_json(dir.0.join("jobs/registry.chorus.json"));
    let id = registry["jobs"][0]["id"].as_str().unwrap();
    assert_eq!(
        checkpoint_pieces(&dir.0.join(format!("jobs/{id}.chorus.piecemap.json"))),
        [1]
    );
    println!(
        "TCP: Request(1), Cancel(0) após {} ms, novo Request(0) na mesma conexão",
        elapsed.as_millis()
    );
}

fn f14a_provar_chokes(chokes: usize, espera: Duration) {
    let dir = TestDir::new();
    let piece_length = 2 * PIECE;
    let data: Vec<_> = (0..piece_length).map(|i| (i % 251) as u8).collect();
    let (torrent, hash) = make_torrent_with_piece_length(&data, &[], false, piece_length);
    fs::write(dir.0.join("input.torrent"), torrent).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let peer = listener.local_addr().unwrap();
    thread::scope(|scope| {
        let seeder = scope.spawn(|| {
            let mut stream = accept_payload_peer(&listener, hash);
            send_message(&mut stream, 5, &[0x80]);
            send_message(&mut stream, 1, &[]);
            for rodada in 0..=chokes {
                for begin in [0, PIECE] {
                    let request = read_message(&mut stream).unwrap();
                    assert_eq!(request_index(&request), 0);
                    let pedido = BordaPedido::from_wire(&request);
                    assert_eq!(
                        pedido,
                        BordaPedido {
                            index: 0,
                            begin,
                            length: PIECE
                        }
                    );
                    if begin == 0 || rodada == chokes {
                        pedido
                            .responder(&mut stream, &data, piece_length, false)
                            .unwrap();
                    }
                }
                if rodada < chokes {
                    send_message(&mut stream, 0, &[]);
                    thread::sleep(espera);
                    send_message(&mut stream, 1, &[]);
                }
            }
        });
        let output = wait_for_download(download_command(&dir.0, &[peer]).spawn().unwrap());
        seeder.join().unwrap();
        assert_complete(&dir.0, &output, &data);
    });
    assert_eq!(
        Sha256::digest(fs::read(dir.0.join("output/payload.bin")).unwrap()),
        Sha256::digest(&data)
    );
    listener.set_nonblocking(true).unwrap();
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    println!(
        "TCP: {chokes} Choke, espera de {} ms, aceitos=1, Complete, SHA-256 ok",
        espera.as_millis()
    );
}

#[test]
fn f14a_choke_e_unchoke_apos_sete_segundos() {
    f14a_provar_chokes(1, Duration::from_secs(7));
}

#[test]
fn f14a_quatro_chokes_na_mesma_conexao() {
    f14a_provar_chokes(4, Duration::from_millis(10));
}

#[test]
fn f17_baixa_de_aparelho_ipv6() {
    // BEP 7: a peer reached over IPv6 (manual --peer [::1]:port) serves the whole torrent.
    let Ok(listener) = TcpListener::bind("[::1]:0") else {
        println!("sem IPv6 de loopback nesta máquina: teste pulado");
        return;
    };
    let dir = TestDir::new();
    let total = 3 * PIECE + 19;
    let data: Vec<_> = (0..total).map(|i| (i % 251) as u8).collect();
    let (torrent, hash) = make_torrent(&data, &[], false);
    fs::write(dir.0.join("input.torrent"), &torrent).unwrap();
    let peer = listener.local_addr().unwrap();
    assert!(peer.is_ipv6());
    let seeder_data = data.clone();
    let seeder = thread::spawn(move || seed(&listener, &seeder_data, hash, Seeder::Clean));
    let output = wait_for_download(download_command(&dir.0, &[peer]).spawn().unwrap());
    assert_eq!(seeder.join().unwrap().len(), total.div_ceil(PIECE));
    assert_complete(&dir.0, &output, &data);
    println!("IPv6: {} peças baixadas de {peer}", total.div_ceil(PIECE));
}

/// Minimal HTTP/1.1 server for BEP 19 tests: serves `files` by path with byte ranges.
/// `corromper` flips one byte of every answer. Returns the number of range requests served.
fn servidor_web(
    listener: TcpListener,
    files: std::collections::HashMap<String, Vec<u8>>,
    corromper: bool,
    fim: std::sync::mpsc::Receiver<()>,
) -> usize {
    listener.set_nonblocking(true).unwrap();
    let mut servidos = 0;
    loop {
        if fim.try_recv().is_ok() {
            return servidos;
        }
        let mut stream = match listener.accept() {
            Ok((stream, _)) => stream,
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5));
                continue;
            }
            Err(err) => panic!("accept: {err}"),
        };
        stream.set_nonblocking(false).unwrap();
        stream.set_read_timeout(Some(DEADLINE)).unwrap();
        let mut pedido = Vec::new();
        let mut byte = [0];
        while !pedido.ends_with(b"\r\n\r\n") {
            if stream.read(&mut byte).unwrap_or(0) == 0 {
                break;
            }
            pedido.push(byte[0]);
        }
        let texto = String::from_utf8_lossy(&pedido);
        let caminho = texto.split_whitespace().nth(1).unwrap_or("/").to_string();
        let faixa = texto.lines().find_map(|l| {
            l.to_ascii_lowercase()
                .strip_prefix("range: bytes=")
                .map(str::to_string)
        });
        let Some(dados) = files.get(&caminho) else {
            let _ = stream.write_all(
                b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
            );
            continue;
        };
        let (a, b) = faixa
            .and_then(|f| {
                let (a, b) = f.trim().split_once('-')?;
                Some((a.parse::<usize>().ok()?, b.parse::<usize>().ok()?))
            })
            .unwrap_or((0, dados.len() - 1));
        let mut corpo = dados[a..=b].to_vec();
        if corromper {
            corpo[0] ^= 0xff;
        }
        let cabecalho = format!(
            "HTTP/1.1 206 Partial Content\r\ncontent-range: bytes {a}-{b}/{}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            dados.len(),
            corpo.len()
        );
        let _ = stream.write_all(cabecalho.as_bytes());
        let _ = stream.write_all(&corpo);
        servidos += 1;
    }
}

fn torrent_com_url_list(torrent: &[u8], url: &str) -> Vec<u8> {
    // Keys in order: "info" < "url-list".
    let mut com = torrent[..torrent.len() - 1].to_vec();
    com.extend_from_slice(format!("8:url-list{}:{url}e", url.len()).as_bytes());
    com
}

fn porta_fechada() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

#[test]
fn f18_web_seed_baixa_multiarquivo_por_faixas() {
    let dir = TestDir::new();
    let files: &[(&[&str], usize)] = &[
        (&["a.bin"], PIECE + 100),
        (&["sub", "b c.bin"], 2 * PIECE + 7),
    ];
    let total: usize = files.iter().map(|(_, n)| n).sum();
    let data: Vec<_> = (0..total).map(|i| (i % 251) as u8).collect();
    let (torrent, _) = make_torrent(&data, files, false);
    let http = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/pub/", http.local_addr().unwrap());
    fs::write(
        dir.0.join("input.torrent"),
        torrent_com_url_list(&torrent, &url),
    )
    .unwrap();
    let servidos = std::collections::HashMap::from([
        (
            "/pub/payload/a.bin".to_string(),
            data[..PIECE + 100].to_vec(),
        ),
        (
            "/pub/payload/sub/b%20c.bin".to_string(),
            data[PIECE + 100..].to_vec(),
        ),
    ]);
    let (parar, fim) = std::sync::mpsc::channel();
    let servidor = thread::spawn(move || servidor_web(http, servidos, false, fim));
    let output = wait_for_download(
        download_command(&dir.0, &[porta_fechada()])
            .spawn()
            .unwrap(),
    );
    parar.send(()).unwrap();
    let pedidos = servidor.join().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut baixado = fs::read(dir.0.join("output/payload/a.bin")).unwrap();
    baixado.extend(fs::read(dir.0.join("output/payload/sub/b c.bin")).unwrap());
    assert_eq!(Sha256::digest(&baixado), Sha256::digest(&data));
    // 4 pieces; the piece crossing the two files needs 2 ranges.
    assert_eq!(pedidos, total.div_ceil(PIECE) + 1);
    println!("web seed: {pedidos} faixas HTTP, 2 arquivos, SHA-256 ok, aparelho morto ignorado");
}

#[test]
fn f18_web_seed_com_hash_errado_e_descartado_sem_gravar() {
    let dir = TestDir::new();
    let total = 2 * PIECE;
    let data: Vec<_> = (0..total).map(|i| (i % 251) as u8).collect();
    let (torrent, _) = make_torrent(&data, &[], false);
    let http = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/f.bin", http.local_addr().unwrap());
    fs::write(
        dir.0.join("input.torrent"),
        torrent_com_url_list(&torrent, &url),
    )
    .unwrap();
    let servidos = std::collections::HashMap::from([("/f.bin".to_string(), data.clone())]);
    let (parar, fim) = std::sync::mpsc::channel();
    let servidor = thread::spawn(move || servidor_web(http, servidos, true, fim));
    let output = wait_for_download(
        download_command(&dir.0, &[porta_fechada()])
            .spawn()
            .unwrap(),
    );
    parar.send(()).unwrap();
    let pedidos = servidor.join().unwrap();
    assert!(
        !output.status.success(),
        "dado corrompido não pode completar"
    );
    assert_eq!(
        pedidos, 1,
        "BEP 19: depois do hash errado, a URL é descartada"
    );
    let registry = read_json(dir.0.join("jobs/registry.chorus.json"));
    let id = registry["jobs"][0]["id"].as_str().unwrap();
    let mapa = dir.0.join(format!("jobs/{id}.chorus.piecemap.json"));
    if mapa.exists() {
        assert!(
            checkpoint_pieces(&mapa).is_empty(),
            "peça corrompida no mapa"
        );
    }
    println!("web seed corrompido: 1 pedido, URL descartada, nada gravado como conferido");
}
