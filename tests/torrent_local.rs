use serde_json::{Value, json};
use sha1::{Digest, Sha1};
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
    assert_eq!(handshake[25] & 0x10, 0, "unexpected metadata handshake");
    assert_eq!(&handshake[28..48], &hash);
    assert_eq!(&handshake[48..56], b"-CH0300-");
    if mode == Seeder::WrongHash {
        handshake[28] ^= 0xff;
    }
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
        assert_eq!(message[0], 6, "expected Request, got {message:?}");
        let index = u32::from_be_bytes(message[1..5].try_into().unwrap()) as usize;
        if mode == Seeder::HaveOnly && index == 0 && requests.len() < pieces - 1 {
            // Close on a request for a piece this peer has not announced yet.
            return requests;
        }
        if mode == Seeder::StallAfterTwo && requests.len() == 2 {
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
        .chunks(PIECE)
        .flat_map(|piece| Sha1::digest(piece).to_vec())
        .collect();
    if bad_count {
        hashes.truncate(hashes.len() - 20);
    }
    write!(info, "12:piece lengthi{PIECE}e6:pieces{}:", hashes.len()).unwrap();
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
    handshake[48..].fill(b'S');
    stream.write_all(&handshake).unwrap();
    send_message(&mut stream, 5, &[0xc0]);
    assert_eq!(read_message(&mut stream).unwrap(), [2]);
    send_message(&mut stream, 1, &[]);

    let request = read_message(&mut stream).unwrap();
    assert_eq!(&request[..9], &[6, 0, 0, 0, 0, 0, 0, 0, 0]);
    let mut first_piece = request[1..9].to_vec();
    first_piece.extend_from_slice(&data[..PIECE]);
    send_message(&mut stream, 7, &first_piece);
    let request = read_message(&mut stream).unwrap();
    assert_eq!(&request[..9], &[6, 0, 0, 0, 1, 0, 0, 0, 0]);

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
    last_piece.push(data[PIECE]);
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
            (map["bitfield"] == json!([0xc0])
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

fn resume_case(
    no_overwrite: bool,
    modify_partial: impl FnOnce(&Path, &Path),
    expected_requests: &[usize],
    expected_bytes: usize,
) {
    let dir = TestDir::new();
    let (data, hash) = make_single_file(&dir.0, 3 * PIECE + 19);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let peer = listener.local_addr().unwrap();
    thread::scope(|scope| {
        let seeder = scope.spawn(|| {
            assert_eq!(seed(&listener, &data, hash, Seeder::StallAfterTwo), [0, 1]);
            seed(&listener, &data, hash, Seeder::Clean)
        });
        let (id, map) = interrupt_download(&dir.0, peer, no_overwrite);
        modify_partial(&dir.0.join("output/payload.bin"), &map);

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
        assert_eq!(seeder.join().unwrap(), expected_requests);
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
    resume_case(false, |_, _| {}, &[2, 3], 2 * PIECE);
}

#[test]
fn requests_corrupt_piece_again_after_kill() {
    resume_case(
        false,
        |file, _| {
            let mut partial = fs::OpenOptions::new().write(true).open(file).unwrap();
            partial.seek(SeekFrom::Start(PIECE as u64)).unwrap();
            partial.write_all(b"corrupt piece").unwrap();
        },
        &[1, 2, 3],
        PIECE,
    );
}

#[test]
fn restarts_from_zero_with_an_invalid_piece_map() {
    for map in [
        json!({"num_pieces": 5, "bitfield": [0xc0]}).to_string(),
        json!({"num_pieces": 4, "bitfield": []}).to_string(),
        json!({"num_pieces": 4, "bitfield": [0xc1]}).to_string(),
        "invalid JSON".to_owned(),
    ] {
        resume_case(
            false,
            |_, path| fs::write(path, map).unwrap(),
            &[0, 1, 2, 3],
            0,
        );
    }
}

#[test]
fn no_overwrite_allows_resume_and_protects_an_unrelated_file() {
    resume_case(true, |_, _| {}, &[2, 3], 2 * PIECE);

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
        assert_eq!(seeder.join().unwrap(), [0, 1]);
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
        assert_eq!(seeder.join().unwrap(), [1, 2, 0]);
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
        assert_eq!(peer.join().unwrap(), [0, 1]);
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
        assert_eq!(peer.join().unwrap(), [0, 1]);
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
        assert_eq!(seeder.join().unwrap(), [1, 2, 0]);
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
        assert_eq!(peer.join().unwrap(), [0, 1]);
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
                    assert_eq!(seed(&honest, &data, hash, Seeder::Clean), [0, 1]);
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
            assert_eq!(seed(&listener, &data, hash, Seeder::StallAfterTwo), [0, 1]);
            seed(&listener, &data, hash, Seeder::Clean)
        });
        let child = magnet_command(&dir.0, &magnet_link(hash), &peers)
            .spawn()
            .unwrap();
        let (id, _) = interrupt_at_checkpoint(&dir.0, child, true);
        let output = wait_for_download(
            job_command(&dir.0)
                .args(["--resume-job", &id])
                .spawn()
                .unwrap(),
        );
        assert_complete(&dir.0, &output, &data);
        assert_eq!(peer.join().unwrap(), [2, 3]);
    });
}
