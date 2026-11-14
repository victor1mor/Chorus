use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[test]
fn downloads_ranges_with_probe_and_retry() {
    let data: Vec<u8> = (0..512 * 1024).map(|i| (i % 251) as u8).collect();
    let sha256 = sha256_hex(&data);
    let server = RangeServer::spawn(data.clone());
    let temp = temp_dir("range-smoke");
    let output = temp.join("download.bin");

    let status = Command::new(chorus_bin())
        .args([
            "-x",
            "4",
            "--retries",
            "2",
            "--piece-size",
            "65536",
            "--sha256",
            &sha256,
            "-o",
            output.to_str().unwrap(),
            &format!("http://{}/fixture.bin", server.addr),
        ])
        .status()
        .unwrap();

    assert!(status.success());
    assert_eq!(fs::read(&output).unwrap(), data);
    assert!(!with_suffix(&output, ".part").exists());
    assert!(!with_suffix(&output, ".part.meta").exists());
    assert!(!with_suffix(&output, ".part.ranges").exists());

    let _ = fs::remove_dir_all(temp);
}

#[test]
fn no_overwrite_keeps_existing_output() {
    let data: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
    let server = RangeServer::spawn(data);
    let temp = temp_dir("no-overwrite");
    let output = temp.join("download.bin");
    fs::write(&output, b"old").unwrap();

    let status = Command::new(chorus_bin())
        .args([
            "--no-overwrite",
            "-o",
            output.to_str().unwrap(),
            &format!("http://{}/fixture.bin", server.addr),
        ])
        .status()
        .unwrap();

    assert!(!status.success());
    assert_eq!(fs::read(&output).unwrap(), b"old");
    assert!(!with_suffix(&output, ".part").exists());
    assert!(!with_suffix(&output, ".part.meta").exists());
    assert!(!with_suffix(&output, ".part.ranges").exists());

    let _ = fs::remove_dir_all(temp);
}

#[test]
fn checksum_mismatch_does_not_preserve_complete_resume_map() {
    let data: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
    let sha256 = sha256_hex(&data);
    let wrong_sha256 = "0".repeat(64);
    let server = RangeServer::spawn(data.clone());
    let temp = temp_dir("checksum-resume");
    let output = temp.join("download.bin");
    let url = format!("http://{}/fixture.bin", server.addr);

    let status = Command::new(chorus_bin())
        .args([
            "-x",
            "4",
            "--retries",
            "2",
            "--sha256",
            &wrong_sha256,
            "-o",
            output.to_str().unwrap(),
            &url,
        ])
        .status()
        .unwrap();

    assert!(!status.success());
    assert!(!output.exists());
    assert!(with_suffix(&output, ".part").exists());
    assert!(!with_suffix(&output, ".part.ranges").exists());

    let status = Command::new(chorus_bin())
        .args([
            "-x",
            "4",
            "--retries",
            "2",
            "--sha256",
            &sha256,
            "-o",
            output.to_str().unwrap(),
            &url,
        ])
        .status()
        .unwrap();

    assert!(status.success());
    assert_eq!(fs::read(&output).unwrap(), data);
    assert!(!with_suffix(&output, ".part").exists());
    assert!(!with_suffix(&output, ".part.meta").exists());
    assert!(!with_suffix(&output, ".part.ranges").exists());

    let _ = fs::remove_dir_all(temp);
}

#[test]
fn resumes_after_process_is_killed_mid_download() {
    let data: Vec<u8> = (0..4 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    let server = RangeServer::spawn_slow(data.clone(), 8 * 1024, Duration::from_millis(2));
    let temp = temp_dir("hard-kill-resume");
    let output = temp.join("download.bin");
    let ranges = with_suffix(&output, ".part.ranges");
    let url = format!("http://{}/fixture.bin", server.addr);

    let mut child = Command::new(chorus_bin())
        .args([
            "-x",
            "2",
            "--retries",
            "0",
            "--piece-size",
            "65536",
            "-o",
            output.to_str().unwrap(),
            &url,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    wait_for_non_empty(&ranges, Duration::from_secs(5));
    assert!(
        child.try_wait().unwrap().is_none(),
        "download finished before kill"
    );
    child.kill().unwrap();
    assert!(!child.wait().unwrap().success());
    assert!(with_suffix(&output, ".part").exists());
    assert!(ranges.exists());

    let output_run = Command::new(chorus_bin())
        .args([
            "-x",
            "2",
            "--retries",
            "0",
            "--piece-size",
            "65536",
            "-o",
            output.to_str().unwrap(),
            &url,
        ])
        .output()
        .unwrap();

    assert!(output_run.status.success());
    assert!(
        String::from_utf8_lossy(&output_run.stdout).contains("Resuming"),
        "second run did not report resume"
    );
    assert_eq!(fs::read(&output).unwrap(), data);
    assert!(!with_suffix(&output, ".part").exists());
    assert!(!with_suffix(&output, ".part.meta").exists());
    assert!(!ranges.exists());

    let _ = fs::remove_dir_all(temp);
}

#[test]
fn default_output_name_is_sanitized() {
    let data: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
    let server = RangeServer::spawn(data.clone());
    let temp = temp_dir("default-name");

    let status = Command::new(chorus_bin())
        .current_dir(&temp)
        .args([&format!("http://{}/bad:name*.bin", server.addr)])
        .status()
        .unwrap();

    assert!(status.success());
    assert_eq!(fs::read(temp.join("bad_name_.bin")).unwrap(), data);

    let _ = fs::remove_dir_all(temp);
}

#[allow(dead_code)]
struct RangeServer {
    addr: SocketAddr,
    active_connections: Arc<std::sync::atomic::AtomicUsize>,
    peak_connections: Arc<std::sync::atomic::AtomicUsize>,
}

impl RangeServer {
    fn spawn(data: Vec<u8>) -> Self {
        Self::spawn_with(data, true, usize::MAX, Duration::from_millis(0))
    }

    fn spawn_slow(data: Vec<u8>, bytes_per_write: usize, delay: Duration) -> Self {
        Self::spawn_with(data, false, bytes_per_write, delay)
    }

    fn spawn_with(
        data: Vec<u8>,
        fail_once_per_range: bool,
        bytes_per_write: usize,
        delay: Duration,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let data = Arc::new(data);
        let failed_ends = Arc::new(Mutex::new(HashSet::new()));
        let active_connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let peak_connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let active = Arc::clone(&active_connections);
        let peak = Arc::clone(&peak_connections);

        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let data = Arc::clone(&data);
                let failed_ends = Arc::clone(&failed_ends);
                let active_conn = Arc::clone(&active);
                let peak_conn = Arc::clone(&peak);

                thread::spawn(move || {
                    handle_connection(
                        stream,
                        data,
                        failed_ends,
                        fail_once_per_range,
                        bytes_per_write,
                        delay,
                        active_conn,
                        peak_conn,
                    );
                });
            }
        });

        Self {
            addr,
            active_connections,
            peak_connections,
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_connection(
    mut stream: TcpStream,
    data: Arc<Vec<u8>>,
    failed_ends: Arc<Mutex<HashSet<u64>>>,
    fail_once_per_range: bool,
    bytes_per_write: usize,
    delay: Duration,
    active_conn: Arc<std::sync::atomic::AtomicUsize>,
    peak_conn: Arc<std::sync::atomic::AtomicUsize>,
) {
    let request = read_request(&mut stream);
    let Some(first_line) = request.lines().next() else {
        return;
    };

    if first_line.starts_with("HEAD ") {
        write_response(
            &mut stream,
            200,
            &headers(data.len(), None),
            &[],
            bytes_per_write,
            delay,
        );
        return;
    }

    let is_get = first_line.starts_with("GET ");
    let _active_guard = if is_get {
        struct Guard(Arc<std::sync::atomic::AtomicUsize>);
        impl Drop for Guard {
            fn drop(&mut self) {
                self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let current = active_conn.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        let mut p = peak_conn.load(std::sync::atomic::Ordering::SeqCst);
        while current > p {
            match peak_conn.compare_exchange(
                p,
                current,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(actual) => p = actual,
            }
        }
        Some(Guard(active_conn))
    } else {
        None
    };

    if !first_line.starts_with("GET /fixture.bin ")
        && !first_line.starts_with("GET /bad:name*.bin ")
    {
        write_response(
            &mut stream,
            404,
            &[("Content-Length", "0".to_string())],
            &[],
            bytes_per_write,
            delay,
        );
        return;
    }

    let range = request
        .lines()
        .find_map(|line| header_value(line, "range")?.strip_prefix("bytes="))
        .and_then(|value| value.split_once('-'))
        .map(|(start, end)| {
            (
                start.parse::<u64>().unwrap(),
                end.parse::<u64>().unwrap_or(data.len() as u64 - 1),
            )
        });

    let Some((start, end)) = range else {
        write_response(
            &mut stream,
            200,
            &headers(data.len(), None),
            &data,
            bytes_per_write,
            delay,
        );
        return;
    };

    let if_range = request
        .lines()
        .find_map(|line| header_value(line, "if-range"));
    if (start, end) != (0, 0) && if_range != Some("\"range-smoke\"") {
        write_response(
            &mut stream,
            412,
            &[("Content-Length", "0".to_string())],
            &[],
            bytes_per_write,
            delay,
        );
        return;
    }

    let body = &data[start as usize..=end as usize];
    let mut headers = headers(
        body.len(),
        Some(format!("bytes {}-{}/{}", start, end, data.len())),
    );
    headers.push(("Accept-Ranges", "bytes".to_string()));

    let mut failed = failed_ends.lock().unwrap();
    let fail_once = fail_once_per_range && body.len() > 1 && failed.insert(end);
    drop(failed);

    if fail_once {
        write_response(
            &mut stream,
            206,
            &headers,
            &body[..body.len() / 2],
            bytes_per_write,
            delay,
        );
    } else {
        write_response(&mut stream, 206, &headers, body, bytes_per_write, delay);
    }
}

fn read_request(stream: &mut TcpStream) -> String {
    let mut buf = [0; 4096];
    let mut request = Vec::new();
    loop {
        let n = stream.read(&mut buf).unwrap_or(0);
        if n == 0 {
            break;
        }
        request.extend_from_slice(&buf[..n]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8_lossy(&request).into_owned()
}

fn header_value<'a>(line: &'a str, name: &str) -> Option<&'a str> {
    let (got_name, value) = line.split_once(':')?;
    got_name.eq_ignore_ascii_case(name).then_some(value.trim())
}

fn headers(content_length: usize, content_range: Option<String>) -> Vec<(&'static str, String)> {
    let mut headers = vec![
        ("Content-Length", content_length.to_string()),
        ("ETag", "\"range-smoke\"".to_string()),
        ("Connection", "close".to_string()),
    ];
    if let Some(content_range) = content_range {
        headers.push(("Content-Range", content_range));
    }
    headers
}

fn write_response(
    stream: &mut TcpStream,
    status: u16,
    headers: &[(&'static str, String)],
    body: &[u8],
    bytes_per_write: usize,
    delay: Duration,
) {
    let reason = if status == 206 {
        "Partial Content"
    } else if status == 200 {
        "OK"
    } else {
        "Not Found"
    };
    if write!(stream, "HTTP/1.1 {} {}\r\n", status, reason).is_err() {
        return;
    }
    for (name, value) in headers {
        if write!(stream, "{}: {}\r\n", name, value).is_err() {
            return;
        }
    }
    if write!(stream, "\r\n").is_err() {
        return;
    }

    if delay == Duration::from_millis(0) || body.len() <= 1 {
        let _ = stream.write_all(body);
        return;
    }

    for chunk in body.chunks(bytes_per_write) {
        thread::sleep(delay);
        if stream.write_all(chunk).is_err() {
            return;
        }
    }
}

fn chorus_bin() -> PathBuf {
    if let Some(path) = option_env!("CARGO_BIN_EXE_Chorus") {
        return PathBuf::from(path);
    }

    let mut path = std::env::current_exe().unwrap();
    path.pop();
    path.pop();
    path.push(format!("Chorus{}", std::env::consts::EXE_SUFFIX));
    path
}

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path =
        std::env::temp_dir().join(format!("chorus-{}-{}-{}", name, std::process::id(), nanos));
    fs::create_dir_all(&path).unwrap();
    path
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn wait_for_non_empty(path: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if fs::read_to_string(path)
            .map(|text| !text.trim().is_empty())
            .unwrap_or(false)
        {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("timed out waiting for {}", path.display());
}

fn sha256_hex(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        out.push_str(&format!("{:02x}", byte));
    }
    out
}

#[test]
fn job_manifest_success_redacts_secrets_and_completes() {
    let data: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
    let server = RangeServer::spawn(data.clone());
    let temp = temp_dir("manifest-success");
    let output = temp.join("download.bin");
    let jobs = temp.join("jobs");

    let url = format!("http://user:secret@{}/fixture.bin", server.addr);

    let status = Command::new(chorus_bin())
        .args([
            "--job-dir",
            jobs.to_str().unwrap(),
            "-o",
            output.to_str().unwrap(),
            &url,
        ])
        .status()
        .unwrap();

    assert!(status.success());
    let manifest_path = fs::read_dir(&jobs)
        .unwrap()
        .find_map(|entry| {
            let path = entry.unwrap().path();
            if path.to_string_lossy().ends_with(".chorus.json")
                && !path.to_string_lossy().ends_with("registry.chorus.json")
            {
                Some(path)
            } else {
                None
            }
        })
        .unwrap();
    let manifest = fs::read_to_string(&manifest_path).unwrap();

    assert!(manifest.contains("\"state\": \"Complete\""));
    assert!(!manifest.contains("user"));
    assert!(!manifest.contains("secret"));
    assert!(!manifest.contains("token"));

    let _ = fs::remove_dir_all(temp);
}

#[test]
fn job_manifest_checksum_failure_is_persisted() {
    let data: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
    let server = RangeServer::spawn(data.clone());
    let temp = temp_dir("manifest-checksum");
    let output = temp.join("download.bin");
    let jobs = temp.join("jobs");

    let wrong_sha256 = "0".repeat(64);
    let url = format!("http://user:secret@{}/fixture.bin", server.addr);

    let status = Command::new(chorus_bin())
        .args([
            "--job-dir",
            jobs.to_str().unwrap(),
            "--sha256",
            &wrong_sha256,
            "-o",
            output.to_str().unwrap(),
            &url,
        ])
        .status()
        .unwrap();

    assert!(!status.success());
    let manifest_path = fs::read_dir(&jobs)
        .unwrap()
        .find_map(|entry| {
            let path = entry.unwrap().path();
            if path.to_string_lossy().ends_with(".chorus.json")
                && !path.to_string_lossy().ends_with("registry.chorus.json")
            {
                Some(path)
            } else {
                None
            }
        })
        .unwrap();
    let manifest = fs::read_to_string(&manifest_path).unwrap();

    assert!(manifest.contains("\"Failed\""));
    assert!(manifest.contains("\"Integrity\""));
    assert!(!manifest.contains("user"));
    assert!(!manifest.contains("secret"));
    assert!(!manifest.contains("token"));

    let _ = fs::remove_dir_all(temp);
}

#[test]
fn job_manifest_http_failure_is_persisted() {
    let data: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
    let server = RangeServer::spawn(data.clone());
    let temp = temp_dir("manifest-http");
    let output = temp.join("download.bin");
    let jobs = temp.join("jobs");

    let url = format!("http://user:secret@{}/not-found.bin", server.addr);

    let status = Command::new(chorus_bin())
        .args([
            "--job-dir",
            jobs.to_str().unwrap(),
            "-o",
            output.to_str().unwrap(),
            &url,
        ])
        .status()
        .unwrap();

    assert!(!status.success());
    let manifest_path = fs::read_dir(&jobs)
        .unwrap()
        .find_map(|entry| {
            let path = entry.unwrap().path();
            if path.to_string_lossy().ends_with(".chorus.json")
                && !path.to_string_lossy().ends_with("registry.chorus.json")
            {
                Some(path)
            } else {
                None
            }
        })
        .unwrap();
    let manifest = fs::read_to_string(&manifest_path).unwrap();

    assert!(manifest.contains("\"Failed\""));
    assert!(manifest.contains("\"Network\""));
    assert!(!manifest.contains("user"));
    assert!(!manifest.contains("secret"));
    assert!(!manifest.contains("token"));

    let _ = fs::remove_dir_all(temp);
}

#[test]
fn resumes_job_by_id_after_kill() {
    let data: Vec<u8> = (0..4 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    let server = RangeServer::spawn_slow(data.clone(), 8 * 1024, Duration::from_millis(2));
    let temp = temp_dir("resume-id-kill");
    let output = temp.join("download.bin");
    let ranges = with_suffix(&output, ".part.ranges");
    let jobs = temp.join("jobs");
    let url = format!("http://user:secret@{}/fixture.bin", server.addr);

    let mut child = Command::new(chorus_bin())
        .args([
            "-x",
            "2",
            "--retries",
            "0",
            "--piece-size",
            "65536",
            "--job-dir",
            jobs.to_str().unwrap(),
            "-o",
            output.to_str().unwrap(),
            &url,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    wait_for_non_empty(&ranges, Duration::from_secs(5));
    assert!(
        child.try_wait().unwrap().is_none(),
        "download finished before kill"
    );
    child.kill().unwrap();
    assert!(!child.wait().unwrap().success());
    assert!(with_suffix(&output, ".part").exists());
    assert!(ranges.exists());

    let manifest_path = fs::read_dir(&jobs)
        .unwrap()
        .find_map(|entry| {
            let path = entry.unwrap().path();
            if path.to_string_lossy().ends_with(".chorus.json")
                && !path.to_string_lossy().ends_with("registry.chorus.json")
            {
                Some(path)
            } else {
                None
            }
        })
        .unwrap();

    let manifest_content = fs::read_to_string(&manifest_path).unwrap();
    let id_line = manifest_content
        .lines()
        .find(|l| l.contains("\"id\":"))
        .unwrap();
    let job_id = id_line.split('"').nth(3).unwrap().to_string();

    let output_run = Command::new(chorus_bin())
        .args(["--job-dir", jobs.to_str().unwrap(), "--resume-job", &job_id])
        .output()
        .unwrap();

    assert!(
        output_run.status.success(),
        "Resume run failed: {}",
        String::from_utf8_lossy(&output_run.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output_run.stdout).contains("Resuming"),
        "second run did not report resume"
    );
    assert_eq!(fs::read(&output).unwrap(), data);
    assert!(!with_suffix(&output, ".part").exists());
    assert!(!with_suffix(&output, ".part.meta").exists());
    assert!(!ranges.exists());

    let _ = fs::remove_dir_all(temp);
}

#[test]
fn scheduler_fifo_order_and_recovery() {
    let data1: Vec<u8> = (0..2 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    let data2: Vec<u8> = (0..1024 * 1024).map(|i| (i % 241) as u8).collect();

    let server1 = RangeServer::spawn_slow(data1.clone(), 8 * 1024, Duration::from_millis(2));
    let server2 = RangeServer::spawn(data2.clone());

    let temp = temp_dir("scheduler-fifo");
    let output1 = temp.join("download1.bin");
    let output2 = temp.join("download2.bin");
    let ranges1 = with_suffix(&output1, ".part.ranges");
    let jobs = temp.join("jobs");
    let url1 = format!("http://user:secret@{}/fixture.bin", server1.addr);
    let url2 = format!("http://user:secret@{}/fixture.bin", server2.addr);

    // 1. Start job1 and kill it to simulate interruption
    let mut child = Command::new(chorus_bin())
        .args([
            "-x",
            "2",
            "--retries",
            "0",
            "--piece-size",
            "65536",
            "--job-dir",
            jobs.to_str().unwrap(),
            "-o",
            output1.to_str().unwrap(),
            &url1,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    wait_for_non_empty(&ranges1, Duration::from_secs(5));
    assert!(
        child.try_wait().unwrap().is_none(),
        "job1 finished before kill"
    );
    child.kill().unwrap();
    assert!(!child.wait().unwrap().success());

    // Windows might take a moment to release file handles after kill
    std::thread::sleep(Duration::from_millis(1000));

    assert!(with_suffix(&output1, ".part").exists());

    // 2. Enqueue job2. The scheduler should resume job1 then execute job2.
    let output_run = Command::new(chorus_bin())
        .args([
            "-x",
            "2",
            "--retries",
            "2",
            "--piece-size",
            "65536",
            "--job-dir",
            jobs.to_str().unwrap(),
            "-o",
            output2.to_str().unwrap(),
            &url2,
        ])
        .output()
        .unwrap();

    let stdout = String::from_utf8_lossy(&output_run.stdout);
    assert!(
        output_run.status.success(),
        "Scheduler run failed: {}",
        String::from_utf8_lossy(&output_run.stderr)
    );

    // Verify execution order by checking the order in the output
    let s1 = format!("(http://{}/fixture.bin)", server1.addr);
    let s2 = format!("(http://{}/fixture.bin)", server2.addr);
    let idx1 = stdout.find(&s1).expect("job1 not scheduled");
    let idx2 = stdout.find(&s2).expect("job2 not scheduled");
    assert!(idx1 < idx2, "job1 must execute before job2");
    assert!(stdout.contains("Resuming"), "job1 did not resume");
    assert!(!stdout.contains("secret"), "{stdout}");

    // Verify bytes
    assert_eq!(fs::read(&output1).unwrap(), data1);
    assert_eq!(fs::read(&output2).unwrap(), data2);

    // Verify states
    let registry_path = jobs.join("registry.chorus.json");
    let registry = fs::read_to_string(&registry_path).unwrap();
    assert_eq!(registry.matches("\"Complete\"").count(), 2);

    let _ = fs::remove_dir_all(temp);
}

#[test]
fn scheduler_concurrency_limits() {
    let data_a: Vec<u8> = (0..512 * 1024).map(|i| (i % 251) as u8).collect();
    let data_b: Vec<u8> = (0..512 * 1024).map(|i| (i % 241) as u8).collect();

    // Server A is very slow (host 1 limit)
    let server_a = RangeServer::spawn_slow(data_a.clone(), 8 * 1024, Duration::from_millis(10));
    // Server B is also slow but different host
    let server_b = RangeServer::spawn_slow(data_b.clone(), 8 * 1024, Duration::from_millis(5));

    let temp = temp_dir("scheduler-limits");
    let jobs = temp.join("jobs");
    fs::create_dir_all(&jobs).unwrap();

    let url_a = format!("http://{}/fixture.bin", server_a.addr);
    let url_b = format!("http://localhost:{}/fixture.bin", server_b.addr.port());

    // Create 4 jobs for host A and 2 jobs for host B manually to avoid executing them sequentially
    // wait, chorus currently doesn't allow adding jobs without starting the scheduler,
    // but if we kill it immediately, or if we just write the manifests:

    // Actually, we can run chorus 5 times, killing it very quickly, but it might not be reliable.
    // It's safer to just write the manifest files directly. We can use the JSON structure.
    for i in 0..4 {
        let job_json = format!(
            r#"{{
            "version": 1,
            "id": "job-a-{}",
            "state": "Waiting",
            "source": "{}",
            "output": "{}"
        }}"#,
            i,
            url_a,
            temp.join(format!("out-a-{}.bin", i)).display()
        )
        .replace('\\', "\\\\");
        let priv_json = format!(
            r#"{{
            "version": 1,
            "original_url": "{}"
        }}"#,
            url_a
        );
        fs::write(jobs.join(format!("job-a-{}.chorus.json", i)), job_json).unwrap();
        fs::write(
            jobs.join(format!("job-a-{}.chorus.private.json", i)),
            priv_json,
        )
        .unwrap();
    }

    for i in 0..1 {
        let job_json = format!(
            r#"{{
            "version": 1,
            "id": "job-b-{}",
            "state": "Waiting",
            "source": "{}",
            "output": "{}"
        }}"#,
            i,
            url_b,
            temp.join(format!("out-b-{}.bin", i)).display()
        )
        .replace('\\', "\\\\");
        let priv_json = format!(
            r#"{{
            "version": 1,
            "original_url": "{}"
        }}"#,
            url_b
        );
        fs::write(jobs.join(format!("job-b-{}.chorus.json", i)), job_json).unwrap();
        fs::write(
            jobs.join(format!("job-b-{}.chorus.private.json", i)),
            priv_json,
        )
        .unwrap();
    }

    // Now run chorus submitting the 6th job (for host B), with max-jobs=3, max-jobs-per-host=2
    // It should load the 5 previous jobs, plus the new one.
    // The limits: max 3 jobs total. max 2 per host.
    // We expect: 2 jobs for host A, 1 job for host B to run simultaneously.
    let mut child = Command::new(chorus_bin())
        .args([
            "-j",
            "3",
            "--max-jobs-per-host",
            "2",
            "-x",
            "1", // 1 connection per job for simpler tracking
            "--retries",
            "0",
            "--piece-size",
            "65536",
            "--job-dir",
            jobs.to_str().unwrap(),
            "-o",
            temp.join("out-b-1.bin").to_str().unwrap(),
            &url_b,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    thread::sleep(Duration::from_millis(500));
    assert!(child.try_wait().unwrap().is_none(), "finished too early");
    child.kill().unwrap();
    let _ = child.wait();

    let peak_a_before = server_a
        .peak_connections
        .load(std::sync::atomic::Ordering::SeqCst);
    let peak_b_before = server_b
        .peak_connections
        .load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        peak_a_before <= 2,
        "Host A exceeded limit before restart: {}",
        peak_a_before
    );
    assert!(
        peak_b_before <= 2,
        "Host B exceeded limit before restart: {}",
        peak_b_before
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    while server_a
        .active_connections
        .load(std::sync::atomic::Ordering::SeqCst)
        != 0
        || server_b
            .active_connections
            .load(std::sync::atomic::Ordering::SeqCst)
            != 0
    {
        assert!(
            Instant::now() < deadline,
            "Old scheduler connections did not close within 5 seconds"
        );
        thread::sleep(Duration::from_millis(20));
    }
    server_a
        .peak_connections
        .store(0, std::sync::atomic::Ordering::SeqCst);
    server_b
        .peak_connections
        .store(0, std::sync::atomic::Ordering::SeqCst);

    // Restart the jobs to verify limits are respected after restart (J6.5 requirement)
    let output = Command::new(chorus_bin())
        .args([
            "-j",
            "3",
            "--max-jobs-per-host",
            "2",
            "-x",
            "1",
            "--retries",
            "0",
            "--piece-size",
            "65536",
            "--job-dir",
            jobs.to_str().unwrap(),
            "--resume-job",
            "job-a-0", // Resuming one will trigger the scheduler to pick up all Waiting jobs
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "Scheduler limit run failed after restart: {}\nSTDOUT: {}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );

    // Verify peak connections after restart
    let peak_a = server_a
        .peak_connections
        .load(std::sync::atomic::Ordering::SeqCst);
    let peak_b = server_b
        .peak_connections
        .load(std::sync::atomic::Ordering::SeqCst);

    assert!(
        peak_a <= 2,
        "Host A exceeded limit after restart: {}",
        peak_a
    );
    assert!(peak_a >= 1, "Host A should have had connections");
    assert!(
        peak_b <= 2,
        "Host B exceeded limit after restart: {}",
        peak_b
    );

    let _ = fs::remove_dir_all(temp);
}

#[test]
fn multi_uri_fallback_and_integrity() {
    let data: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
    let sha256 = sha256_hex(&data);
    let server_good = RangeServer::spawn(data.clone());

    // Server that doesn't exist, will cause network error immediately
    let bad_url = "http://127.0.0.1:1/nonexistent.bin";
    let good_url = format!("http://{}/fixture.bin", server_good.addr);

    let temp = temp_dir("multi-uri");
    let output = temp.join("download.bin");

    let output_run = Command::new(chorus_bin())
        .args([
            "-x",
            "2",
            "--sha256",
            &sha256,
            "-o",
            output.to_str().unwrap(),
            bad_url,
            &good_url,
        ])
        .output()
        .unwrap();

    assert!(
        output_run.status.success(),
        "Fallback run failed: {}",
        String::from_utf8_lossy(&output_run.stderr)
    );
    assert_eq!(fs::read(&output).unwrap(), data);
    assert!(String::from_utf8_lossy(&output_run.stdout).contains("Fallback to next source"));

    // Incompatible resume test:
    // Create a partial file with a different metadata string (resume_key)
    let temp2 = temp_dir("multi-uri-incompat");
    let output2 = temp2.join("download2.bin");
    let part2 = with_suffix(&output2, ".part");
    let meta2 = with_suffix(&output2, ".part.meta");
    let ranges2 = with_suffix(&output2, ".part.ranges");

    fs::write(&part2, vec![0; data.len()]).unwrap();
    fs::write(&meta2, "size=99999\netag=\nlast_modified=\n").unwrap();
    fs::write(&ranges2, format!("0-{}", data.len() - 1)).unwrap();

    let status2 = Command::new(chorus_bin())
        .args([
            "-x",
            "2",
            "--sha256",
            &sha256,
            "-o",
            output2.to_str().unwrap(),
            &good_url,
        ])
        .output()
        .unwrap();

    // It should wipe the partial file since metadata is incompatible and download correctly
    assert!(
        status2.status.success(),
        "Incompatible resume run failed: {}",
        String::from_utf8_lossy(&status2.stderr)
    );
    assert_eq!(fs::read(&output2).unwrap(), data);

    let _ = fs::remove_dir_all(temp);
    let _ = fs::remove_dir_all(temp2);
}

#[test]
fn test_controls_and_rate_limits() {
    let data: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect(); // 256KB
    let server = RangeServer::spawn(data.clone());

    let temp = temp_dir("controls");
    let jobs = temp.join("jobs");
    let output = temp.join("out.bin");
    let url = format!("http://{}/fixture.bin", server.addr);

    // 1. Rate Limiting Test
    // 256KB at 64KB/s should take roughly 3 seconds
    let start_time = Instant::now();
    let status = Command::new(chorus_bin())
        .args([
            "--job-dir",
            jobs.to_str().unwrap(),
            "--limit-rate",
            "65536", // 64 KB/s
            "-o",
            output.to_str().unwrap(),
            &url,
        ])
        .status()
        .unwrap();

    let elapsed = start_time.elapsed();
    assert!(status.success());
    assert!(
        elapsed.as_millis() >= 3500,
        "Download was too fast, rate limiter didn't work. Took {}ms",
        elapsed.as_millis()
    );
    assert_eq!(fs::read(&output).unwrap(), data);

    // Clear for next test
    let _ = fs::remove_file(&output);

    // 2. Pause and Resume Test
    let output2 = temp.join("out2.bin");

    let server_slow = RangeServer::spawn_slow(data.clone(), 32 * 1024, Duration::from_millis(1500));
    let url_slow = format!("http://{}/fixture.bin", server_slow.addr);

    let mut child = Command::new(chorus_bin())
        .args([
            "-x",
            "2",
            "--piece-size",
            "65536",
            "--job-dir",
            jobs.to_str().unwrap(),
            "-o",
            output2.to_str().unwrap(),
            &url_slow,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    // Wait for the job to be registered
    thread::sleep(Duration::from_millis(1500));

    // Find job id
    let manifest_path = fs::read_dir(&jobs)
        .unwrap()
        .find_map(|entry| {
            let path = entry.unwrap().path();
            if path.to_string_lossy().ends_with(".chorus.json")
                && !path.to_string_lossy().ends_with("registry.chorus.json")
            {
                let content = fs::read_to_string(&path).unwrap();
                if content.contains("out2.bin") {
                    Some(path)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .unwrap();

    let manifest_content = fs::read_to_string(&manifest_path).unwrap();
    let id_line = manifest_content
        .lines()
        .find(|l| l.contains("\"id\":"))
        .unwrap();
    let job_id = id_line.split('"').nth(3).unwrap().to_string();

    // Pause the job
    let pause_status = Command::new(chorus_bin())
        .args(["--job-dir", jobs.to_str().unwrap(), "--pause-job", &job_id])
        .status()
        .unwrap();

    assert!(pause_status.success());

    // The child should exit now with a non-zero status
    let _ = child.wait();

    assert!(with_suffix(&output2, ".part").exists());

    // Resume the job
    let resume_status = Command::new(chorus_bin())
        .args(["--job-dir", jobs.to_str().unwrap(), "--resume-job", &job_id])
        .status()
        .unwrap();

    assert!(resume_status.success());
    assert_eq!(fs::read(&output2).unwrap(), data);

    // 3. Cancel Test
    let output3 = temp.join("out3.bin");
    let mut child = Command::new(chorus_bin())
        .args([
            "-x",
            "2",
            "--piece-size",
            "65536",
            "--remove-partial",
            "--job-dir",
            jobs.to_str().unwrap(),
            "-o",
            output3.to_str().unwrap(),
            &url_slow,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    thread::sleep(Duration::from_millis(1500));

    let manifest_path3 = fs::read_dir(&jobs)
        .unwrap()
        .find_map(|entry| {
            let path = entry.unwrap().path();
            if path.to_string_lossy().ends_with(".chorus.json")
                && !path.to_string_lossy().ends_with("registry.chorus.json")
            {
                let content = fs::read_to_string(&path).unwrap();
                if content.contains("out3.bin") {
                    Some(path)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .unwrap();

    let manifest_content3 = fs::read_to_string(&manifest_path3).unwrap();
    let id_line3 = manifest_content3
        .lines()
        .find(|l| l.contains("\"id\":"))
        .unwrap();
    let job_id3 = id_line3.split('"').nth(3).unwrap().to_string();

    // Cancel the job with remove-partial
    let cancel_status = Command::new(chorus_bin())
        .args([
            "--job-dir",
            jobs.to_str().unwrap(),
            "--cancel-job",
            &job_id3,
            "--remove-partial",
        ])
        .status()
        .unwrap();

    assert!(cancel_status.success());
    let _ = child.wait();

    // Since we used remove-partial, the partial file should be gone
    assert!(!with_suffix(&output3, ".part").exists());

    let _ = fs::remove_dir_all(temp);
}
