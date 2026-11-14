use sha2::{Digest, Sha256};
use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
    let path = std::env::temp_dir().join(format!(
        "chorus-perf-{}-{}-{}",
        name,
        std::process::id(),
        nanos
    ));
    fs::create_dir_all(&path).unwrap();
    path
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
fn benchmark_fast_local_download() {
    // 5 MB of data
    let data: Vec<u8> = (0..5 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    let sha256 = sha256_hex(&data);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let data_arc = Arc::new(data.clone());

    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let data = Arc::clone(&data_arc);
            thread::spawn(move || {
                let mut stream = stream;
                let mut buf = [0; 4096];
                let _ = stream.read(&mut buf);

                let headers = format!(
                    "HTTP/1.1 200 OK\r\n\
                    Content-Length: {}\r\n\
                    ETag: \"perf\"\r\n\
                    Connection: close\r\n\
                    \r\n",
                    data.len()
                );
                let _ = stream.write_all(headers.as_bytes());
                let _ = stream.write_all(&data);
            });
        }
    });

    let temp = temp_dir("benchmark");
    let output = temp.join("download.bin");

    let start = Instant::now();
    let status = Command::new(chorus_bin())
        .args([
            "-x",
            "4",
            "--piece-size",
            "65536",
            "--sha256",
            &sha256,
            "-o",
            output.to_str().unwrap(),
            &format!("http://{}/fixture.bin", addr),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();

    let duration = start.elapsed();
    assert!(status.success());
    assert_eq!(fs::read(&output).unwrap().len(), 5 * 1024 * 1024);

    // Performance tolerance: 5 MB on localhost should take less than 1.5 seconds.
    // If it takes longer, we have a performance regression.
    assert!(
        duration < Duration::from_millis(1500),
        "Download took too long: {:?}",
        duration
    );

    let _ = fs::remove_dir_all(temp);
}
