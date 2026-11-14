use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn chorus_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_Chorus"))
}

fn temp_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "chorus-{}-{}-{}",
        prefix,
        std::process::id(),
        nanos
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn get_free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[test]
fn rpc_server_persists_job_across_restart() {
    let dir = temp_dir("rpc-smoke");
    let port = get_free_port();

    // 1. Start the server
    let mut server = Command::new(chorus_bin())
        .arg("--job-dir")
        .arg(&dir)
        .arg("--download-dir")
        .arg(&dir)
        .arg("--rpc-bind")
        .arg(format!("127.0.0.1:{}", port))
        .arg("--disable-dht")
        .arg("--allow-private-network")
        .env("NO_PROXY", "127.0.0.1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    // Wait for it to listen
    std::thread::sleep(Duration::from_millis(1500)); // increased wait slightly for windows

    let mut stream =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect to RPC");

    // 2. Add job
    let add_job_req = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "add_job",
        "params": {
            "urls": ["http://127.0.0.1:0/test.bin"],
            "output": "test.bin"
        },
        "id": 1
    });

    let mut req_str = serde_json::to_string(&add_job_req).unwrap();
    req_str.push('\n');
    stream.write_all(req_str.as_bytes()).unwrap();

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();

    let res: serde_json::Value = serde_json::from_str(&line).unwrap();
    let job_id = res["result"]["id"].as_str().unwrap().to_string();
    assert!(!job_id.is_empty());

    // 3. Kill the server
    server.kill().unwrap();
    server.wait().unwrap();

    // 4. Restart the server
    let mut server = Command::new(chorus_bin())
        .arg("--job-dir")
        .arg(&dir)
        .arg("--download-dir")
        .arg(&dir)
        .arg("--rpc-bind")
        .arg(format!("127.0.0.1:{}", port))
        .arg("--disable-dht")
        .arg("--allow-private-network")
        .env("NO_PROXY", "127.0.0.1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    std::thread::sleep(Duration::from_millis(1500));

    // 5. Query job
    let mut stream =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect to RPC again");

    let get_job_req = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "get_job",
        "params": {
            "id": job_id
        },
        "id": 2
    });

    let mut req_str = serde_json::to_string(&get_job_req).unwrap();
    req_str.push('\n');
    stream.write_all(req_str.as_bytes()).unwrap();

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();

    let res: serde_json::Value = serde_json::from_str(&line).unwrap();
    println!("Response: {}", line);

    assert_eq!(res["result"]["id"].as_str().expect("id not found"), job_id);
    let state = &res["result"]["state"];
    assert!(!state.is_null(), "state should be present");

    server.kill().unwrap();
    server.wait().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
