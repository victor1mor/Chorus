use futures::StreamExt;
use std::net::TcpListener;
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

#[tokio::test]
async fn events_smoke_test() {
    let dir = temp_dir("events-smoke");
    let port = get_free_port();
    let bind_addr = format!("127.0.0.1:{}", port);

    let mut server = Command::new(chorus_bin())
        .arg("--job-dir")
        .arg(&dir)
        .arg("--download-dir")
        .arg(&dir)
        .arg("--rpc-bind")
        .arg(&bind_addr)
        .arg("--disable-dht")
        .arg("--allow-private-network")
        .env("NO_PROXY", "127.0.0.1")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();

    tokio::time::sleep(Duration::from_millis(1500)).await;

    let ws_url = format!("ws://{}", bind_addr);
    let (mut ws_stream, _) = tokio_tungstenite::connect_async(&ws_url)
        .await
        .expect("Failed to connect");

    let mut tcp_stream = tokio::net::TcpStream::connect(&bind_addr).await.unwrap();

    let add_job_req = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "add_job",
        "params": {
            "urls": ["http://127.0.0.1:9999/dummy.bin"], // doesn't matter if it fails downloading
            "output": "dummy.bin"
        },
        "id": 1
    });

    let mut req_str = serde_json::to_string(&add_job_req).unwrap();
    req_str.push('\n');
    tokio::io::AsyncWriteExt::write_all(&mut tcp_stream, req_str.as_bytes())
        .await
        .unwrap();

    // Read the WS stream to see the JobCreated event
    let msg = ws_stream.next().await.unwrap().unwrap();
    let text = msg.to_text().unwrap();

    let event: serde_json::Value = serde_json::from_str(text).unwrap();
    assert_eq!(event["type"], "JobCreated");
    assert!(event["seq"].as_u64().unwrap() > 0);
    assert!(!event["data"]["id"].as_str().unwrap().is_empty());

    server.kill().unwrap();
    server.wait().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Proves that a slow WS client that never reads its messages
/// does not block the server from accepting new NDJSON commands.
/// The server's broadcast channel will lag, but must not crash.
#[tokio::test]
async fn ws_slow_client_does_not_block_server() {
    let dir = temp_dir("ws-slow");
    let port = get_free_port();
    let bind_addr = format!("127.0.0.1:{}", port);

    let mut server = Command::new(chorus_bin())
        .arg("--job-dir")
        .arg(&dir)
        .arg("--download-dir")
        .arg(&dir)
        .arg("--rpc-bind")
        .arg(&bind_addr)
        .arg("--disable-dht")
        .arg("--allow-private-network")
        .env("NO_PROXY", "127.0.0.1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Connect a WS client but NEVER read from it (simulates a slow consumer)
    let ws_url = format!("ws://{}", bind_addr);
    let (_ws_stream, _) = tokio_tungstenite::connect_async(&ws_url)
        .await
        .expect("Failed to connect WS");

    // Now send multiple NDJSON add_job requests to flood events
    let tcp_stream = tokio::net::TcpStream::connect(&bind_addr).await.unwrap();
    let (read_half, mut write_half) = tcp_stream.into_split();
    let mut reader = tokio::io::BufReader::new(read_half);

    for i in 0..20 {
        let add_job_req = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "add_job",
            "params": {
                "urls": [format!("http://127.0.0.1:9999/slow-{}.bin", i)],
                "output": format!("slow-{}.bin", i)
            },
            "id": i + 100
        });

        let mut req_str = serde_json::to_string(&add_job_req).unwrap();
        req_str.push('\n');
        tokio::io::AsyncWriteExt::write_all(&mut write_half, req_str.as_bytes())
            .await
            .unwrap();

        // Read the NDJSON response to confirm the server is not blocked
        let mut response_line = String::new();
        tokio::io::AsyncBufReadExt::read_line(&mut reader, &mut response_line)
            .await
            .unwrap();
        let res: serde_json::Value = serde_json::from_str(&response_line).unwrap();
        assert!(
            res.get("result").is_some(),
            "Server must respond even with a slow WS client: {}",
            response_line
        );
    }

    // If we got here, the server handled all 20 requests without blocking.
    // The slow WS client's backlog was handled gracefully.

    server.kill().unwrap();
    server.wait().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
