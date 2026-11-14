use std::process::Stdio;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::process::Command;
use tokio::time::timeout;

#[tokio::test]
async fn rpc_local_aceita_add_job_sem_token() {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir =
        std::env::temp_dir().join(format!("chorus-rpc-remoto-{}-{nanos}", std::process::id()));
    let mut server = Command::new(env!("CARGO_BIN_EXE_Chorus"))
        .arg("--job-dir")
        .arg(&dir)
        .args([
            "--rpc-bind",
            "127.0.0.1:0",
            "--disable-dht",
            "--allow-private-network",
        ])
        .arg("--download-dir")
        .arg(&dir)
        .env("NO_PROXY", "127.0.0.1")
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stdout = BufReader::new(server.stdout.take().unwrap());

    let response = timeout(Duration::from_secs(10), async {
        let mut line = String::new();
        stdout.read_line(&mut line).await?;
        let address = line
            .trim_end()
            .strip_prefix("Chorus: RPC/WebSocket Server listening on ")
            .ok_or_else(|| std::io::Error::other(format!("RPC startup output: {line:?}")))?;
        let mut stream = TcpStream::connect(address).await?;
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "add_job",
            "params": { "urls": ["http://127.0.0.1:0/test.bin"], "output": "test.bin" },
            "id": 1
        });
        stream.write_all(format!("{request}\n").as_bytes()).await?;
        line.clear();
        BufReader::new(stream).read_line(&mut line).await?;
        Ok::<_, std::io::Error>(line)
    })
    .await;

    server.kill().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);

    let line = response.expect("Local RPC timed out").unwrap();
    let result: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(result["id"], 1);
    assert!(
        result["result"]["id"]
            .as_str()
            .is_some_and(|id| !id.is_empty()),
        "{result}"
    );
}
