use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::process::Command;
use tokio::time::timeout;

#[tokio::test]
async fn rpc_recusa_loopback_esquema_e_saida_invalida() {
    let mut server = Command::new(env!("CARGO_BIN_EXE_Chorus"))
        .args(["--rpc-bind", "127.0.0.1:0", "--disable-dht"])
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stdout = BufReader::new(server.stdout.take().unwrap());

    timeout(Duration::from_secs(10), async {
        let mut line = String::new();
        stdout.read_line(&mut line).await.unwrap();
        let address = line
            .trim_end()
            .strip_prefix("Chorus: RPC/WebSocket Server listening on ")
            .expect("RPC startup address");
        let loopback_url = format!("http://{address}/x");
        let mut connection = BufReader::new(TcpStream::connect(address).await.unwrap());

        for (id, (urls, output, error)) in [
            (
                vec![loopback_url.as_str()],
                "test.bin",
                "destination is a private or local address",
            ),
            (
                vec!["http://localhost:0/x", "ftp://localhost/x"],
                "test.bin",
                "unsupported URL scheme: ftp",
            ),
            (
                vec!["http://localhost:0/x"],
                "../test.bin",
                "output must be a relative path inside the download directory",
            ),
        ]
        .into_iter()
        .enumerate()
        {
            let request = serde_json::json!({
                "jsonrpc": "2.0",
                "method": "add_job",
                "params": { "urls": urls, "output": output },
                "id": id,
            });
            connection
                .get_mut()
                .write_all(format!("{request}\n").as_bytes())
                .await
                .unwrap();
            line.clear();
            connection.read_line(&mut line).await.unwrap();
            let response: serde_json::Value = serde_json::from_str(&line).unwrap();
            assert_eq!(response["id"], id);
            assert_eq!(response["error"]["code"], -32000);
            assert_eq!(response["error"]["message"], error);
            assert!(response.get("result").is_none());
        }
    })
    .await
    .expect("RPC timed out");

    server.kill().await.unwrap();
}

#[test]
fn cli_recusa_esquema_de_url_alternativa_sem_exibir_url() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_Chorus"))
        .args([
            "--disable-dht",
            "http://127.0.0.1:0/x",
            "ftp://localhost/segredo-da-url",
        ])
        .env("NO_PROXY", "127.0.0.1")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert_eq!(
        String::from_utf8(output.stderr).unwrap().trim(),
        "Error: unsupported URL scheme: ftp"
    );
    assert!(output.stdout.is_empty());
}
