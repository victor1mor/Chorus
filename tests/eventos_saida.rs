use serde_json::Value;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Output};
use std::thread;
use std::time::Duration;

#[test]
fn stdout_com_events_e_ndjson_v1() {
    let output = download(true);
    let stdout = String::from_utf8(output.stdout).unwrap();
    let events: Vec<Value> = stdout
        .lines()
        .map(|line| {
            let event: Value = serde_json::from_str(line).unwrap();
            assert_eq!(event["v"], 1);
            event
        })
        .collect();

    let created = &events[0];
    assert_eq!(created["type"], "JobCreated");
    let id = &created["data"]["id"];
    assert!(id.is_string());
    assert!(events.iter().all(|event| &event["data"]["id"] == id));
    assert_eq!(events[1]["type"], "StateChanged");
    assert_eq!(events[1]["data"]["from"], "Waiting");
    assert_eq!(events[1]["data"]["to"], "Active");
    assert!(events.iter().any(|event| event["type"] == "Progress"));
    let completed = events.last().unwrap();
    assert_eq!(completed["type"], "StateChanged");
    assert_eq!(completed["data"]["to"], "Complete");
}

#[test]
fn stdout_sem_events_tem_conclusao_sem_escape() {
    let output = download(false);
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(!stdout.contains('\x1b'));
    assert!(
        stdout
            .lines()
            .any(|line| line.starts_with("Chorus: job ") && line.ends_with(" complete"))
    );
}

#[test]
fn falha_sem_expor_credenciais_ou_token() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    for url in [
        format!("http://user:senha@{addr}/x?token=abc"),
        format!("http://user:senha,extra@{addr}/x?token=abc"),
    ] {
        let output = chorus()
            .args(["--retries", "0", "--rpc-auth-token", "SEGREDO123"])
            .arg(url)
            .output()
            .unwrap();
        assert!(!output.status.success());
        let stdout = String::from_utf8(output.stdout).unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(stderr.contains("error sending request"));
        for secret in ["senha", "extra", "abc", "SEGREDO123"] {
            assert!(!stdout.contains(secret));
            assert!(!stderr.contains(secret));
        }
    }
}

fn chorus() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_Chorus"));
    command
        .args(["--disable-dht", "--timeout-ms", "5000"])
        .env("NO_PROXY", "127.0.0.1");
    command
}

fn download(events: bool) -> Output {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let path = std::env::temp_dir().join(format!(
        "chorus-c6-{}-{}.bin",
        std::process::id(),
        addr.port()
    ));
    let output = thread::scope(|scope| {
        scope.spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut reader = BufReader::new(&mut stream);
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap() == 0 {
                    break;
                }
                let head = line.starts_with("HEAD ");
                loop {
                    line.clear();
                    if reader.read_line(&mut line).unwrap() == 0 || line == "\r\n" {
                        break;
                    }
                }
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Length: 6\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n"
                )
                .unwrap();
                if !head {
                    stream.write_all(b"chorus").unwrap();
                }
            }
        });

        let mut command = chorus();
        if events {
            command.arg("--events");
        }
        let output = command
            .args(["--connections", "1", "-o"])
            .arg(&path)
            .arg(format!("http://{addr}/x"))
            .output();
        // Uma conexão vazia encerra o servidor mesmo quando o processo falha cedo.
        drop(TcpStream::connect(addr));
        output.unwrap()
    });
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(fs::read(&path).unwrap(), b"chorus");
    fs::remove_file(path).unwrap();
    output
}
