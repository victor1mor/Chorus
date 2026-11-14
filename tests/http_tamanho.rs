use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const SEM_TAMANHO: &str = "200 OK";
static PROXIMA_PASTA: AtomicUsize = AtomicUsize::new(0);

fn dados() -> Vec<u8> {
    (0..300 * 1024).map(|i| (i % 251) as u8).collect()
}

#[test]
fn cria_arquivo_vazio() {
    let resposta = "200 OK\r\nContent-Length: 0";
    verificar_download(resposta, resposta, &[], None, Ok(&[]));
}

#[test]
fn baixa_ate_fechar_conexao() {
    let corpo = dados();
    let sha: String = Sha256::digest(&corpo)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    verificar_download(SEM_TAMANHO, SEM_TAMANHO, &corpo, Some(&sha), Ok(&corpo));
}

#[test]
fn baixa_resposta_chunked() {
    let corpo = dados();
    let mut codificado = Vec::new();
    for chunk in corpo.chunks(16 * 1024) {
        write!(codificado, "{:x}\r\n", chunk.len()).unwrap();
        codificado.extend_from_slice(chunk);
        codificado.extend_from_slice(b"\r\n");
    }
    codificado.extend_from_slice(b"0\r\n\r\n");
    let get = "200 OK\r\nTransfer-Encoding: chunked";
    verificar_download(SEM_TAMANHO, get, &codificado, None, Ok(&corpo));
}

#[test]
fn recusa_sha256_incorreto() {
    verificar_download(
        SEM_TAMANHO,
        SEM_TAMANHO,
        &dados(),
        Some(&"0".repeat(64)),
        Err(("Integrity", "SHA-256 mismatch")),
    );
}

#[test]
fn baixa_sem_head() {
    let corpo = dados();
    let head = "405 Method Not Allowed\r\nContent-Length: 0";
    verificar_download(head, SEM_TAMANHO, &corpo, None, Ok(&corpo));
}

#[test]
fn recusa_corpo_menor_que_content_length() {
    let get = "200 OK\r\nContent-Length: 1000";
    verificar_download(
        SEM_TAMANHO,
        get,
        &[0; 500],
        None,
        Err(("Network", "network")),
    );
}

fn verificar_download(
    head: &str,
    get: &str,
    corpo: &[u8],
    sha: Option<&str>,
    esperado: Result<&[u8], (&str, &str)>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let indice = PROXIMA_PASTA.fetch_add(1, Ordering::Relaxed);
    let pasta =
        std::env::temp_dir().join(format!("chorus-c4-{}-{nanos}-{indice}", std::process::id()));
    fs::create_dir(&pasta).unwrap();
    let arquivo = pasta.join("download.bin");
    let resultado = thread::scope(|scope| {
        scope.spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut leitor = BufReader::new(&mut stream);
                let mut linha = String::new();
                if leitor.read_line(&mut linha).unwrap() == 0 {
                    break;
                }
                let is_head = linha.starts_with("HEAD ");
                let mut range = false;
                loop {
                    linha.clear();
                    if leitor.read_line(&mut linha).unwrap() == 0 || linha == "\r\n" {
                        break;
                    }
                    range |= linha.to_ascii_lowercase().starts_with("range:");
                }
                let resposta = if is_head { head } else { get };
                write!(stream, "HTTP/1.1 {resposta}\r\nConnection: close\r\n\r\n").unwrap();
                if !is_head {
                    let escrito = stream.write_all(corpo);
                    if !range {
                        escrito.unwrap();
                    }
                }
            }
        });
        let mut comando = Command::new(env!("CARGO_BIN_EXE_Chorus"));
        comando
            .args(["--disable-dht", "--timeout-ms", "5000", "--job-dir"])
            .arg(&pasta);
        comando.arg("-o").arg(&arquivo);
        if let Some(sha) = sha {
            comando.args(["--sha256", sha]);
        }
        let resultado = comando.arg(format!("http://{addr}/download.bin")).output();
        // Uma conexão sem requisição encerra o servidor, inclusive se o binário falhar cedo.
        drop(std::net::TcpStream::connect(addr));
        resultado.unwrap()
    });
    let registro: Value =
        serde_json::from_slice(&fs::read(pasta.join("registry.chorus.json")).unwrap()).unwrap();
    let estado = &registro["jobs"][0]["state"];
    let stderr = String::from_utf8_lossy(&resultado.stderr);
    match esperado {
        Ok(bytes) => {
            assert!(resultado.status.success(), "{stderr}");
            assert_eq!(estado, "Complete");
            assert_eq!(fs::read(&arquivo).expect("download final ausente"), bytes);
        }
        Err((categoria, mensagem)) => {
            assert!(!resultado.status.success());
            assert_eq!(estado["Failed"], categoria, "{stderr}");
            assert!(stderr.contains(mensagem), "{stderr}");
            assert!(!arquivo.exists());
        }
    }
    fs::remove_dir_all(pasta).unwrap();
}
