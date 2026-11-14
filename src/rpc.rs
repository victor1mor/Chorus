use crate::events::{EventBroker, EventType};
use crate::job::{JobManifest, JobPrivateData, JobRegistry, JobState, com_registro};
use crate::magnet::MagnetLink;
use crate::metainfo::Metainfo;
use crate::rede::{saida_segura, validar_url};
use crate::saida;
use crate::torrent_job::{criar_magnet, criar_torrent, tem_fonte_torrent};
use anyhow::{Context, Result, bail, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use futures::SinkExt;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};

const MAX_NDJSON_LINE: usize = 8 * 1024 * 1024;
const MAX_TORRENT_BYTES: usize = 4 * 1024 * 1024;

#[derive(Deserialize)]
struct RpcRequest {
    jsonrpc: String,
    method: String,
    #[serde(default)]
    params: serde_json::Value,
    id: Option<serde_json::Value>,
}

#[derive(Serialize)]
struct RpcResponse {
    jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcError>,
    id: Option<serde_json::Value>,
}

#[derive(Serialize)]
struct RpcError {
    code: i32,
    message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Protocolo {
    Ndjson,
    WebSocket,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Acesso {
    Permitido,
    Recusado(&'static str),
}

pub(crate) fn decidir_acesso(
    is_loopback: bool,
    allow_remote_insecure: bool,
    token: Option<&str>,
    protocolo: Protocolo,
    authorization: Option<&str>,
) -> Acesso {
    if is_loopback {
        return Acesso::Permitido;
    }
    if !allow_remote_insecure {
        return Acesso::Recusado(
            "non-local connection requires --allow-remote-insecure and a token",
        );
    }
    let Some(token) = token else {
        return Acesso::Recusado("non-local connection requires an auth token");
    };
    if protocolo == Protocolo::Ndjson {
        return Acesso::Recusado(
            "non-local NDJSON is not allowed; use WebSocket with Authorization",
        );
    }

    if let Some(authorization) = authorization {
        let expected = format!("Bearer {token}");
        if authorization.len() == expected.len() {
            // Keep the compiler from shortcutting the byte-by-byte comparison.
            let difference = authorization.bytes().zip(expected.bytes()).fold(
                0,
                |difference, (actual, expected)| {
                    std::hint::black_box(difference | (actual ^ expected))
                },
            );
            if difference == 0 {
                return Acesso::Permitido;
            }
        }
    }
    Acesso::Recusado("unauthorized")
}

pub struct RpcServer {
    bind: String,
    directory: PathBuf,
    event_broker: EventBroker,
    auth_token: Option<String>,
    allow_remote_insecure: bool,
    download_dir: PathBuf,
    rede_protegida: bool,
    dht_ligado: bool,
}

impl RpcServer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bind: String,
        directory: PathBuf,
        event_broker: EventBroker,
        auth_token: Option<String>,
        allow_remote_insecure: bool,
        download_dir: PathBuf,
        rede_protegida: bool,
        dht_ligado: bool,
    ) -> Self {
        Self {
            bind,
            directory,
            event_broker,
            auth_token,
            allow_remote_insecure,
            download_dir,
            rede_protegida,
            dht_ligado,
        }
    }

    pub async fn serve(self) -> Result<()> {
        let listener = TcpListener::bind(&self.bind).await?;
        saida::info(format!(
            "Chorus: RPC/WebSocket Server listening on {}",
            listener.local_addr()?
        ));
        let server = Arc::new(self);

        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    let remote_is_loopback = addr.ip().is_loopback();
                    let allow_remote_insecure = server.allow_remote_insecure;
                    if !remote_is_loopback
                        && (!allow_remote_insecure || server.auth_token.is_none())
                        && let Acesso::Recusado(reason) = decidir_acesso(
                            remote_is_loopback,
                            allow_remote_insecure,
                            server.auth_token.as_deref(),
                            Protocolo::Ndjson,
                            None,
                        )
                    {
                        saida::erro(format!("Rejected non-local connection: {reason}"));
                        continue;
                    }

                    let server = Arc::clone(&server);

                    tokio::spawn(async move {
                        let mut peek_buf = [0u8; 1];
                        if socket.peek(&mut peek_buf).await.unwrap_or(0) == 0 {
                            return;
                        }

                        if peek_buf[0] == b'{' {
                            handle_ndjson_connection(socket, &server, remote_is_loopback).await;
                        } else if peek_buf[0] == b'G' {
                            handle_ws_connection(
                                socket,
                                server.auth_token.clone(),
                                remote_is_loopback,
                                allow_remote_insecure,
                                server.event_broker.clone(),
                            )
                            .await;
                        } else {
                            saida::erro(format!("Unknown protocol byte: {}", peek_buf[0]));
                        }
                    });
                }
                Err(e) => {
                    saida::erro(format!("RPC accept error: {}", e));
                }
            }
        }
    }
}

async fn handle_ndjson_connection(
    mut socket: tokio::net::TcpStream,
    server: &RpcServer,
    is_loopback: bool,
) {
    if let Acesso::Recusado(reason) = decidir_acesso(
        is_loopback,
        server.allow_remote_insecure,
        server.auth_token.as_deref(),
        Protocolo::Ndjson,
        None,
    ) {
        saida::erro(format!("Rejected NDJSON connection: {reason}"));
        return;
    }

    let (read_half, mut write_half) = socket.split();
    let mut reader = BufReader::new(read_half);
    let mut line = Vec::new();

    loop {
        line.clear();
        match (&mut reader)
            .take(MAX_NDJSON_LINE as u64 + 1)
            .read_until(b'\n', &mut line)
            .await
        {
            Ok(0) => break, // EOF
            Ok(n) if n > MAX_NDJSON_LINE => {
                // Best effort so a client that isn't reading cannot delay the close.
                let _ = write_half.try_write(
                    b"{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32000,\"message\":\"NDJSON line exceeds 8 MiB\"},\"id\":null}\n",
                );
                break;
            }
            Ok(_) => {
                let Ok(line) = std::str::from_utf8(&line) else {
                    break;
                };
                let response = handle_request(line, server).await;
                if let Some(res) = &response.result
                    && line.contains("\"add_job\"")
                    && res.get("id").is_some()
                {
                    let id = res.get("id").unwrap().as_str().unwrap().to_string();
                    server.event_broker.emit(EventType::JobCreated { id });
                }
                if let Ok(res_str) = serde_json::to_string(&response) {
                    let _ = write_half.write_all(res_str.as_bytes()).await;
                    let _ = write_half.write_all(b"\n").await;
                }
            }
            Err(_) => break,
        }
    }
}

#[allow(clippy::result_large_err)]
async fn handle_ws_connection(
    socket: tokio::net::TcpStream,
    expected_token: Option<String>,
    is_loopback: bool,
    allow_remote_insecure: bool,
    broker: EventBroker,
) {
    let callback =
        |req: &Request, response: Response| -> std::result::Result<Response, ErrorResponse> {
            let authorization = req
                .headers()
                .get("Authorization")
                .and_then(|value| value.to_str().ok());
            if matches!(
                decidir_acesso(
                    is_loopback,
                    allow_remote_insecure,
                    expected_token.as_deref(),
                    Protocolo::WebSocket,
                    authorization,
                ),
                Acesso::Recusado(_)
            ) {
                let err: ErrorResponse = Response::builder()
                    .status(tokio_tungstenite::tungstenite::http::StatusCode::UNAUTHORIZED)
                    .body(Some("Unauthorized".into()))
                    .unwrap();
                return Err(err);
            }
            Ok(response)
        };

    match tokio_tungstenite::accept_hdr_async(socket, callback).await {
        Ok(mut ws_stream) => {
            let mut rx = broker.subscribe();
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        if let Ok(json) = serde_json::to_string(&event)
                            && ws_stream
                                .send(tokio_tungstenite::tungstenite::Message::Text(json.into()))
                                .await
                                .is_err()
                        {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        }
        Err(e) => {
            saida::erro(format!("WebSocket handshake error: {}", e));
        }
    }
}

async fn handle_request(line: &str, server: &RpcServer) -> RpcResponse {
    let req: RpcRequest = match serde_json::from_str(line) {
        Ok(r) => r,
        Err(_) => return make_error(None, -32700, "Parse error".to_string()),
    };

    if req.jsonrpc != "2.0" {
        return make_error(req.id, -32600, "Invalid Request".to_string());
    }

    let directory = &server.directory;
    let result = match req.method.as_str() {
        "add_job" => handle_add_job(&req.params, server),
        "get_job" => handle_get_job(&req.params, directory),
        "list_jobs" => handle_list_jobs(directory),
        "pause_job" => handle_state_transition(&req.params, directory, JobState::Paused),
        "resume_job" => handle_resume_job(&req.params, directory),
        "cancel_job" => handle_state_transition(&req.params, directory, JobState::Cancelled),
        _ => return make_error(req.id, -32601, "Method not found".to_string()),
    };

    match result {
        Ok(res) => RpcResponse {
            jsonrpc: "2.0".to_string(),
            result: Some(res),
            error: None,
            id: req.id,
        },
        Err(e) => make_error(req.id, -32000, saida::redigir_urls(&e.to_string())),
    }
}

fn make_error(id: Option<serde_json::Value>, code: i32, message: String) -> RpcResponse {
    RpcResponse {
        jsonrpc: "2.0".to_string(),
        result: None,
        error: Some(RpcError { code, message }),
        id,
    }
}

fn handle_add_job(params: &serde_json::Value, server: &RpcServer) -> Result<serde_json::Value> {
    ensure!(
        params.get("peers").is_none(),
        "peers are not accepted over RPC"
    );
    let output_str = params
        .get("output")
        .and_then(|v| v.as_str())
        .context("Missing 'output' field")?;
    let output_path = saida_segura(&server.download_dir, output_str)?;

    let directory = &server.directory;
    let id = match (
        params.get("urls"),
        params.get("torrent"),
        params.get("magnet"),
    ) {
        (Some(urls), None, None) => {
            let urls: Vec<String> = urls
                .as_array()
                .context("Missing or invalid 'urls' array")?
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
            ensure!(!urls.is_empty(), "'urls' array cannot be empty");
            for url in &urls {
                validar_url(url, server.rede_protegida)?;
            }
            com_registro(directory, |registry| {
                let job = JobManifest::new(&urls, &output_path);
                let path = job.save(directory)?;
                JobPrivateData::new(&urls).save(directory, &job.id)?;
                registry.update(&job, &path);
                Ok(job.id)
            })?
        }
        (None, Some(torrent), None) => {
            let encoded = torrent
                .as_str()
                .context("'torrent' must be a base64 string")?;
            let data = STANDARD.decode(encoded).context("invalid torrent base64")?;
            ensure!(data.len() <= MAX_TORRENT_BYTES, ".torrent exceeds 4 MiB");
            let metainfo = Metainfo::from_bytes(&data)?;
            ensure!(
                tem_fonte_torrent(
                    metainfo.tracker_urls(),
                    server.rede_protegida,
                    server.dht_ligado,
                    metainfo.private,
                ),
                if metainfo.private {
                    "private .torrent cannot use DHT and requires a tracker"
                } else {
                    "no usable tracker"
                }
            );
            criar_torrent(directory, &output_path, &data, &metainfo, Vec::new())?
        }
        (None, None, Some(magnet)) => {
            let link = magnet.as_str().context("'magnet' must be a string")?;
            let magnet = MagnetLink::parse(link)?;
            ensure!(
                tem_fonte_torrent(
                    magnet.trackers.iter().map(String::as_str),
                    server.rede_protegida,
                    server.dht_ligado,
                    false,
                ),
                "no usable tracker"
            );
            criar_magnet(directory, &output_path, link, &magnet, Vec::new())?
        }
        _ => bail!("exactly one of 'urls', 'torrent' or 'magnet' is required"),
    };
    Ok(serde_json::json!({ "id": id }))
}

fn handle_get_job(params: &serde_json::Value, directory: &Path) -> Result<serde_json::Value> {
    let id = params
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing 'id' field"))?;

    let registry = JobRegistry::load(directory)?;
    let record = registry
        .get(id)
        .ok_or_else(|| anyhow::anyhow!("Job ID not found"))?;

    let state_str = serde_json::to_string(&record.state)?;

    Ok(serde_json::json!({
        "id": record.id,
        "state": serde_json::from_str::<serde_json::Value>(&state_str).unwrap(),
        "manifest_path": record.manifest_path
    }))
}

fn handle_list_jobs(directory: &Path) -> Result<serde_json::Value> {
    let registry = JobRegistry::load(directory)?;
    let mut jobs = Vec::new();
    for record in registry.list() {
        let state_str = serde_json::to_string(&record.state)?;
        jobs.push(serde_json::json!({
            "id": record.id,
            "state": serde_json::from_str::<serde_json::Value>(&state_str).unwrap()
        }));
    }
    Ok(serde_json::json!({ "jobs": jobs }))
}

fn handle_state_transition(
    params: &serde_json::Value,
    directory: &Path,
    target_state: JobState,
) -> Result<serde_json::Value> {
    let id = params
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing 'id' field"))?;

    com_registro(directory, |registry| {
        let record = registry
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Job ID not found"))?;

        let mut job = JobManifest::load(&record.manifest_path)?;
        job.transition_to(target_state)?;
        let path = job.save(directory)?;
        registry.update(&job, &path);
        Ok(serde_json::json!({ "success": true }))
    })
}

fn handle_resume_job(params: &serde_json::Value, directory: &Path) -> Result<serde_json::Value> {
    let id = params
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing 'id' field"))?;

    com_registro(directory, |registry| {
        let record = registry
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Job ID not found"))?;

        if record.state == JobState::Complete {
            return Err(anyhow::anyhow!("Cannot resume job: already complete"));
        }

        let mut job = JobManifest::load(&record.manifest_path)?;
        if job.state != JobState::Waiting {
            job.transition_to(JobState::Waiting)?;
        }
        let path = job.save(directory)?;
        registry.update(&job, &path);
        Ok(serde_json::json!({ "success": true }))
    })
}

#[cfg(test)]
mod access_tests {
    use super::{
        Acesso::{Permitido, Recusado},
        Protocolo::{Ndjson, WebSocket},
        decidir_acesso,
    };

    #[test]
    fn decidir_acesso_cobre_conexoes_locais_e_remotas() {
        let token = Some("test-token");
        let bearer = Some("Bearer test-token");
        let flag_required = "non-local connection requires --allow-remote-insecure and a token";
        let token_required = "non-local connection requires an auth token";
        let ws_required = "non-local NDJSON is not allowed; use WebSocket with Authorization";
        let cases = [
            (true, false, None, Ndjson, None, Permitido),
            (true, false, None, WebSocket, None, Permitido),
            (false, false, None, Ndjson, None, Recusado(flag_required)),
            (
                false,
                false,
                token,
                WebSocket,
                bearer,
                Recusado(flag_required),
            ),
            (false, true, None, Ndjson, None, Recusado(token_required)),
            (false, true, None, WebSocket, None, Recusado(token_required)),
            (false, true, token, Ndjson, bearer, Recusado(ws_required)),
            (false, true, token, WebSocket, bearer, Permitido),
            (
                false,
                true,
                token,
                WebSocket,
                Some("Bearer test-tokeN"),
                Recusado("unauthorized"),
            ),
            (
                false,
                true,
                token,
                WebSocket,
                Some("Bearer short"),
                Recusado("unauthorized"),
            ),
            (
                false,
                true,
                token,
                WebSocket,
                None,
                Recusado("unauthorized"),
            ),
        ];

        for (index, (loopback, allow_remote, token, protocol, auth, expected)) in
            cases.into_iter().enumerate()
        {
            assert_eq!(
                decidir_acesso(loopback, allow_remote, token, protocol, auth),
                expected,
                "access case {index}"
            );
        }
    }
}

#[cfg(test)]
mod fuzz_tests {
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn fuzz_rpc_json_parser(data in "\\PC*") {
            // Exercise the JSON-RPC request parser with arbitrary strings.
            // The handler must never panic regardless of input.
            let dir = std::env::temp_dir().join(format!(
                "chorus-rpc-fuzz-{}",
                std::process::id()
            ));
            let _ = std::fs::create_dir_all(&dir);
            let server = super::RpcServer::new(
                String::new(),
                dir.clone(),
                crate::events::EventBroker::new(1),
                None,
                false,
                dir.clone(),
                true,
                false,
            );
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let _ = super::handle_request(&data, &server).await;
            });
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}
