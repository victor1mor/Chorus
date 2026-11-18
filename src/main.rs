mod bencode;
mod cli;
mod dht;
mod engine;
pub mod events;
mod http;
mod job;
mod magnet;
mod metadata_transfer;
mod metainfo;
mod peer_transfer;
mod peer_wire;
mod quadro;
mod rede;
mod rpc;
mod saida;
mod semeador_web;
mod storage;
mod throttle;
mod torrent_job;
mod torrent_storage;
mod tracker;
mod tracker_udp;

use crate::cli::Cli;
use crate::engine::DownloadEngine;
use crate::events::{EventBroker, EventType};
use crate::http::{HttpFetcher, Metadata, content_length_from_headers};
use crate::job::{ErrorCategory, JobManifest, JobPrivateData, JobRegistry, JobState, com_registro};
use crate::storage::ChunkStorage;
use crate::throttle::RateLimiter;
use anyhow::{Context, Result, anyhow};
use futures::StreamExt;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use url::Url;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            saida::erro(format!("Error: {e:#}"));
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    let args = Cli::parse_args();
    if args.events {
        saida::ativar_modo_eventos();
    }

    let (directory, is_ephemeral) = match args.job_dir.as_deref() {
        Some(dir) => (dir.to_path_buf(), false),
        None => {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let d = std::env::temp_dir().join(format!(
                "chorus-ephemeral-{}-{}",
                std::process::id(),
                nanos
            ));
            (d, true)
        }
    };

    let event_broker = EventBroker::new(4096);
    let rede_protegida = args.rpc_bind.is_some() && !args.allow_private_network;
    let dht_ligado = !args.disable_dht;
    if let Some(id) = &args.pause_job {
        saida::info(format!("Chorus: Pausing job {}", id));
        if is_ephemeral {
            return Err(anyhow!("--job-dir is required to pause a job by ID"));
        }
        return com_registro(&directory, |registry| {
            let record = registry
                .get(id)
                .ok_or_else(|| anyhow!("Job ID {} not found", id))?;
            let mut job = JobManifest::load(&record.manifest_path)?;
            job.transition_to(JobState::Paused)?;
            let path = job.save(&directory)?;
            registry.update(&job, &path);
            Ok(())
        });
    } else if let Some(id) = &args.cancel_job {
        saida::info(format!("Chorus: Cancelling job {}", id));
        if is_ephemeral {
            return Err(anyhow!("--job-dir is required to cancel a job by ID"));
        }
        return com_registro(&directory, |registry| {
            let record = registry
                .get(id)
                .ok_or_else(|| anyhow!("Job ID {} not found", id))?;
            let mut job = JobManifest::load(&record.manifest_path)?;
            job.transition_to(JobState::Cancelled)?;
            let path = job.save(&directory)?;
            registry.update(&job, &path);
            Ok(())
        });
    } else if let Some(id) = &args.resume_job {
        saida::info(format!("Chorus: Resuming job {}", id));
        if is_ephemeral {
            return Err(anyhow!("--job-dir is required to resume a job by ID"));
        }

        com_registro(&directory, |registry| {
            let record = registry
                .get(id)
                .ok_or_else(|| anyhow!("Job ID {} not found", id))?;

            if record.state == JobState::Complete {
                return Err(anyhow!("Cannot resume job {}: already complete", id));
            }

            let mut job = JobManifest::load(&record.manifest_path)?;

            if job.state != JobState::Waiting {
                job.transition_to(JobState::Waiting)?;
            }
            let path = job.save(&directory)?;
            registry.update(&job, &path);
            Ok(())
        })?;
    } else if let Some(torrent) = &args.torrent {
        let data = fs::read(torrent).context("could not read .torrent file")?;
        let metainfo = metainfo::Metainfo::from_bytes(&data)?;
        if args.peer.is_empty()
            && !torrent_job::tem_fonte_torrent(
                metainfo.tracker_urls(),
                rede_protegida,
                dht_ligado,
                metainfo.private,
            )
        {
            if metainfo.private {
                return Err(anyhow!(torrent_job::PRIVATE_TORRENT_CLI_ERROR));
            }
            return Err(anyhow!(
                ".torrent requires --peer or an HTTP(S)/UDP tracker"
            ));
        }
        let output = std::path::absolute(args.output.as_deref().unwrap_or("."))?;
        let id =
            torrent_job::criar_torrent(&directory, &output, &data, &metainfo, args.peer.clone())?;
        event_broker.emit(EventType::JobCreated { id });
    } else if let Some(link) = &args.magnet {
        let magnet = magnet::MagnetLink::parse(link)?;
        if args.peer.is_empty()
            && !torrent_job::tem_fonte_torrent(
                magnet.trackers.iter().map(String::as_str),
                rede_protegida,
                dht_ligado,
                false,
            )
        {
            return Err(anyhow!(
                "--magnet requires --peer or an HTTP(S)/UDP tracker"
            ));
        }
        let output = std::path::absolute(args.output.as_deref().unwrap_or("."))?;
        let id = torrent_job::criar_magnet(&directory, &output, link, &magnet, args.peer.clone())?;
        event_broker.emit(EventType::JobCreated { id });
    } else if !args.urls.is_empty() {
        for url in &args.urls {
            rede::validar_url(url, false)?;
        }
        if is_ephemeral {
            saida::info(format!(
                "Chorus: Starting download for {}",
                saida::redigir_url(&args.urls[0])
            ));
        }
        let output_path = output_path(&args.urls[0], args.output.as_deref());
        ensure_output_allowed(&output_path, args.no_overwrite)?;

        let id = com_registro(&directory, |registry| {
            let job = JobManifest::new(&args.urls, &output_path);
            let path = job.save(&directory)?;
            let private_data = JobPrivateData::new(&args.urls);
            private_data.save(&directory, &job.id)?;
            registry.update(&job, &path);
            Ok(job.id)
        })?;
        event_broker.emit(EventType::JobCreated { id });
    } else if args.rpc_bind.is_none() && args.urls.is_empty() && args.resume_job.is_none() {
        return Err(anyhow!(
            "Provide URLs, --torrent, --magnet, --resume-job, or --rpc-bind"
        ));
    }

    let mut join_set = tokio::task::JoinSet::new();

    if let Some(ref bind) = args.rpc_bind {
        let auth_token = args.rpc_auth_token.clone();
        let allow_remote_insecure = args.allow_remote_insecure;
        let download_dir =
            std::path::absolute(args.download_dir.as_deref().unwrap_or(Path::new(".")))?;
        let server = rpc::RpcServer::new(
            bind.clone(),
            directory.clone(),
            event_broker.clone(),
            auth_token,
            allow_remote_insecure,
            download_dir,
            rede_protegida,
            dht_ligado,
        );
        join_set.spawn(async move {
            if let Err(e) = server.serve().await {
                saida::erro(format!("RPC Server error: {}", e));
            }
            (String::new(), Ok(()))
        });
    }

    let mut running_jobs: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let mut had_failures = false;
    let mut ephemeral_error = None;

    let global_rate_limiter = Arc::new(RateLimiter::new(args.max_overall_download_limit));

    loop {
        let registry = JobRegistry::load(&directory)?;

        let available_global = args.max_jobs.saturating_sub(running_jobs.len());
        if available_global > 0 {
            let mut host_counts: std::collections::HashMap<String, usize> =
                std::collections::HashMap::new();
            for host in running_jobs.values() {
                *host_counts.entry(host.clone()).or_insert(0) += 1;
            }

            let mut candidates = Vec::new();
            for record in registry.list() {
                if record.state == JobState::Active && !running_jobs.contains_key(&record.id) {
                    candidates.push(record.clone());
                }
            }
            for record in registry.list() {
                if record.state == JobState::Waiting && !running_jobs.contains_key(&record.id) {
                    candidates.push(record.clone());
                }
            }

            for record in candidates {
                if running_jobs.len() >= args.max_jobs {
                    break;
                }

                let Ok(private_data) = JobPrivateData::load(&directory, &record.id) else {
                    continue;
                };
                let is_torrent = private_data.torrent.is_some() || private_data.magnet.is_some();
                let host = if is_torrent {
                    "torrent".to_string()
                } else {
                    private_data
                        .original_urls
                        .first()
                        .and_then(|url| Url::parse(url).ok())
                        .and_then(|url| url.host_str().map(str::to_owned))
                        .unwrap_or_else(|| "unknown".to_string())
                };

                let count = host_counts.get(&host).copied().unwrap_or(0);
                if count >= args.max_jobs_per_host {
                    continue;
                }

                let job = match JobManifest::load(&record.manifest_path) {
                    Ok(j) => j,
                    Err(_) => continue,
                };
                let job = match job.state {
                    JobState::Active => job,
                    JobState::Waiting => {
                        let activated = com_registro(&directory, |registry| {
                            let Some(current) = registry.get(&job.id) else {
                                return Ok(None);
                            };
                            if current.state != JobState::Waiting {
                                return Ok(None);
                            }
                            let mut job = JobManifest::load(&current.manifest_path)
                                .context("could not load manifest")?;
                            job.transition_to(JobState::Active)?;
                            let path = job.save(&directory)?;
                            registry.update(&job, &path);
                            Ok(Some(job))
                        })
                        .map_err(|err| anyhow!("Could not activate job {}: {err}", job.id))?;
                        let Some(activated) = activated else {
                            continue;
                        };
                        event_broker.emit(EventType::StateChanged {
                            id: activated.id.clone(),
                            from: JobState::Waiting,
                            to: JobState::Active,
                        });
                        activated
                    }
                    _ => continue,
                };

                if !is_ephemeral {
                    saida::info(format!(
                        "Chorus: Scheduling job {} ({})",
                        job.id,
                        saida::redigir_url(job.sources.first().map_or("", String::as_str))
                    ));
                }

                let args_clone = args.clone();
                let job_id = job.id.clone();
                let scheduled_id = job.id.clone();
                let directory_clone = directory.clone();
                let global_limit_clone = Arc::clone(&global_rate_limiter);
                let broker_clone = event_broker.clone();

                join_set.spawn(async move {
                    let result = if is_torrent {
                        torrent_job::executar(
                            private_data,
                            &job,
                            &directory_clone,
                            broker_clone,
                            &args_clone,
                            global_limit_clone,
                        )
                        .await
                    } else {
                        execute_http_task(
                            &args_clone,
                            private_data.original_urls,
                            &job.output,
                            global_limit_clone,
                            &directory_clone,
                            &job_id,
                            broker_clone,
                        )
                        .await
                    };
                    (job_id, result)
                });

                running_jobs.insert(scheduled_id, host.clone());
                *host_counts.entry(host).or_insert(0) += 1;
            }
        }

        if running_jobs.is_empty() && args.rpc_bind.is_none() {
            break;
        }

        let maybe_res =
            match tokio::time::timeout(Duration::from_millis(500), join_set.join_next()).await {
                Ok(Some(res)) => Some(res),
                Ok(None) => break,
                Err(_) => None, // Timeout, loop again to pick up Pause/Cancel changes
            };

        if let Some(res) = maybe_res {
            match res {
                Ok((job_id, result)) => {
                    if job_id.is_empty() {
                        continue;
                    }
                    running_jobs.remove(&job_id);
                    let next_state = match &result {
                        Err((category, _)) => JobState::Failed(category.clone()),
                        Ok(()) => JobState::Complete,
                    };
                    let old_state = com_registro(&directory, |registry| {
                        let record = registry.get(&job_id).context("job not found")?;
                        let mut job = JobManifest::load(&record.manifest_path)
                            .context("could not load manifest")?;
                        let old_state = job.state.clone();
                        if old_state != JobState::Active
                            && job
                                .sources
                                .first()
                                .is_some_and(|s| s.starts_with("torrent:"))
                        {
                            return Ok(None);
                        }
                        if let Err(err) = job.transition_to(next_state.clone()) {
                            had_failures = true;
                            saida::erro(format!("Could not transition job {job_id}: {err}"));
                            return Ok(None);
                        }
                        let path = job.save(&directory)?;
                        registry.update(&job, &path);
                        Ok(Some(old_state))
                    })
                    .map_err(|err| anyhow!("Could not persist result for job {job_id}: {err}"))?;
                    if let Some(old_state) = old_state {
                        if let Err((_, err)) = result {
                            had_failures = true;
                            if is_ephemeral {
                                ephemeral_error = Some(err);
                            } else {
                                saida::erro(format!("Job {} failed: {}", job_id, err));
                            }
                        }
                        if next_state == JobState::Complete {
                            saida::info(format!("Chorus: job {job_id} complete"));
                        }
                        event_broker.emit(EventType::StateChanged {
                            id: job_id,
                            from: old_state,
                            to: next_state,
                        });
                    }
                }
                Err(e) => {
                    had_failures = true;
                    saida::erro(format!("Task panicked: {}", e));
                }
            }
        }
    }

    if is_ephemeral {
        let _ = fs::remove_dir_all(&directory);
        if let Some(err) = ephemeral_error {
            return Err(err);
        }
    }

    if had_failures {
        return Err(anyhow::anyhow!("One or more jobs failed during execution"));
    }

    Ok(())
}

async fn execute_http_task(
    args: &Cli,
    urls: Vec<String>,
    output_path: &Path,
    global_rate_limiter: Arc<RateLimiter>,
    directory: &Path,
    job_id: &str,
    event_broker: EventBroker,
) -> Result<(), (ErrorCategory, anyhow::Error)> {
    match execute_download_inner(
        args,
        urls,
        output_path,
        global_rate_limiter,
        directory,
        job_id,
        event_broker,
    )
    .await
    {
        Ok(()) => Ok(()),
        Err(e) => Err((categorize_error(&e), saida::redigir_erro_http(e))),
    }
}

async fn execute_download_inner(
    args: &Cli,
    urls: Vec<String>,
    output_path: &Path,
    global_rate_limiter: Arc<RateLimiter>,
    directory: &Path,
    job_id: &str,
    event_broker: EventBroker,
) -> Result<()> {
    let rede_protegida = args.rpc_bind.is_some() && !args.allow_private_network;
    let fetcher = HttpFetcher::new(
        timeout_duration(args.connect_timeout_ms),
        timeout_duration(args.read_timeout_ms),
        timeout_duration(args.timeout_ms),
        rede_protegida,
    )?;

    let piece_size = normalize_piece_size(args.piece_size)?;
    let expected_sha256 = args.sha256.as_deref().map(normalize_sha256).transpose()?;

    let part_path = path_with_suffix(output_path, ".part");
    let meta_path = path_with_suffix(output_path, ".part.meta");
    let ranges_path = path_with_suffix(output_path, ".part.ranges");

    let mut last_err = None;

    for (i, url) in urls.iter().enumerate() {
        if i > 0 {
            saida::info(format!(
                "Chorus: Fallback to next source: {}",
                saida::redigir_url(url)
            ));
        }

        let result = async {
            if rede_protegida {
                rede::validar_url(url, true)?;
            }
            let metadata = fetcher.get_metadata(url).await?;
            let size = metadata.content_length;

            if !metadata.length_known || size == 0 {
                let job_rate_limiter = RateLimiter::new(args.limit_rate);
                return baixar_em_fluxo(
                    &fetcher,
                    url,
                    output_path,
                    &global_rate_limiter,
                    &job_rate_limiter,
                    directory,
                    job_id,
                    &event_broker,
                    expected_sha256.as_deref(),
                )
                .await;
            }

            let connections =
                normalize_connections(args.connections, size, metadata.supports_ranges)?;
            let use_ranges = metadata.supports_ranges && connections > 1;
            let if_range = range_validator(&metadata);

            if !metadata.supports_ranges {
                saida::info("Server does not support ranges. Falling back to 1 connection.");
            }

            let resume_key = resume_key(&metadata);

            if i > 0
                && meta_path.exists()
                && let Ok(existing_meta) = fs::read_to_string(&meta_path)
                && existing_meta != resume_key
            {
                return Err(anyhow!(
                    "Fallback source is incompatible with existing partial file"
                ));
            }

            let can_resume = !args.no_resume && if_range.is_some();
            let completed_ranges = prepare_resume(
                &part_path,
                &meta_path,
                &ranges_path,
                &resume_key,
                can_resume,
                size,
            )?;
            let has_resume = !completed_ranges.is_empty();

            if i == 0 {
                saida::info(format!(
                    "File size: {} bytes. Connections: {}",
                    size, connections
                ));
                saida::info(format!("Saving to: {}", output_path.display()));
            }
            if has_resume {
                saida::info(format!(
                    "Resuming {} completed range(s).",
                    completed_ranges.len()
                ));
            }

            let storage = if has_resume {
                ChunkStorage::resume(&part_path, size)?
            } else {
                ChunkStorage::new(&part_path, size)?
            };

            let job_rate_limiter = Arc::new(RateLimiter::new(args.limit_rate));

            let engine = DownloadEngine::new(
                fetcher.clone(),
                url.to_string(),
                size,
                connections,
                storage,
                ranges_path.clone(),
                completed_ranges,
                use_ranges,
                if_range,
                args.retries,
                piece_size,
                global_rate_limiter.clone(),
                job_rate_limiter,
                directory.to_path_buf(),
                job_id.to_string(),
                event_broker.clone(),
            );
            engine.execute().await?;
            drop(engine);

            if let Some(expected) = &expected_sha256 {
                verify_sha256_or_forget_resume(&part_path, &ranges_path, expected)?;
            }

            replace_file(&part_path, output_path)?;
            remove_if_exists(&meta_path)?;
            remove_if_exists(&ranges_path)?;
            remove_if_exists(&path_with_suffix(&ranges_path, ".tmp"))?;

            Ok::<_, anyhow::Error>(())
        }
        .await;

        match result {
            Ok(()) => return Ok(()),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("Job was paused") || msg.contains("Job was cancelled") {
                    if msg.contains("Job was cancelled") && args.remove_partial {
                        let _ = remove_if_exists(&part_path);
                        let _ = remove_if_exists(&meta_path);
                        let _ = remove_if_exists(&ranges_path);
                        let _ = remove_if_exists(&path_with_suffix(&ranges_path, ".tmp"));
                    }
                    return Err(e);
                }

                let category = categorize_error(&e);
                match category {
                    ErrorCategory::Network | ErrorCategory::Internal => {
                        last_err = Some(e);
                        continue;
                    }
                    _ => {
                        return Err(e);
                    }
                }
            }
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow!("No URLs provided or all failed")))
}

#[allow(clippy::too_many_arguments)]
async fn baixar_em_fluxo(
    fetcher: &HttpFetcher,
    url: &str,
    output_path: &Path,
    global_rate_limiter: &RateLimiter,
    job_rate_limiter: &RateLimiter,
    directory: &Path,
    job_id: &str,
    event_broker: &EventBroker,
    expected_sha256: Option<&str>,
) -> Result<()> {
    const MIB: u64 = 1024 * 1024;
    let conferir_estado = || {
        if let Ok(registry) = JobRegistry::load(directory)
            && let Some(record) = registry.get(job_id)
        {
            match record.state {
                JobState::Paused => return Err(anyhow!("Job was paused")),
                JobState::Cancelled => return Err(anyhow!("Job was cancelled")),
                _ => {}
            }
        }
        Ok(())
    };
    conferir_estado()?;

    let part_path = path_with_suffix(output_path, ".part");
    let ranges_path = path_with_suffix(output_path, ".part.ranges");
    remove_if_exists(&part_path)?;
    remove_if_exists(&path_with_suffix(output_path, ".part.meta"))?;
    remove_if_exists(&ranges_path)?;
    remove_if_exists(&path_with_suffix(&ranges_path, ".tmp"))?;

    saida::info("File size: unknown. Connections: 1");
    saida::info(format!("Saving to: {}", output_path.display()));
    let response = fetcher.fetch_full(url).await?;
    let expected_length = content_length_from_headers(response.headers());
    let mut stream = response.bytes_stream();
    let mut file = fs::File::create(&part_path)?;
    let mut bytes_downloaded = 0;
    let emitir_progresso = |bytes_downloaded| {
        event_broker.emit(EventType::Progress {
            id: job_id.to_string(),
            bytes_downloaded,
            bytes_total: None,
        });
    };
    emitir_progresso(0);

    while let Some(chunk) = stream.next().await {
        let mut chunk = chunk.context("network error while reading response body")?;
        while !chunk.is_empty() {
            let ate_proxima_verificacao = MIB - bytes_downloaded % MIB;
            let trecho = chunk.split_to(chunk.len().min(ate_proxima_verificacao as usize));
            let len = trecho.len() as u64;
            global_rate_limiter.wait_for(len).await;
            job_rate_limiter.wait_for(len).await;
            file.write_all(&trecho)?;
            bytes_downloaded += len;
            emitir_progresso(bytes_downloaded);
            if bytes_downloaded.is_multiple_of(MIB) {
                conferir_estado()?;
            }
        }
    }
    conferir_estado()?;
    if let Some(expected_length) = expected_length
        && bytes_downloaded != expected_length
    {
        return Err(anyhow!(
            "network error: received {bytes_downloaded} bytes, expected {expected_length}"
        ));
    }

    file.sync_all()?;
    drop(file);
    if let Some(expected) = expected_sha256 {
        verify_sha256_or_forget_resume(&part_path, &ranges_path, expected)?;
    }
    replace_file(&part_path, output_path)
}

fn categorize_error(err: &anyhow::Error) -> ErrorCategory {
    let msg = err.to_string().to_lowercase();
    if msg.contains("network")
        || msg.contains("status:")
        || msg.contains("server ignored")
        || msg.contains("timeout")
        || msg.contains("reqwest")
    {
        ErrorCategory::Network
    } else if msg.contains("sha-256 mismatch") {
        ErrorCategory::Integrity
    } else if msg.contains("already exists")
        || msg.contains("busy")
        || msg.contains("output")
        || msg.contains("no such file")
    {
        ErrorCategory::Storage
    } else if msg.contains("--") || msg.contains("must be") || msg.contains("invalid") {
        ErrorCategory::InvalidInput
    } else {
        ErrorCategory::Internal
    }
}

fn normalize_connections(requested: usize, size: u64, supports_ranges: bool) -> Result<usize> {
    if requested == 0 {
        return Err(anyhow!("--connections must be at least 1"));
    }
    if !supports_ranges {
        return Ok(1);
    }

    let max_connections = usize::try_from(size).unwrap_or(usize::MAX);
    Ok(requested.min(max_connections))
}

fn normalize_piece_size(piece_size: u64) -> Result<u64> {
    if piece_size == 0 {
        return Err(anyhow!("--piece-size must be at least 1"));
    }
    Ok(piece_size)
}

fn normalize_sha256(value: &str) -> Result<String> {
    let value = value.trim().to_ascii_lowercase();
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("--sha256 must be a 64-character hex digest"));
    }
    Ok(value)
}

fn timeout_duration(ms: u64) -> Option<Duration> {
    (ms != 0).then(|| Duration::from_millis(ms))
}

fn range_validator(metadata: &Metadata) -> Option<String> {
    metadata
        .etag
        .as_deref()
        .filter(|etag| !etag.trim_start().starts_with("W/"))
        .map(ToOwned::to_owned)
        .or_else(|| metadata.last_modified.clone())
}

fn output_path(url: &str, output: Option<&str>) -> PathBuf {
    output
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(default_filename(url)))
}

fn default_filename(url: &str) -> String {
    Url::parse(url)
        .ok()
        .and_then(|u| {
            u.path_segments()
                .and_then(|mut segments| segments.next_back())
                .map(sanitize_filename)
        })
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "downloaded_file".to_string())
}

fn sanitize_filename(name: &str) -> String {
    let mut value: String = name
        .chars()
        .map(|ch| match ch {
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' => '_',
            ch if ch.is_control() => '_',
            ch => ch,
        })
        .collect();
    value.truncate(240);

    let mut value = value.trim_matches([' ', '.']).to_string();
    if value.is_empty() {
        value = "downloaded_file".to_string();
    }

    if is_windows_reserved_name(&value) {
        value.insert(0, '_');
    }

    value
}

fn is_windows_reserved_name(name: &str) -> bool {
    let stem = name
        .split('.')
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase();
    matches!(
        stem.as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    )
}

fn ensure_output_allowed(path: &Path, no_overwrite: bool) -> Result<()> {
    if no_overwrite && path.exists() {
        return Err(anyhow!(
            "Output already exists: {}. Remove it or omit --no-overwrite.",
            path.display()
        ));
    }
    Ok(())
}

fn verify_sha256(path: &Path, expected: &str) -> Result<()> {
    let actual = sha256_file(path)?;
    if actual != expected {
        return Err(anyhow!(
            "SHA-256 mismatch: expected {}, got {}",
            expected,
            actual
        ));
    }
    Ok(())
}

fn verify_sha256_or_forget_resume(
    part_path: &Path,
    ranges_path: &Path,
    expected: &str,
) -> Result<()> {
    if let Err(err) = verify_sha256(part_path, expected) {
        remove_if_exists(ranges_path)?;
        return Err(err);
    }
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut reader = BufReader::new(fs::File::open(path)?);
    let mut hasher = Sha256::new();
    let mut buf = [0; 64 * 1024];

    loop {
        let read = reader.read(&mut buf)?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }

    Ok(hex_lower(&hasher.finalize()))
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn path_with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn resume_key(metadata: &Metadata) -> String {
    format!(
        "size={}\netag={}\nlast_modified={}\n",
        metadata.content_length,
        metadata.etag.as_deref().unwrap_or(""),
        metadata.last_modified.as_deref().unwrap_or("")
    )
}

fn prepare_resume(
    part_path: &Path,
    meta_path: &Path,
    ranges_path: &Path,
    expected_meta: &str,
    can_resume: bool,
    size: u64,
) -> Result<Vec<(u64, u64)>> {
    if !can_resume {
        remove_if_exists(part_path)?;
        remove_if_exists(meta_path)?;
        remove_if_exists(ranges_path)?;
        return Ok(Vec::new());
    }

    let meta_matches = fs::read_to_string(meta_path)
        .map(|meta| meta == expected_meta)
        .unwrap_or(false);

    let part_size_matches = fs::metadata(part_path)
        .map(|metadata| metadata.len() == size)
        .unwrap_or(false);

    if !meta_matches || !part_size_matches {
        remove_if_exists(part_path)?;
        remove_if_exists(ranges_path)?;
        fs::write(meta_path, expected_meta)?;
        return Ok(Vec::new());
    }

    load_ranges(ranges_path, size).or_else(|_| {
        remove_if_exists(ranges_path)?;
        Ok(Vec::new())
    })
}

fn load_ranges(path: &Path, size: u64) -> Result<Vec<(u64, u64)>> {
    let Ok(text) = fs::read_to_string(path) else {
        return Ok(Vec::new());
    };

    let mut ranges = Vec::new();
    for line in text.lines() {
        let Some((start, end)) = line.split_once(' ') else {
            return Err(anyhow!("Invalid range line: {}", line));
        };
        let start = start.parse::<u64>()?;
        let end = end.parse::<u64>()?;
        if start > end || end >= size {
            return Err(anyhow!("Invalid completed range: {} {}", start, end));
        }
        ranges.push((start, end));
    }

    ranges.sort_unstable();
    Ok(ranges)
}

fn replace_file(from: &Path, to: &Path) -> Result<()> {
    if !to.exists() {
        rename_with_retries(from, to)?;
        return Ok(());
    }

    let backup = replacement_backup_path(to);
    rename_with_retries(to, &backup)?;
    if let Err(err) = rename_with_retries(from, to) {
        let _ = rename_with_retries(&backup, to);
        return Err(err.into());
    }

    remove_if_exists(&backup)?;
    Ok(())
}

fn rename_with_retries(from: &Path, to: &Path) -> std::io::Result<()> {
    let mut last_error = None;

    for _ in 0..50 {
        match fs::rename(from, to) {
            Ok(()) => return Ok(()),
            Err(err) if is_transient_file_busy(&err) => {
                last_error = Some(err);
                thread::sleep(Duration::from_millis(100));
            }
            Err(err) => return Err(err),
        }
    }

    Err(last_error.expect("retry loop records transient errors"))
}

fn is_transient_file_busy(err: &std::io::Error) -> bool {
    err.raw_os_error() == Some(32)
}

fn replacement_backup_path(path: &Path) -> PathBuf {
    let mut attempt = 0;
    loop {
        let candidate = path_with_suffix(
            path,
            &format!(".chorus-replace-{}-{}.bak", std::process::id(), attempt),
        );
        if !candidate.exists() {
            return candidate;
        }
        attempt += 1;
    }
}

#[cfg(test)]
fn read_file(path: &Path) -> Result<Vec<u8>> {
    Ok(fs::read(path)?)
}

#[cfg(test)]
fn write_file(path: &Path, data: &[u8]) -> Result<()> {
    fs::write(path, data)?;
    Ok(())
}

fn remove_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        default_filename, ensure_output_allowed, load_ranges, normalize_connections,
        normalize_piece_size, normalize_sha256, output_path, path_with_suffix, prepare_resume,
        range_validator, read_file, replace_file, sha256_file, timeout_duration, verify_sha256,
        verify_sha256_or_forget_resume, write_file,
    };
    use crate::http::Metadata;
    use std::path::{Path, PathBuf};

    #[test]
    fn normalizes_connection_count() {
        assert_eq!(normalize_connections(4, 10, true).unwrap(), 4);
        assert_eq!(normalize_connections(99, 10, true).unwrap(), 10);
        assert_eq!(normalize_connections(4, 10, false).unwrap(), 1);
        assert!(normalize_connections(0, 10, true).is_err());
    }

    #[test]
    fn normalizes_piece_size() {
        assert_eq!(normalize_piece_size(1).unwrap(), 1);
        assert!(normalize_piece_size(0).is_err());
    }

    #[test]
    fn normalizes_sha256() {
        let digest = "BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD";
        assert_eq!(
            normalize_sha256(digest).unwrap(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert!(normalize_sha256("abc").is_err());
        assert!(normalize_sha256(&"z".repeat(64)).is_err());
    }

    #[test]
    fn converts_zero_timeout_to_none() {
        assert_eq!(timeout_duration(0), None);
        assert_eq!(
            timeout_duration(250).unwrap(),
            std::time::Duration::from_millis(250)
        );
    }

    #[test]
    fn chooses_safe_range_validator() {
        let metadata = Metadata {
            content_length: 1,
            length_known: true,
            supports_ranges: true,
            etag: Some("\"strong\"".to_string()),
            last_modified: Some("Sun, 05 Jul 2026 00:00:00 GMT".to_string()),
        };
        assert_eq!(range_validator(&metadata).unwrap(), "\"strong\"");

        let metadata = Metadata {
            content_length: 1,
            length_known: true,
            supports_ranges: true,
            etag: Some("W/\"weak\"".to_string()),
            last_modified: Some("Sun, 05 Jul 2026 00:00:00 GMT".to_string()),
        };
        assert_eq!(
            range_validator(&metadata).unwrap(),
            "Sun, 05 Jul 2026 00:00:00 GMT"
        );

        let metadata = Metadata {
            content_length: 1,
            length_known: true,
            supports_ranges: true,
            etag: Some("W/\"weak\"".to_string()),
            last_modified: None,
        };
        assert!(range_validator(&metadata).is_none());
    }

    #[test]
    fn appends_suffix_to_whole_path() {
        assert_eq!(
            path_with_suffix(Path::new("file.tar.gz"), ".part"),
            PathBuf::from("file.tar.gz.part")
        );
    }

    #[test]
    fn derives_safe_default_filename() {
        assert_eq!(
            default_filename("https://example.com/files/a:b*.zip"),
            "a_b_.zip"
        );
        assert_eq!(
            default_filename("https://example.com/files/CON.txt"),
            "_CON.txt"
        );
        assert_eq!(default_filename("https://example.com/"), "downloaded_file");
    }

    #[test]
    fn explicit_output_path_is_preserved() {
        assert_eq!(
            output_path("https://example.com/files/a:b.zip", Some("custom:name.bin")),
            PathBuf::from("custom:name.bin")
        );
    }

    #[test]
    fn no_overwrite_rejects_existing_output() {
        let path = std::env::temp_dir().join(format!(
            "chorus-output-{}-{}.tmp",
            std::process::id(),
            "exists"
        ));
        let _ = std::fs::remove_file(&path);
        write_file(&path, b"old").unwrap();

        assert!(ensure_output_allowed(&path, true).is_err());
        assert!(ensure_output_allowed(&path, false).is_ok());

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn rejects_invalid_ranges() {
        let path = std::env::temp_dir().join(format!(
            "chorus-ranges-{}-{}.tmp",
            std::process::id(),
            "invalid"
        ));
        std::fs::write(&path, "0 10\n").unwrap();
        let result = load_ranges(&path, 10);
        let _ = std::fs::remove_file(path);
        assert!(result.is_err());
    }

    #[test]
    fn resume_restarts_when_part_size_does_not_match() {
        let base = std::env::temp_dir().join(format!(
            "chorus-resume-{}-{}",
            std::process::id(),
            "wrong-size"
        ));
        let part = path_with_suffix(&base, ".part");
        let meta = path_with_suffix(&base, ".meta");
        let ranges = path_with_suffix(&base, ".ranges");
        let _ = std::fs::remove_file(&part);
        let _ = std::fs::remove_file(&meta);
        let _ = std::fs::remove_file(&ranges);

        write_file(&part, b"too small").unwrap();
        write_file(&meta, b"same-meta").unwrap();
        write_file(&ranges, b"0 8\n").unwrap();

        let completed = prepare_resume(&part, &meta, &ranges, "same-meta", true, 16).unwrap();
        assert!(completed.is_empty());
        assert!(!part.exists());
        assert!(!ranges.exists());

        let _ = std::fs::remove_file(meta);
    }

    #[test]
    fn no_resume_cleans_partial_state() {
        let base = std::env::temp_dir().join(format!(
            "chorus-resume-{}-{}",
            std::process::id(),
            "disabled"
        ));
        let part = path_with_suffix(&base, ".part");
        let meta = path_with_suffix(&base, ".meta");
        let ranges = path_with_suffix(&base, ".ranges");
        let _ = std::fs::remove_file(&part);
        let _ = std::fs::remove_file(&meta);
        let _ = std::fs::remove_file(&ranges);

        write_file(&part, b"partial").unwrap();
        write_file(&meta, b"meta").unwrap();
        write_file(&ranges, b"0 6\n").unwrap();

        let completed = prepare_resume(&part, &meta, &ranges, "meta", false, 7).unwrap();
        assert!(completed.is_empty());
        assert!(!part.exists());
        assert!(!meta.exists());
        assert!(!ranges.exists());
    }

    #[test]
    fn verifies_sha256_digest() {
        let path = std::env::temp_dir().join(format!(
            "chorus-sha256-{}-{}.tmp",
            std::process::id(),
            "abc"
        ));
        let _ = std::fs::remove_file(&path);
        write_file(&path, b"abc").unwrap();

        let expected = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        assert_eq!(sha256_file(&path).unwrap(), expected);
        assert!(verify_sha256(&path, expected).is_ok());
        assert!(verify_sha256(&path, &"0".repeat(64)).is_err());

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn checksum_mismatch_forgets_completed_ranges() {
        let base = std::env::temp_dir().join(format!(
            "chorus-sha256-{}-{}",
            std::process::id(),
            "forget-ranges"
        ));
        let part = path_with_suffix(&base, ".part");
        let ranges = path_with_suffix(&base, ".ranges");
        let _ = std::fs::remove_file(&part);
        let _ = std::fs::remove_file(&ranges);

        write_file(&part, b"abc").unwrap();
        write_file(&ranges, b"0 2\n").unwrap();

        assert!(verify_sha256_or_forget_resume(&part, &ranges, &"0".repeat(64)).is_err());
        assert!(part.exists());
        assert!(!ranges.exists());

        let _ = std::fs::remove_file(part);
    }

    #[test]
    fn replace_file_restores_existing_output_when_new_file_is_missing() {
        let base = std::env::temp_dir().join(format!(
            "chorus-replace-{}-{}",
            std::process::id(),
            "missing"
        ));
        let old = path_with_suffix(&base, ".old");
        let missing = path_with_suffix(&base, ".missing");
        let _ = std::fs::remove_file(&old);
        let _ = std::fs::remove_file(&missing);

        write_file(&old, b"old").unwrap();
        assert!(replace_file(&missing, &old).is_err());
        assert_eq!(read_file(&old).unwrap(), b"old");

        let _ = std::fs::remove_file(old);
    }

    #[test]
    fn replace_file_overwrites_existing_output() {
        let base = std::env::temp_dir().join(format!(
            "chorus-replace-{}-{}",
            std::process::id(),
            "success"
        ));
        let old = path_with_suffix(&base, ".old");
        let new = path_with_suffix(&base, ".new");
        let _ = std::fs::remove_file(&old);
        let _ = std::fs::remove_file(&new);

        write_file(&old, b"old").unwrap();
        write_file(&new, b"new").unwrap();
        replace_file(&new, &old).unwrap();
        assert_eq!(read_file(&old).unwrap(), b"new");
        assert!(!new.exists());

        let _ = std::fs::remove_file(old);
    }
}
