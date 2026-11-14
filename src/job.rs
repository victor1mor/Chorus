use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use url::Url;

static NEXT_ID: AtomicU64 = AtomicU64::new(0);
// ponytail: one mutex serializes all job directories in this process. If that
// becomes a bottleneck, use a mutex per directory alongside the OS file lock.
static TRAVA_REGISTRO: std::sync::Mutex<()> = std::sync::Mutex::new(());

pub fn com_registro<T>(dir: &Path, f: impl FnOnce(&mut JobRegistry) -> Result<T>) -> Result<T> {
    let _trava = TRAVA_REGISTRO
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    fs::create_dir_all(dir)?;
    let registry_lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(dir.join("registry.lock"))
        .context("could not lock job registry")?;
    registry_lock
        .lock()
        .context("could not lock job registry")?;
    let mut registry = JobRegistry::load(dir)?;
    let result = f(&mut registry)?;
    registry.save()?;
    Ok(result)
}

fn save_json(path: PathBuf, value: &impl Serialize) -> Result<PathBuf> {
    let sequence = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let temp = path.with_extension(format!("json.{}.{sequence}.tmp", std::process::id()));
    let mut file = File::create_new(&temp)?;
    let result = (|| -> Result<()> {
        file.write_all(&serde_json::to_vec_pretty(value)?)?;
        file.sync_all()?;
        drop(file);
        // Keep the previous file readable until the new one is ready to replace it.
        fs::rename(&temp, &path)?;
        Ok(())
    })();

    if let Err(err) = result {
        let _ = fs::remove_file(&temp);
        return Err(err);
    }
    Ok(path)
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[allow(dead_code)]
pub enum ErrorCategory {
    InvalidInput,
    Network,
    Integrity,
    Storage,
    Internal,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum JobState {
    Waiting,
    Active,
    Seeding,
    Paused,
    Cancelled,
    Complete,
    Failed(ErrorCategory),
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct JobManifest {
    pub version: u8,
    pub id: String,
    pub state: JobState,
    pub sources: Vec<String>,
    pub output: PathBuf,
}

impl JobManifest {
    pub fn new(urls: &[String], output: &Path) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let sequence = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        Self {
            version: 2,
            id: format!("{nanos:016x}-{:08x}-{sequence:016x}", std::process::id()),
            state: JobState::Waiting,
            sources: urls.iter().map(|u| redact_source(u)).collect(),
            output: output.to_path_buf(),
        }
    }

    pub fn transition_to(&mut self, next: JobState) -> Result<()> {
        let valid = matches!(
            (&self.state, &next),
            (JobState::Waiting, JobState::Active)
                | (JobState::Waiting, JobState::Cancelled)
                | (JobState::Waiting, JobState::Failed(_))
                | (JobState::Active, JobState::Complete)
                | (JobState::Active, JobState::Seeding)
                | (JobState::Active, JobState::Failed(_))
                | (JobState::Active, JobState::Waiting)
                | (JobState::Active, JobState::Paused)
                | (JobState::Active, JobState::Cancelled)
                | (JobState::Seeding, JobState::Complete)
                | (JobState::Seeding, JobState::Paused)
                | (JobState::Seeding, JobState::Cancelled)
                | (JobState::Seeding, JobState::Failed(_))
                | (JobState::Paused, JobState::Waiting)
                | (JobState::Paused, JobState::Cancelled)
                | (JobState::Failed(_), JobState::Waiting)
        );
        if valid {
            self.state = next;
            Ok(())
        } else {
            anyhow::bail!(
                "Invalid state transition from {:?} to {:?}",
                self.state,
                next
            )
        }
    }

    pub fn save(&self, directory: &Path) -> Result<PathBuf> {
        fs::create_dir_all(directory)?;
        let path = directory.join(format!("{}.chorus.json", self.id));
        save_json(path, self)
    }

    #[allow(dead_code)]
    pub fn load(path: &Path) -> Result<Self> {
        let try_load = |p: &Path| -> Result<Self> {
            let data = fs::read(p)?;
            let mut val: serde_json::Value = serde_json::from_slice(&data)?;
            if val.get("version").and_then(|v| v.as_u64()) == Some(1) {
                if let Some(source) = val.get("source").cloned() {
                    val.as_object_mut().unwrap().insert(
                        "sources".to_string(),
                        serde_json::Value::Array(vec![source]),
                    );
                    val.as_object_mut().unwrap().remove("source");
                }
                val.as_object_mut()
                    .unwrap()
                    .insert("version".to_string(), serde_json::Value::Number(2.into()));
            }
            let manifest: Self = serde_json::from_value(val)?;
            if manifest.version != 2 {
                anyhow::bail!("Unknown manifest version: {}", manifest.version);
            }
            Ok(manifest)
        };

        let temp = path.with_extension("chorus.json.tmp");
        let bak = path.with_extension("chorus.json.bak");

        let candidates = [path, &temp, &bak];
        let mut loaded = None;
        let mut first_err = None;

        for p in candidates {
            if p.exists() {
                match try_load(p) {
                    Ok(m) => {
                        loaded = Some((m, p.to_path_buf()));
                        break;
                    }
                    Err(e) => {
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                    }
                }
            }
        }

        match loaded {
            Some((manifest, valid_path)) => {
                if valid_path != path {
                    let _ = fs::rename(&valid_path, path);
                }
                if valid_path != temp && temp.exists() {
                    let _ = fs::remove_file(&temp);
                }
                if valid_path != bak && bak.exists() {
                    let _ = fs::remove_file(&bak);
                }
                Ok(manifest)
            }
            None => match first_err {
                Some(e) => Err(e),
                None => Err(anyhow::anyhow!("Manifest not found")),
            },
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct JobPrivateData {
    pub version: u8,
    pub original_urls: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub torrent: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub magnet: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peers: Option<Vec<SocketAddr>>,
}

impl JobPrivateData {
    pub fn new(urls: &[String]) -> Self {
        Self {
            version: 2,
            original_urls: urls.to_vec(),
            torrent: None,
            magnet: None,
            peers: None,
        }
    }

    pub fn new_torrent(torrent: PathBuf, peers: Vec<SocketAddr>) -> Self {
        Self {
            torrent: Some(torrent),
            peers: Some(peers),
            ..Self::new(&[])
        }
    }

    pub fn save(&self, directory: &Path, id: &str) -> Result<PathBuf> {
        fs::create_dir_all(directory)?;
        let path = directory.join(format!("{}.chorus.private.json", id));
        save_json(path, self)
    }

    pub fn load(directory: &Path, id: &str) -> Result<Self> {
        let path = directory.join(format!("{}.chorus.private.json", id));
        let try_load = |p: &Path| -> Result<Self> {
            let data = fs::read(p)?;
            let mut val: serde_json::Value = serde_json::from_slice(&data)?;
            if val.get("version").and_then(|v| v.as_u64()) == Some(1) {
                if let Some(original_url) = val.get("original_url").cloned() {
                    val.as_object_mut().unwrap().insert(
                        "original_urls".to_string(),
                        serde_json::Value::Array(vec![original_url]),
                    );
                    val.as_object_mut().unwrap().remove("original_url");
                }
                val.as_object_mut()
                    .unwrap()
                    .insert("version".to_string(), serde_json::Value::Number(2.into()));
            }
            let data: Self = serde_json::from_value(val)?;
            if data.version != 2 {
                anyhow::bail!("Unknown private data version: {}", data.version);
            }
            Ok(data)
        };

        let temp = path.with_extension("chorus.private.json.tmp");
        let bak = path.with_extension("chorus.private.json.bak");

        let candidates = [&path, &temp, &bak];
        let mut loaded = None;
        let mut first_err = None;

        for p in candidates {
            if p.exists() {
                match try_load(p) {
                    Ok(m) => {
                        loaded = Some((m, p.to_path_buf()));
                        break;
                    }
                    Err(e) => {
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                    }
                }
            }
        }

        match loaded {
            Some((data, valid_path)) => {
                if valid_path != path {
                    let _ = fs::rename(&valid_path, &path);
                }
                if valid_path != temp && temp.exists() {
                    let _ = fs::remove_file(&temp);
                }
                if valid_path != bak && bak.exists() {
                    let _ = fs::remove_file(&bak);
                }
                Ok(data)
            }
            None => match first_err {
                Some(e) => Err(e),
                None => Err(anyhow::anyhow!("Private data not found for job {}", id)),
            },
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct JobRecord {
    pub id: String,
    pub state: JobState,
    pub manifest_path: PathBuf,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct JobRegistryData {
    pub version: u8,
    pub jobs: Vec<JobRecord>,
}

#[derive(Clone, Debug)]
pub struct JobRegistry {
    pub dir: PathBuf,
    pub data: JobRegistryData,
}

impl JobRegistry {
    pub fn load(dir: &Path) -> Result<Self> {
        let mut jobs = Vec::new();

        match fs::read_dir(dir) {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry?;
                    let path = entry.path();
                    if !path.is_file() {
                        continue;
                    }
                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                        if !name.ends_with(".chorus.json") || name == "registry.chorus.json" {
                            continue;
                        }
                        match JobManifest::load(&path) {
                            Ok(manifest) => {
                                if jobs.iter().any(|j: &JobRecord| j.id == manifest.id) {
                                    anyhow::bail!("Duplicate job ID found: {}", manifest.id);
                                }
                                jobs.push(JobRecord {
                                    id: manifest.id,
                                    state: manifest.state,
                                    manifest_path: path.clone(),
                                });
                            }
                            Err(e) => {
                                eprintln!(
                                    "Warning: ignoring invalid manifest {}: {}",
                                    path.display(),
                                    e
                                );
                            }
                        }
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }

        jobs.sort_by(|a, b| a.id.cmp(&b.id));

        Ok(Self {
            dir: dir.to_path_buf(),
            data: JobRegistryData { version: 1, jobs },
        })
    }

    fn save(&self) -> Result<PathBuf> {
        fs::create_dir_all(&self.dir)?;
        let path = self.dir.join("registry.chorus.json");
        save_json(path, &self.data)
    }

    #[allow(dead_code)]
    pub fn get(&self, id: &str) -> Option<&JobRecord> {
        self.data.jobs.iter().find(|j| j.id == id)
    }

    #[allow(dead_code)]
    pub fn list(&self) -> &[JobRecord] {
        &self.data.jobs
    }

    pub fn update(&mut self, manifest: &JobManifest, path: &Path) {
        let record = JobRecord {
            id: manifest.id.clone(),
            state: manifest.state.clone(),
            manifest_path: path.to_path_buf(),
        };
        if let Some(pos) = self.data.jobs.iter().position(|j| j.id == manifest.id) {
            self.data.jobs[pos] = record;
        } else {
            self.data.jobs.push(record);
            self.data.jobs.sort_by(|a, b| a.id.cmp(&b.id));
        }
    }
}

pub(crate) fn redact_source(value: &str) -> String {
    let Ok(mut url) = Url::parse(value) else {
        return "<invalid-url>".to_string();
    };
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.set_fragment(None);
    url.to_string()
}

#[cfg(test)]
mod tests {
    use super::{JobManifest, JobRegistry, JobRegistryData, JobState, com_registro};
    use std::sync::mpsc::{self, RecvTimeoutError};
    use std::time::Duration;

    #[test]
    fn registry_write_waits_for_file_lock() {
        let directory =
            std::env::temp_dir().join(format!("chorus-registry-lock-{}", std::process::id()));
        std::fs::create_dir(&directory).unwrap();
        let lock = std::fs::File::create_new(directory.join("registry.lock")).unwrap();
        lock.lock().unwrap();

        let (started, ready) = mpsc::channel();
        let (finished, done) = mpsc::channel();
        let result_while_locked = std::thread::scope(|scope| {
            let directory = &directory;
            let writer = scope.spawn(move || {
                started.send(()).unwrap();
                let result = com_registro(directory, |registry| {
                    let job = JobManifest::new(&[], std::path::Path::new("out.bin"));
                    let path = job.save(directory)?;
                    registry.update(&job, &path);
                    Ok(())
                });
                finished.send(()).unwrap();
                result
            });

            ready.recv().unwrap();
            let result_while_locked = done.recv_timeout(Duration::from_millis(300));
            drop(lock);
            writer.join().unwrap().unwrap();
            result_while_locked
        });

        assert_eq!(result_while_locked, Err(RecvTimeoutError::Timeout));
        assert_eq!(JobRegistry::load(&directory).unwrap().list().len(), 1);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn concurrent_registry_writes_keep_all_200_jobs() {
        let directory =
            std::env::temp_dir().join(format!("chorus-registry-threads-{}", std::process::id()));
        let start = std::sync::Barrier::new(8);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    start.wait();
                    for _ in 0..25 {
                        com_registro(&directory, |registry| {
                            let job = JobManifest::new(&[], std::path::Path::new("out.bin"));
                            let path = job.save(&directory)?;
                            registry.update(&job, &path);
                            Ok(())
                        })
                        .unwrap();
                    }
                });
            }
        });

        let saved = std::fs::read(directory.join("registry.chorus.json")).unwrap();
        let saved: JobRegistryData = serde_json::from_slice(&saved).unwrap();
        let reloaded = JobRegistry::load(&directory).unwrap();
        assert_eq!(reloaded.list().len(), 200);
        assert_eq!(saved.jobs, reloaded.list());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn persists_redacted_lifecycle_manifest() {
        let directory = std::env::temp_dir().join(format!("chorus-job-{}", std::process::id()));
        let mut manifest = JobManifest::new(
            &["https://user:secret@example.test/file?token=secret".to_string()],
            std::path::Path::new("output.bin"),
        );
        let path = manifest.save(&directory).unwrap();
        assert_eq!(manifest.sources[0], "https://example.test/file");
        assert_eq!(JobManifest::load(&path).unwrap().state, JobState::Waiting);
        manifest.transition_to(JobState::Active).unwrap();
        manifest.transition_to(JobState::Complete).unwrap();
        manifest.save(&directory).unwrap();
        assert_eq!(JobManifest::load(&path).unwrap().state, JobState::Complete);
        let _ = std::fs::remove_dir_all(directory);
    }

    #[test]
    fn failed_state_does_not_leak_secrets_in_json() {
        let mut manifest = JobManifest::new(
            &["https://user:secret@example.test/file?token=secret#frag".to_string()],
            std::path::Path::new("output.bin"),
        );
        manifest
            .transition_to(JobState::Failed(super::ErrorCategory::Network))
            .unwrap();

        let json = serde_json::to_string(&manifest).unwrap();
        assert!(!json.contains("user"));
        assert!(!json.contains("secret"));
        assert!(!json.contains("token"));
        assert!(!json.contains("frag"));

        let loaded: JobManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(
            loaded.state,
            JobState::Failed(super::ErrorCategory::Network)
        );
    }
    #[test]
    fn load_recovers_from_interruptions() {
        let directory = std::env::temp_dir().join(format!("chorus-recover-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();

        let path = directory.join("test.chorus.json");
        let temp = path.with_extension("chorus.json.tmp");
        let bak = path.with_extension("chorus.json.bak");

        let mut manifest = JobManifest::new(
            &["http://example.test".to_string()],
            std::path::Path::new("out.bin"),
        );
        manifest.id = "test".to_string();

        let write_json = |p: &std::path::Path, content: &str| std::fs::write(p, content).unwrap();

        // 1. Valid path
        write_json(&path, &serde_json::to_string(&manifest).unwrap());
        assert!(JobManifest::load(&path).is_ok());

        // 2. Invalid path, valid temp
        write_json(&path, "invalid json");
        write_json(&temp, &serde_json::to_string(&manifest).unwrap());
        assert!(JobManifest::load(&path).is_ok());

        // 3. Missing path, missing temp, valid bak
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(&temp).ok();
        write_json(&bak, &serde_json::to_string(&manifest).unwrap());
        assert!(JobManifest::load(&path).is_ok());

        // 4. Unknown version returns explicit error
        let mut m2 = manifest.clone();
        m2.version = 3;
        write_json(&path, &serde_json::to_string(&m2).unwrap());
        std::fs::remove_file(&bak).ok(); // Ensure fallback doesn't succeed
        let err = JobManifest::load(&path).unwrap_err();
        assert!(err.to_string().contains("Unknown manifest version"));

        let _ = std::fs::remove_dir_all(directory);
    }

    #[test]
    fn save_interruption_scenarios() {
        let directory =
            std::env::temp_dir().join(format!("chorus-interrupt-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();

        let path = directory.join("test-interrupt.chorus.json");
        let temp = path.with_extension("chorus.json.tmp");
        let bak = path.with_extension("chorus.json.bak");

        let mut manifest_v1 = JobManifest::new(
            &["http://example.test".to_string()],
            std::path::Path::new("out.bin"),
        );
        manifest_v1.id = "test-interrupt".to_string();
        manifest_v1.transition_to(JobState::Active).unwrap();
        let json_v1 = serde_json::to_string(&manifest_v1).unwrap();

        let mut manifest_v2 = manifest_v1.clone();
        manifest_v2.transition_to(JobState::Complete).unwrap();
        let json_v2 = serde_json::to_string(&manifest_v2).unwrap();

        let write_json = |p: &std::path::Path, content: &str| std::fs::write(p, content).unwrap();

        // Scenario 1: Crash during write to temp (temp is truncated, path is v1)
        write_json(&path, &json_v1);
        write_json(&temp, &json_v2[..json_v2.len() / 2]); // Truncated v2
        let loaded = JobManifest::load(&path).unwrap();
        assert_eq!(loaded.state, JobState::Active); // Recovered v1
        assert!(!temp.exists()); // Temp was cleaned up

        // Scenario 2: Crash after temp is synced, before path -> backup rename (temp is v2, path is v1)
        write_json(&path, &json_v1);
        write_json(&temp, &json_v2);
        let loaded = JobManifest::load(&path).unwrap();
        assert_eq!(loaded.state, JobState::Active); // Path is preferred, so v1 is loaded
        assert!(!temp.exists()); // Temp was cleaned up

        // Scenario 3: Crash after path -> backup, before temp -> path (path missing, bak is v1, temp is v2)
        std::fs::remove_file(&path).ok();
        write_json(&bak, &json_v1);
        write_json(&temp, &json_v2);
        let loaded = JobManifest::load(&path).unwrap();
        assert_eq!(loaded.state, JobState::Complete); // Temp was preferred and is valid, so v2 is loaded
        assert!(path.exists()); // Temp was renamed to path
        assert!(!bak.exists()); // Backup was cleaned up
        assert!(!temp.exists());

        // Scenario 4: Crash after temp -> path, before backup deletion (path is v2, bak is v1)
        write_json(&path, &json_v2);
        write_json(&bak, &json_v1);
        let loaded = JobManifest::load(&path).unwrap();
        assert_eq!(loaded.state, JobState::Complete); // Path is v2
        assert!(!bak.exists()); // Backup was cleaned up

        let _ = std::fs::remove_dir_all(directory);
    }

    #[test]
    fn registry_reconstructs_from_valid_manifests() {
        let directory =
            std::env::temp_dir().join(format!("chorus-registry-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();

        let mut m1 = super::JobManifest::new(
            &["http://test.com/1".to_string()],
            std::path::Path::new("out1"),
        );
        m1.id = "job-1".to_string();
        let p1 = m1.save(&directory).unwrap();

        let mut m2 = super::JobManifest::new(
            &["http://test.com/2".to_string()],
            std::path::Path::new("out2"),
        );
        m2.id = "job-2".to_string();
        m2.transition_to(super::JobState::Active).unwrap();
        m2.transition_to(super::JobState::Complete).unwrap();
        let p2 = m2.save(&directory).unwrap();

        // Write an invalid manifest
        std::fs::write(directory.join("job-3.chorus.json"), "invalid json").unwrap();

        let registry = super::JobRegistry::load(&directory).unwrap();
        let jobs = registry.list();
        assert_eq!(jobs.len(), 2);

        // Stable order by ID
        assert_eq!(jobs[0].id, "job-1");
        assert_eq!(jobs[0].state, super::JobState::Waiting);
        assert_eq!(jobs[0].manifest_path, p1);

        assert_eq!(jobs[1].id, "job-2");
        assert_eq!(jobs[1].state, super::JobState::Complete);
        assert_eq!(jobs[1].manifest_path, p2);

        let _ = std::fs::remove_dir_all(directory);
    }

    #[test]
    fn registry_rejects_duplicate_ids() {
        let directory =
            std::env::temp_dir().join(format!("chorus-registry-dup-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();

        let mut m1 = super::JobManifest::new(
            &["http://test.com/1".to_string()],
            std::path::Path::new("out1"),
        );
        m1.id = "dup-job".to_string();
        m1.save(&directory).unwrap();

        // Create a different file with the SAME id inside the manifest
        let dup_path = directory.join("dup-job-2.chorus.json");
        std::fs::write(&dup_path, serde_json::to_string(&m1).unwrap()).unwrap();

        let err = super::JobRegistry::load(&directory).unwrap_err();
        assert!(err.to_string().contains("Duplicate job ID found"));

        let _ = std::fs::remove_dir_all(directory);
    }

    #[test]
    fn registry_saves_and_updates_durably() {
        let directory =
            std::env::temp_dir().join(format!("chorus-registry-save-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();

        let mut registry = super::JobRegistry::load(&directory).unwrap();

        let mut m1 = super::JobManifest::new(
            &["http://test.com/1".to_string()],
            std::path::Path::new("out1"),
        );
        m1.id = "job-1".to_string();
        let p1 = m1.save(&directory).unwrap();

        registry.update(&m1, &p1);
        let reg_path = registry.save().unwrap();
        assert!(reg_path.ends_with("registry.chorus.json"));

        let reg_data = std::fs::read_to_string(&reg_path).unwrap();
        assert!(reg_data.contains("job-1"));
        assert!(reg_data.contains("Waiting"));

        // Transition
        m1.transition_to(super::JobState::Active).unwrap();
        m1.transition_to(super::JobState::Complete).unwrap();
        m1.save(&directory).unwrap();
        registry.update(&m1, &p1);
        registry.save().unwrap();

        let reg_data = std::fs::read_to_string(&reg_path).unwrap();
        assert!(reg_data.contains("Complete"));

        // Test reopening
        let registry2 = super::JobRegistry::load(&directory).unwrap();
        assert_eq!(registry2.list().len(), 1);
        assert_eq!(
            registry2.get("job-1").unwrap().state,
            super::JobState::Complete
        );

        let _ = std::fs::remove_dir_all(directory);
    }
}
