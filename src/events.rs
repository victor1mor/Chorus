use crate::job::JobState;
use crate::saida;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{self, StdoutLock, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::broadcast;

static SEQUENCE: AtomicU64 = AtomicU64::new(1);
const PROGRESS_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(tag = "type", content = "data")]
pub enum EventType {
    JobCreated {
        id: String,
    },
    StateChanged {
        id: String,
        from: JobState,
        to: JobState,
    },
    Progress {
        id: String,
        bytes_downloaded: u64,
        bytes_total: Option<u64>,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Event {
    pub v: u8,
    pub seq: u64,
    #[serde(flatten)]
    pub event_type: EventType,
}

#[derive(Clone)]
pub struct EventBroker {
    tx: broadcast::Sender<Event>,
    last_progress: Arc<Mutex<HashMap<String, Instant>>>,
}

impl EventBroker {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self {
            tx,
            last_progress: Arc::default(),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.tx.subscribe()
    }

    pub fn emit(&self, event_type: EventType) {
        let stdout = saida::modo_eventos().then(|| io::stdout().lock());
        let seq = SEQUENCE.fetch_add(1, Ordering::SeqCst);
        let event = Event {
            v: 1,
            seq,
            event_type,
        };
        if let Some(stdout) = stdout {
            self.write_stdout(&event, stdout);
        }
        let _ = self.tx.send(event);
    }

    fn write_stdout(&self, event: &Event, mut stdout: StdoutLock<'_>) {
        let mut last_progress = self.last_progress.lock().unwrap();
        if let EventType::Progress { id, .. } = &event.event_type
            && last_progress
                .get(id)
                .is_some_and(|last| last.elapsed() < PROGRESS_INTERVAL)
        {
            return;
        }

        if let Ok(json) = serde_json::to_string(event)
            && writeln!(stdout, "{json}")
                .and_then(|()| stdout.flush())
                .is_ok()
            && let EventType::Progress { id, .. } = &event.event_type
        {
            // ponytail: scan recent jobs on emission; use ordered expiry if concurrency grows.
            last_progress.retain(|_, last| last.elapsed() < PROGRESS_INTERVAL);
            last_progress.insert(id.clone(), Instant::now());
        }
    }
}
