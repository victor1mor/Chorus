use crate::events::{EventBroker, EventType};
use crate::http::HttpFetcher;
use crate::job::{JobRegistry, JobState};
use crate::saida;
use crate::storage::ChunkStorage;
use crate::throttle::RateLimiter;
use anyhow::{Result, anyhow};
use futures::StreamExt;
use std::collections::VecDeque;
use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::task::JoinSet;
use tokio::time::{Duration, sleep};

pub struct DownloadEngine {
    fetcher: Arc<HttpFetcher>,
    storage: Arc<ChunkStorage>,
    url: String,
    total_size: u64,
    connections: usize,
    resume: Arc<ResumeState>,
    use_ranges: bool,
    if_range: Option<String>,
    retries: usize,
    piece_size: u64,
    global_rate_limiter: Arc<RateLimiter>,
    job_rate_limiter: Arc<RateLimiter>,
    directory: PathBuf,
    job_id: String,
    event_broker: EventBroker,
}

impl DownloadEngine {
    #[expect(
        clippy::too_many_arguments,
        reason = "explicit wiring is smaller than a builder"
    )]
    pub fn new(
        fetcher: HttpFetcher,
        url: String,
        total_size: u64,
        connections: usize,
        storage: ChunkStorage,
        ranges_path: PathBuf,
        completed_ranges: Vec<(u64, u64)>,
        use_ranges: bool,
        if_range: Option<String>,
        retries: usize,
        piece_size: u64,
        global_rate_limiter: Arc<RateLimiter>,
        job_rate_limiter: Arc<RateLimiter>,
        directory: PathBuf,
        job_id: String,
        event_broker: EventBroker,
    ) -> Self {
        Self {
            fetcher: Arc::new(fetcher),
            storage: Arc::new(storage),
            url,
            total_size,
            connections,
            resume: Arc::new(ResumeState::new(ranges_path, completed_ranges)),
            use_ranges,
            if_range,
            retries,
            piece_size,
            global_rate_limiter,
            job_rate_limiter,
            directory,
            job_id,
            event_broker,
        }
    }

    pub async fn execute(&self) -> Result<()> {
        let mut completed_bytes = 0;
        let mut queue = VecDeque::new();
        for (start, end) in self.pieces() {
            if self.resume.is_complete(start, end) {
                completed_bytes += end - start + 1;
            } else {
                queue.push_back((start, end));
            }
        }

        let downloaded = Arc::new(AtomicU64::new(completed_bytes));
        let queue = Arc::new(Mutex::new(queue));
        let mut join_set = JoinSet::new();

        for _ in 0..self.connections {
            let fetcher = Arc::clone(&self.fetcher);
            let storage = Arc::clone(&self.storage);
            let resume = Arc::clone(&self.resume);
            let queue = Arc::clone(&queue);
            let downloaded = Arc::clone(&downloaded);
            let url = self.url.clone();
            let use_ranges = self.use_ranges;
            let if_range = self.if_range.clone();
            let total_size = self.total_size;
            let retries = self.retries;
            let global_limiter = Arc::clone(&self.global_rate_limiter);
            let job_limiter = Arc::clone(&self.job_rate_limiter);
            let directory = self.directory.clone();
            let job_id = self.job_id.clone();
            let event_broker = self.event_broker.clone();

            join_set.spawn(async move {
                loop {
                    let Some((start, end)) = queue.lock().unwrap().pop_front() else {
                        break;
                    };

                    // Check if paused or cancelled
                    if let Ok(registry) = JobRegistry::load(&directory)
                        && let Some(record) = registry.get(&job_id)
                    {
                        if record.state == JobState::Paused {
                            return Err(anyhow!("Job was paused"));
                        }
                        if record.state == JobState::Cancelled {
                            return Err(anyhow!("Job was cancelled"));
                        }
                    }

                    download_piece(
                        &fetcher,
                        &storage,
                        &url,
                        use_ranges,
                        if_range.as_deref(),
                        total_size,
                        retries,
                        start,
                        end,
                        &downloaded,
                        &global_limiter,
                        &job_limiter,
                        &job_id,
                        &event_broker,
                    )
                    .await?;
                    resume.mark_complete(start, end)?;
                }

                Ok(())
            });
        }

        // Wait for all chunk tasks to complete
        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(Ok(())) => continue,
                Ok(Err(e)) => {
                    join_set.abort_all();
                    return Err(e);
                }
                Err(e) => {
                    join_set.abort_all();
                    return Err(anyhow!("Task failed: {}", e));
                }
            }
        }

        Ok(())
    }

    fn pieces(&self) -> Vec<(u64, u64)> {
        let piece_size = if self.use_ranges {
            self.total_size
                .div_ceil(self.connections as u64)
                .min(self.piece_size)
                .max(1)
        } else {
            self.total_size
        };

        build_ranges(self.total_size, piece_size)
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "piece worker has no reusable config yet"
)]
async fn download_piece(
    fetcher: &HttpFetcher,
    storage: &ChunkStorage,
    url: &str,
    use_ranges: bool,
    if_range: Option<&str>,
    total_size: u64,
    retries: usize,
    start: u64,
    end: u64,
    downloaded: &AtomicU64,
    global_limiter: &RateLimiter,
    job_limiter: &RateLimiter,
    job_id: &str,
    event_broker: &EventBroker,
) -> Result<()> {
    let mut current_offset = start;
    let expected_end = end + 1;
    let mut attempts_left = retries;

    loop {
        let result = async {
            let resp = if use_ranges {
                fetcher
                    .fetch_chunk(url, current_offset, end, total_size, if_range)
                    .await?
            } else {
                fetcher.fetch_full(url).await?
            };
            let mut stream = resp.bytes_stream();

            while let Some(chunk_result) = stream.next().await {
                let chunk = chunk_result?;
                let len = chunk.len() as u64;
                let next_offset = current_offset
                    .checked_add(len)
                    .ok_or_else(|| anyhow!("Chunk offset overflow"))?;

                if next_offset > expected_end {
                    return Err(anyhow!(
                        "Server sent more bytes than expected for chunk {}-{}",
                        start,
                        end
                    ));
                }

                global_limiter.wait_for(len).await;
                job_limiter.wait_for(len).await;

                storage.write_at(current_offset, &chunk)?;
                current_offset = next_offset;
                let bytes_downloaded = downloaded.fetch_add(len, Ordering::Relaxed) + len;

                event_broker.emit(EventType::Progress {
                    id: job_id.to_string(),
                    bytes_downloaded,
                    bytes_total: Some(total_size),
                });
            }

            if current_offset != expected_end {
                return Err(anyhow!(
                    "Chunk {}-{} ended at {}, expected {}",
                    start,
                    end,
                    current_offset,
                    expected_end
                ));
            }

            Ok(())
        }
        .await
        .map_err(saida::redigir_erro_http);

        match result {
            Ok(()) => return Ok(()),
            Err(err) if attempts_left > 0 => {
                attempts_left -= 1;
                let delay = retry_delay(retries, attempts_left);
                if !use_ranges {
                    current_offset = start;
                    downloaded.store(0, Ordering::Relaxed);
                }
                saida::erro(format!(
                    "Retrying chunk {}-{} in {}ms ({} left): {}",
                    current_offset,
                    end,
                    delay.as_millis(),
                    attempts_left,
                    err
                ));
                sleep(delay).await;
            }
            Err(err) => {
                return Err(anyhow!(
                    "Chunk {}-{} failed after {} retries: {}",
                    start,
                    end,
                    retries,
                    err
                ));
            }
        }
    }
}

fn retry_delay(retries: usize, attempts_left: usize) -> Duration {
    let used = retries.saturating_sub(attempts_left).min(5) as u32;
    Duration::from_millis((100_u64 * 2_u64.pow(used)).min(2_000))
}

fn build_ranges(total_size: u64, piece_size: u64) -> Vec<(u64, u64)> {
    let mut ranges = Vec::new();
    let mut start = 0;

    while start < total_size {
        let end = start.saturating_add(piece_size - 1).min(total_size - 1);
        ranges.push((start, end));
        start = end + 1;
    }

    ranges
}

struct ResumeState {
    ranges_path: PathBuf,
    completed: Mutex<Vec<(u64, u64)>>,
}

impl ResumeState {
    fn new(ranges_path: PathBuf, mut completed: Vec<(u64, u64)>) -> Self {
        completed.sort_unstable();
        Self {
            ranges_path,
            completed: Mutex::new(completed),
        }
    }

    fn is_complete(&self, start: u64, end: u64) -> bool {
        range_is_complete(&self.completed.lock().unwrap(), start, end)
    }

    fn mark_complete(&self, start: u64, end: u64) -> Result<()> {
        let mut completed = self.completed.lock().unwrap();
        if range_is_complete(&completed, start, end) {
            return Ok(());
        }

        add_completed_range(&mut completed, start, end);
        write_ranges(&self.ranges_path, &completed)?;
        Ok(())
    }
}

fn add_completed_range(ranges: &mut Vec<(u64, u64)>, start: u64, end: u64) {
    ranges.push((start, end));
    ranges.sort_unstable();

    let mut merged: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
    for &(range_start, range_end) in ranges.iter() {
        match merged.last_mut() {
            Some((_, last_end)) if range_start <= *last_end + 1 => {
                *last_end = (*last_end).max(range_end);
            }
            _ => merged.push((range_start, range_end)),
        }
    }

    *ranges = merged;
}

fn write_ranges(path: &Path, ranges: &[(u64, u64)]) -> Result<()> {
    let mut contents = String::new();
    for &(start, end) in ranges {
        writeln!(contents, "{start} {end}")?;
    }

    let temp = crate::path_with_suffix(path, ".tmp");
    let result = (|| -> Result<()> {
        let mut file = File::create(&temp)?;
        file.write_all(contents.as_bytes())?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temp, path)?;
        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

fn range_is_complete(ranges: &[(u64, u64)], start: u64, end: u64) -> bool {
    let mut cursor = start;

    for &(range_start, range_end) in ranges {
        if range_end < cursor {
            continue;
        }
        if range_start > cursor {
            return false;
        }
        if range_end >= end {
            return true;
        }
        cursor = range_end + 1;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::{add_completed_range, build_ranges, range_is_complete, retry_delay, write_ranges};
    use std::{fs, sync::Barrier, thread};

    #[test]
    fn writes_all_ranges_and_removes_temporary_file() {
        let directory = std::env::temp_dir().join(format!("chorus-ranges-{}", std::process::id()));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("download.bin.part.ranges");
        let temp = directory.join("download.bin.part.ranges.tmp");
        fs::write(&temp, b"stale").unwrap();

        write_ranges(&path, &[(0, 4), (20, 29)]).unwrap();

        assert_eq!(fs::read_to_string(&path).unwrap(), "0 4\n20 29\n");
        assert!(!temp.exists());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn parallel_reader_sees_only_complete_range_files() {
        let directory =
            std::env::temp_dir().join(format!("chorus-ranges-parallel-{}", std::process::id()));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("download.bin.part.ranges");
        let mut ranges = vec![(0, 1)];
        write_ranges(&path, &ranges).unwrap();

        let start = Barrier::new(2);
        thread::scope(|scope| {
            let writer = scope.spawn(|| {
                start.wait();
                for index in 1..=2_000 {
                    // The first range records the number of lines in this snapshot.
                    ranges[0].1 = index + 1;
                    ranges.push((index * 4_096, index * 4_096 + 1));
                    write_ranges(&path, &ranges).unwrap();
                }
            });

            let mut reads = 0;
            start.wait();
            while !writer.is_finished() {
                let contents = match fs::read_to_string(&path) {
                    Ok(contents) => contents,
                    Err(err) if cfg!(windows) && crate::is_transient_file_busy(&err) => continue,
                    Err(err) => panic!("could not read ranges: {err}"),
                };
                assert!(!contents.is_empty() && contents.ends_with('\n'));
                let line_count = contents.lines().count() as u64;
                for (index, line) in contents.lines().enumerate() {
                    let (start, end) = line.split_once(' ').unwrap();
                    let range = (start.parse::<u64>().unwrap(), end.parse::<u64>().unwrap());
                    if index == 0 {
                        assert_eq!(range, (0, line_count));
                    } else {
                        let start = index as u64 * 4_096;
                        assert_eq!(range, (start, start + 1));
                    }
                }
                reads += 1;
            }
            writer.join().unwrap();
            assert!(reads > 0);
        });
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn detects_ranges_covered_by_multiple_completed_chunks() {
        let ranges = [(0, 4), (5, 9), (20, 29)];
        assert!(range_is_complete(&ranges, 0, 9));
        assert!(!range_is_complete(&ranges, 0, 10));
        assert!(!range_is_complete(&ranges, 10, 19));
    }

    #[test]
    fn builds_non_overlapping_inclusive_ranges() {
        assert_eq!(build_ranges(0, 4), Vec::<(u64, u64)>::new());
        assert_eq!(build_ranges(10, 4), vec![(0, 3), (4, 7), (8, 9)]);
        assert_eq!(build_ranges(4, 4), vec![(0, 3)]);
    }

    #[test]
    fn retry_delay_is_capped() {
        assert_eq!(retry_delay(3, 2), std::time::Duration::from_millis(200));
        assert_eq!(retry_delay(99, 0), std::time::Duration::from_millis(2_000));
    }

    #[test]
    fn completed_ranges_are_merged() {
        let mut ranges = vec![(20, 29), (0, 4)];
        add_completed_range(&mut ranges, 5, 19);
        assert_eq!(ranges, vec![(0, 29)]);
    }
}
