use std::sync::Mutex;
use tokio::time::{Duration, Instant, sleep};

pub struct RateLimiter {
    capacity: f64,
    tokens: Mutex<f64>,
    last_update: Mutex<Instant>,
    rate: f64,
}

impl RateLimiter {
    pub fn new(rate_bytes_per_sec: u64) -> Self {
        Self {
            capacity: rate_bytes_per_sec as f64,
            tokens: Mutex::new(0.0),
            last_update: Mutex::new(Instant::now()),
            rate: rate_bytes_per_sec as f64,
        }
    }

    pub async fn wait_for(&self, bytes: u64) {
        if let Some(duration) = self.calculate_sleep(bytes) {
            sleep(duration).await;
        }
    }

    pub fn wait_for_sync(&self, bytes: u64) {
        if let Some(duration) = self.calculate_sleep(bytes) {
            std::thread::sleep(duration);
        }
    }

    fn calculate_sleep(&self, bytes: u64) -> Option<std::time::Duration> {
        if self.rate <= 0.0 || bytes == 0 {
            return None;
        }

        let needed = bytes as f64;
        let mut sleep_duration = None;

        {
            let mut tokens = self.tokens.lock().unwrap();
            let mut last_update = self.last_update.lock().unwrap();
            let now = Instant::now();
            let elapsed = now.saturating_duration_since(*last_update).as_secs_f64();

            *tokens = (*tokens + elapsed * self.rate).min(self.capacity);
            if now > *last_update {
                *last_update = now;
            }

            if *tokens >= needed {
                *tokens -= needed;
            } else {
                let deficit = needed - *tokens;
                let wait_time_secs = deficit / self.rate;
                *tokens = 0.0;
                *last_update += Duration::from_secs_f64(wait_time_secs);

                // Sleep until the newly advanced last_update
                let now = Instant::now();
                if *last_update > now {
                    sleep_duration = Some(*last_update - now);
                }
            }
        }
        sleep_duration
    }
}
