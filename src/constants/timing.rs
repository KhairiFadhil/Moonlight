use std::time::Duration;

pub const MOVEMENT_TICK_MS: u64 = 17;
pub const KEEPALIVE_INTERVAL_MS: u64 = 5_000;
pub const FLUSH_INTERVAL_MS: u64 = 50;
pub const HTTP_TIMEOUT_SECS: u64 = 15;

// Adaptive time-sync constants (mirrors KukouriTime SetTimeOffset algorithm)
pub const ST_INTERVAL_INIT_SECS: i32 = 5;
pub const ST_INTERVAL_MIN_SECS: i32 = 5;
pub const ST_INTERVAL_MAX_SECS: i32 = 300;
pub const ST_INTERVAL_STEP_SECS: i32 = 20;
pub const ST_SAMPLE_COUNT: usize = 15;
pub const ST_SUCCESS_THRESHOLD: i32 = 15;

pub fn movement_tick_interval() -> Duration {
    Duration::from_millis(MOVEMENT_TICK_MS)
}

pub fn keepalive_interval() -> Duration {
    Duration::from_millis(KEEPALIVE_INTERVAL_MS)
}

pub fn flush_interval() -> Duration {
    Duration::from_millis(FLUSH_INTERVAL_MS)
}

pub fn http_timeout() -> Duration {
    Duration::from_secs(HTTP_TIMEOUT_SECS)
}
