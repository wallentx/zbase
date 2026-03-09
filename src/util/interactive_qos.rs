use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const QUICK_SWITCHER_ACTIVE_WINDOW_MS: u64 = 900;
const SEARCH_INDEX_COMMIT_IDLE_MS: u64 = 250;
const SEARCH_INDEX_COMMIT_INTERACTIVE_MS: u64 = 900;
const CRAWL_THROTTLE_IDLE_MS: u64 = 0;
const CRAWL_THROTTLE_INTERACTIVE_MS: u64 = 20;

static LAST_QUICK_SWITCHER_INPUT_MS: AtomicU64 = AtomicU64::new(0);

pub fn mark_quick_switcher_input_activity() {
    LAST_QUICK_SWITCHER_INPUT_MS.store(now_unix_ms(), Ordering::Relaxed);
}

pub fn quick_switcher_is_interactive() -> bool {
    quick_switcher_recent_within(Duration::from_millis(QUICK_SWITCHER_ACTIVE_WINDOW_MS))
}

pub fn search_index_commit_interval() -> Duration {
    if quick_switcher_is_interactive() {
        Duration::from_millis(SEARCH_INDEX_COMMIT_INTERACTIVE_MS)
    } else {
        Duration::from_millis(SEARCH_INDEX_COMMIT_IDLE_MS)
    }
}

pub fn crawl_throttle_delay() -> Duration {
    if quick_switcher_is_interactive() {
        Duration::from_millis(CRAWL_THROTTLE_INTERACTIVE_MS)
    } else {
        Duration::from_millis(CRAWL_THROTTLE_IDLE_MS)
    }
}

fn quick_switcher_recent_within(window: Duration) -> bool {
    let now = now_unix_ms();
    let last = LAST_QUICK_SWITCHER_INPUT_MS.load(Ordering::Relaxed);
    if last == 0 || now < last {
        return false;
    }
    now.saturating_sub(last) <= window.as_millis() as u64
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}
