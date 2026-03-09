use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
};

use tokio::{
    runtime::{Builder, Runtime},
    sync::Semaphore,
};

const HIGH_PRIORITY_CONCURRENCY: usize = 4;
const LOW_PRIORITY_CONCURRENCY: usize = 2;
const RUNTIME_WORKER_THREADS: usize = 4;

#[derive(Clone, Copy, Debug)]
pub(crate) enum TaskPriority {
    High,
    Low,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct TaskRuntimeStats {
    pub high_pending: usize,
    pub high_running: usize,
    pub low_pending: usize,
    pub low_running: usize,
}

struct TaskRuntimeState {
    runtime: Runtime,
    high_semaphore: Arc<Semaphore>,
    low_semaphore: Arc<Semaphore>,
    high_pending: AtomicUsize,
    low_pending: AtomicUsize,
    high_running: AtomicUsize,
    low_running: AtomicUsize,
    high_in_flight_keys: Mutex<HashSet<String>>,
    low_in_flight_keys: Mutex<HashSet<String>>,
}

impl TaskRuntimeState {
    fn new() -> Self {
        let runtime = Builder::new_multi_thread()
            .worker_threads(RUNTIME_WORKER_THREADS)
            .enable_all()
            .thread_name("kbui-keybase-task")
            .build()
            .expect("failed to initialize keybase task runtime");
        Self {
            runtime,
            high_semaphore: Arc::new(Semaphore::new(HIGH_PRIORITY_CONCURRENCY)),
            low_semaphore: Arc::new(Semaphore::new(LOW_PRIORITY_CONCURRENCY)),
            high_pending: AtomicUsize::new(0),
            low_pending: AtomicUsize::new(0),
            high_running: AtomicUsize::new(0),
            low_running: AtomicUsize::new(0),
            high_in_flight_keys: Mutex::new(HashSet::new()),
            low_in_flight_keys: Mutex::new(HashSet::new()),
        }
    }
}

fn state() -> &'static TaskRuntimeState {
    static STATE: OnceLock<TaskRuntimeState> = OnceLock::new();
    STATE.get_or_init(TaskRuntimeState::new)
}

fn with_in_flight_set<R>(
    priority: TaskPriority,
    f: impl FnOnce(&Mutex<HashSet<String>>) -> R,
) -> R {
    let state = state();
    match priority {
        TaskPriority::High => f(&state.high_in_flight_keys),
        TaskPriority::Low => f(&state.low_in_flight_keys),
    }
}

fn counters_for_priority(
    priority: TaskPriority,
) -> (&'static AtomicUsize, &'static AtomicUsize, Arc<Semaphore>) {
    let state = state();
    match priority {
        TaskPriority::High => (
            &state.high_pending,
            &state.high_running,
            state.high_semaphore.clone(),
        ),
        TaskPriority::Low => (
            &state.low_pending,
            &state.low_running,
            state.low_semaphore.clone(),
        ),
    }
}

pub(crate) fn spawn_task<F>(priority: TaskPriority, dedupe_key: Option<String>, task: F) -> bool
where
    F: FnOnce() + Send + 'static,
{
    if let Some(key) = dedupe_key.as_ref() {
        let inserted = with_in_flight_set(priority, |mutex| {
            let mut guard = match mutex.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            guard.insert(key.clone())
        });
        if !inserted {
            return false;
        }
    }

    let (pending_counter, running_counter, semaphore) = counters_for_priority(priority);
    pending_counter.fetch_add(1, Ordering::Relaxed);
    let key_for_cleanup = dedupe_key.clone();
    state().runtime.spawn(async move {
        pending_counter.fetch_sub(1, Ordering::Relaxed);
        let permit = semaphore.acquire_owned().await;
        if permit.is_err() {
            if let Some(key) = key_for_cleanup {
                with_in_flight_set(priority, |mutex| {
                    let mut guard = match mutex.lock() {
                        Ok(guard) => guard,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    guard.remove(&key);
                });
            }
            return;
        }
        let _permit = permit.expect("permit checked");
        running_counter.fetch_add(1, Ordering::Relaxed);
        let _ = tokio::task::spawn_blocking(task).await;
        running_counter.fetch_sub(1, Ordering::Relaxed);
        if let Some(key) = key_for_cleanup {
            with_in_flight_set(priority, |mutex| {
                let mut guard = match mutex.lock() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner(),
                };
                guard.remove(&key);
            });
        }
    });
    true
}

pub(crate) fn stats() -> TaskRuntimeStats {
    let state = state();
    TaskRuntimeStats {
        high_pending: state.high_pending.load(Ordering::Relaxed),
        high_running: state.high_running.load(Ordering::Relaxed),
        low_pending: state.low_pending.load(Ordering::Relaxed),
        low_running: state.low_running.load(Ordering::Relaxed),
    }
}
