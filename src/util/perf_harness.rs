use std::{
    array, env,
    fs::OpenOptions,
    io::Write,
    path::PathBuf,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const ENV_BENCH_AUTOSTART: &str = "KBUI_BENCH_AUTOSTART";
const ENV_BENCH_DURATION_SECS: &str = "KBUI_BENCH_DURATION_SECS";
const ENV_BENCH_LABEL: &str = "KBUI_BENCH_LABEL";
const ENV_BENCH_SCENARIO: &str = "KBUI_BENCH_SCENARIO";
const ENV_BENCH_OUTPUT: &str = "KBUI_BENCH_OUTPUT";

const PERF_TIMER_COUNT: usize = 8;

#[derive(Clone, Copy, Debug)]
pub enum PerfTimer {
    RenderTotal,
    SidebarRender,
    MainPanelRender,
    RightPaneRender,
    ComposerInputObserver,
    DispatchUiAction,
    SyncModelsFromStore,
    DrainBackendEvents,
}

impl PerfTimer {
    pub const ALL: [Self; PERF_TIMER_COUNT] = [
        Self::RenderTotal,
        Self::SidebarRender,
        Self::MainPanelRender,
        Self::RightPaneRender,
        Self::ComposerInputObserver,
        Self::DispatchUiAction,
        Self::SyncModelsFromStore,
        Self::DrainBackendEvents,
    ];

    fn index(self) -> usize {
        match self {
            Self::RenderTotal => 0,
            Self::SidebarRender => 1,
            Self::MainPanelRender => 2,
            Self::RightPaneRender => 3,
            Self::ComposerInputObserver => 4,
            Self::DispatchUiAction => 5,
            Self::SyncModelsFromStore => 6,
            Self::DrainBackendEvents => 7,
        }
    }

    fn slug(self) -> &'static str {
        match self {
            Self::RenderTotal => "render_total",
            Self::SidebarRender => "sidebar_render",
            Self::MainPanelRender => "main_panel_render",
            Self::RightPaneRender => "right_pane_render",
            Self::ComposerInputObserver => "composer_input_observer",
            Self::DispatchUiAction => "dispatch_ui_action",
            Self::SyncModelsFromStore => "sync_models_from_store",
            Self::DrainBackendEvents => "drain_backend_events",
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct PerfHarnessConfig {
    pub autostart: bool,
    pub duration: Option<Duration>,
    pub label: Option<String>,
    pub scenario: Option<String>,
    pub output_path: Option<PathBuf>,
}

impl PerfHarnessConfig {
    pub fn from_env() -> Self {
        let autostart = env_bool(ENV_BENCH_AUTOSTART);
        let duration = env::var(ENV_BENCH_DURATION_SECS)
            .ok()
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .filter(|secs| *secs > 0)
            .map(Duration::from_secs);
        let label = env_nonempty(ENV_BENCH_LABEL);
        let scenario = env_nonempty(ENV_BENCH_SCENARIO);
        let output_path = env_nonempty(ENV_BENCH_OUTPUT).map(PathBuf::from);

        Self {
            autostart,
            duration,
            label,
            scenario,
            output_path,
        }
    }
}

pub struct PerfHarness {
    config: PerfHarnessConfig,
    run_seq: u64,
    session: Option<PerfSession>,
}

impl PerfHarness {
    pub fn from_env() -> Self {
        Self {
            config: PerfHarnessConfig::from_env(),
            run_seq: 0,
            session: None,
        }
    }

    pub fn config(&self) -> &PerfHarnessConfig {
        &self.config
    }

    pub fn is_capturing(&self) -> bool {
        self.session.is_some()
    }

    pub fn start_capture(&mut self, label_override: Option<String>) -> bool {
        if self.session.is_some() {
            return false;
        }

        self.run_seq = self.run_seq.saturating_add(1);
        let label = label_override
            .or_else(|| self.config.label.clone())
            .unwrap_or_else(|| format!("run-{}", self.run_seq));
        let scenario = self.config.scenario.clone();
        let duration_label = self
            .config
            .duration
            .map(|duration| format!("{}s", duration.as_secs()))
            .unwrap_or_else(|| "manual".to_string());
        tracing::warn!(
            "bench.capture.start label={label} scenario={} duration={duration_label}",
            scenario.as_deref().unwrap_or("unspecified")
        );

        self.session = Some(PerfSession::new(label, scenario));
        true
    }

    pub fn stop_capture(&mut self) -> bool {
        let Some(session) = self.session.take() else {
            return false;
        };

        let report = session.finish();
        report.log();

        if let Some(path) = self.config.output_path.as_ref() {
            match report.append_csv(path) {
                Ok(()) => tracing::warn!("bench.capture.saved path={}", path.display()),
                Err(error) => tracing::warn!(
                    "bench.capture.save_failed path={} error={error}",
                    path.display()
                ),
            }
        }

        true
    }

    pub fn record_duration(&mut self, timer: PerfTimer, duration: Duration) {
        let micros = duration
            .as_micros()
            .min(u128::from(u64::MAX))
            .try_into()
            .unwrap_or(u64::MAX);
        if let Some(session) = self.session.as_mut() {
            session.samples[timer.index()].push(micros);
        }
    }

    pub fn record_refresh(&mut self) {
        if let Some(session) = self.session.as_mut() {
            session.refresh_count = session.refresh_count.saturating_add(1);
        }
    }

    pub fn record_backend_poll(&mut self, event_count: usize) {
        if let Some(session) = self.session.as_mut() {
            session.backend_poll_count = session.backend_poll_count.saturating_add(1);
            session.backend_event_count = session
                .backend_event_count
                .saturating_add(event_count as u64);
            if event_count == 0 {
                session.backend_empty_poll_count =
                    session.backend_empty_poll_count.saturating_add(1);
            }
        }
    }
}

struct PerfSession {
    label: String,
    scenario: Option<String>,
    started_at: Instant,
    started_unix_ms: u128,
    samples: [Vec<u64>; PERF_TIMER_COUNT],
    refresh_count: u64,
    backend_poll_count: u64,
    backend_empty_poll_count: u64,
    backend_event_count: u64,
}

impl PerfSession {
    fn new(label: String, scenario: Option<String>) -> Self {
        let started_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or_default();

        Self {
            label,
            scenario,
            started_at: Instant::now(),
            started_unix_ms,
            samples: array::from_fn(|_| Vec::new()),
            refresh_count: 0,
            backend_poll_count: 0,
            backend_empty_poll_count: 0,
            backend_event_count: 0,
        }
    }

    fn finish(self) -> PerfRunReport {
        let elapsed_ms = self.started_at.elapsed().as_millis();
        let metrics = array::from_fn(|index| {
            let timer = PerfTimer::ALL[index];
            summarize_metric(timer.slug(), &self.samples[index])
        });

        PerfRunReport {
            label: self.label,
            scenario: self.scenario,
            started_unix_ms: self.started_unix_ms,
            elapsed_ms,
            refresh_count: self.refresh_count,
            backend_poll_count: self.backend_poll_count,
            backend_empty_poll_count: self.backend_empty_poll_count,
            backend_event_count: self.backend_event_count,
            metrics,
        }
    }
}

struct PerfRunReport {
    label: String,
    scenario: Option<String>,
    started_unix_ms: u128,
    elapsed_ms: u128,
    refresh_count: u64,
    backend_poll_count: u64,
    backend_empty_poll_count: u64,
    backend_event_count: u64,
    metrics: [MetricSummary; PERF_TIMER_COUNT],
}

impl PerfRunReport {
    fn log(&self) {
        tracing::warn!(
            "bench.capture.stop label={} scenario={} elapsed_ms={} refresh_count={} backend_poll_count={} backend_empty_poll_count={} backend_event_count={}",
            self.label,
            self.scenario.as_deref().unwrap_or("unspecified"),
            self.elapsed_ms,
            self.refresh_count,
            self.backend_poll_count,
            self.backend_empty_poll_count,
            self.backend_event_count
        );

        for metric in &self.metrics {
            tracing::warn!(
                "bench.metric {} n={} mean_ms={:.3} p50_ms={:.3} p95_ms={:.3} p99_ms={:.3} max_ms={:.3}",
                metric.name,
                metric.n,
                metric.mean_ms,
                metric.p50_ms,
                metric.p95_ms,
                metric.p99_ms,
                metric.max_ms
            );
        }
    }

    fn append_csv(&self, path: &PathBuf) -> std::io::Result<()> {
        let create_header = !path.exists();
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;

        if create_header {
            writeln!(file, "{}", Self::csv_header())?;
        }
        writeln!(file, "{}", self.csv_row())
    }

    fn csv_header() -> String {
        let mut columns = vec![
            "started_unix_ms".to_string(),
            "label".to_string(),
            "scenario".to_string(),
            "elapsed_ms".to_string(),
            "refresh_count".to_string(),
            "backend_poll_count".to_string(),
            "backend_empty_poll_count".to_string(),
            "backend_event_count".to_string(),
        ];
        for timer in PerfTimer::ALL {
            let slug = timer.slug();
            columns.push(format!("{slug}_n"));
            columns.push(format!("{slug}_mean_ms"));
            columns.push(format!("{slug}_p50_ms"));
            columns.push(format!("{slug}_p95_ms"));
            columns.push(format!("{slug}_p99_ms"));
            columns.push(format!("{slug}_max_ms"));
        }
        columns.join(",")
    }

    fn csv_row(&self) -> String {
        let mut values = vec![
            self.started_unix_ms.to_string(),
            csv_escape(&self.label),
            csv_escape(self.scenario.as_deref().unwrap_or("")),
            self.elapsed_ms.to_string(),
            self.refresh_count.to_string(),
            self.backend_poll_count.to_string(),
            self.backend_empty_poll_count.to_string(),
            self.backend_event_count.to_string(),
        ];

        for metric in &self.metrics {
            values.push(metric.n.to_string());
            values.push(format!("{:.3}", metric.mean_ms));
            values.push(format!("{:.3}", metric.p50_ms));
            values.push(format!("{:.3}", metric.p95_ms));
            values.push(format!("{:.3}", metric.p99_ms));
            values.push(format!("{:.3}", metric.max_ms));
        }

        values.join(",")
    }
}

#[derive(Clone, Debug, Default)]
struct MetricSummary {
    name: &'static str,
    n: usize,
    mean_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

fn summarize_metric(name: &'static str, samples_us: &[u64]) -> MetricSummary {
    if samples_us.is_empty() {
        return MetricSummary {
            name,
            ..MetricSummary::default()
        };
    }

    let mut sorted = samples_us.to_vec();
    sorted.sort_unstable();
    let sum_us: u128 = samples_us.iter().map(|sample| u128::from(*sample)).sum();
    let n = samples_us.len();

    MetricSummary {
        name,
        n,
        mean_ms: us_to_ms(sum_us as f64 / n as f64),
        p50_ms: us_to_ms(percentile_us(&sorted, 50.0) as f64),
        p95_ms: us_to_ms(percentile_us(&sorted, 95.0) as f64),
        p99_ms: us_to_ms(percentile_us(&sorted, 99.0) as f64),
        max_ms: us_to_ms(*sorted.last().unwrap_or(&0) as f64),
    }
}

fn percentile_us(sorted: &[u64], percentile: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    if sorted.len() == 1 {
        return sorted[0];
    }

    let clamped = percentile.clamp(0.0, 100.0);
    let rank = (clamped / 100.0) * (sorted.len().saturating_sub(1) as f64);
    let index = rank.round() as usize;
    sorted[index.min(sorted.len() - 1)]
}

fn us_to_ms(value_us: f64) -> f64 {
    value_us / 1_000.0
}

fn env_nonempty(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_bool(name: &str) -> bool {
    env::var(name)
        .ok()
        .map(|raw| {
            matches!(
                raw.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}
