use std::{env, path::PathBuf, process::Command, thread, time::Duration, time::Instant};

use tracing::{info, warn};

const SOCKET_FILE: &str = "keybased.sock";
const SOCKET_OVERRIDE_ENV: &str = "KEYBASE_SOCKET_PATH";
const RUN_MODE_ENV: &str = "KEYBASE_RUN_MODE";
const HOME_ENV: &str = "HOME";

const SERVICE_START_POLL_INTERVAL: Duration = Duration::from_millis(250);
const SERVICE_START_TIMEOUT: Duration = Duration::from_secs(10);

pub fn socket_path() -> Option<PathBuf> {
    if let Ok(value) = env::var(SOCKET_OVERRIDE_ENV) {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return Some(PathBuf::from(trimmed));
        }
    }

    #[cfg(target_os = "macos")]
    {
        return socket_path_macos();
    }

    #[cfg(target_os = "linux")]
    {
        return socket_path_linux();
    }

    #[cfg(target_os = "windows")]
    {
        return socket_path_windows();
    }

    #[allow(unreachable_code)]
    None
}

#[cfg(target_os = "macos")]
fn socket_path_macos() -> Option<PathBuf> {
    let home = env::var(HOME_ENV).ok()?;
    let app_name = app_name();
    Some(
        PathBuf::from(home)
            .join("Library")
            .join("Caches")
            .join(app_name)
            .join(SOCKET_FILE),
    )
}

#[cfg(target_os = "linux")]
fn socket_path_linux() -> Option<PathBuf> {
    let run_mode = run_mode();
    let app_name = linux_app_name(&run_mode);
    let runtime_dir = env::var("XDG_RUNTIME_DIR").ok();

    if let Some(dir) = runtime_dir {
        if !dir.trim().is_empty() {
            return Some(PathBuf::from(dir).join(app_name).join(SOCKET_FILE));
        }
    }

    let home = env::var(HOME_ENV).ok()?;
    Some(
        PathBuf::from(home)
            .join(".config")
            .join(app_name)
            .join(SOCKET_FILE),
    )
}

#[cfg(target_os = "windows")]
fn socket_path_windows() -> Option<PathBuf> {
    None
}

fn app_name() -> String {
    let run_mode = run_mode();
    if run_mode == "prod" {
        "Keybase".to_string()
    } else {
        format!("Keybase{}", capitalize_ascii(&run_mode))
    }
}

#[cfg(target_os = "linux")]
fn linux_app_name(run_mode: &str) -> String {
    if run_mode == "prod" {
        "keybase".to_string()
    } else {
        format!("keybase.{run_mode}")
    }
}

fn run_mode() -> String {
    env::var(RUN_MODE_ENV)
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "prod".to_string())
}

fn capitalize_ascii(value: &str) -> String {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
        None => String::new(),
    }
}

/// Checks whether the Keybase service socket exists. If it does not, attempts
/// to start the service using the platform-native mechanism and polls until the
/// socket appears or a timeout is reached. Returns `true` if the socket is
/// available after the call (whether it was already running or freshly started).
pub fn ensure_service_running() -> bool {
    let Some(path) = socket_path() else {
        warn!(target: "zbase.service", "cannot determine keybase socket path");
        return false;
    };

    if path.exists() {
        return true;
    }

    info!(
        target: "zbase.service",
        socket = %path.display(),
        "keybase socket not found, attempting to start service"
    );

    if !try_start_service() {
        return false;
    }

    let started = Instant::now();
    while started.elapsed() < SERVICE_START_TIMEOUT {
        thread::sleep(SERVICE_START_POLL_INTERVAL);
        if path.exists() {
            info!(
                target: "zbase.service",
                elapsed_ms = started.elapsed().as_millis(),
                "keybase service socket appeared"
            );
            return true;
        }
    }

    warn!(
        target: "zbase.service",
        timeout_secs = SERVICE_START_TIMEOUT.as_secs(),
        "timed out waiting for keybase service socket"
    );
    false
}

#[cfg(target_os = "macos")]
fn try_start_service() -> bool {
    let label = match run_mode().as_str() {
        "prod" => "keybase.service".to_string(),
        mode => format!("keybase.{mode}.service"),
    };

    info!(target: "zbase.service", label = %label, "starting keybase via launchctl");
    match Command::new("launchctl")
        .args(["start", &label])
        .output()
    {
        Ok(output) if output.status.success() => true,
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!(
                target: "zbase.service",
                exit_code = output.status.code(),
                stderr = %stderr.trim(),
                "launchctl start failed, falling back to direct launch"
            );
            try_start_service_direct()
        }
        Err(error) => {
            warn!(target: "zbase.service", %error, "launchctl not available, falling back to direct launch");
            try_start_service_direct()
        }
    }
}

#[cfg(target_os = "linux")]
fn try_start_service() -> bool {
    info!(target: "zbase.service", "trying systemctl --user start keybase");
    match Command::new("systemctl")
        .args(["--user", "start", "keybase"])
        .output()
    {
        Ok(output) if output.status.success() => return true,
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!(
                target: "zbase.service",
                exit_code = output.status.code(),
                stderr = %stderr.trim(),
                "systemctl start failed, falling back to direct launch"
            );
        }
        Err(error) => {
            warn!(target: "zbase.service", %error, "systemctl not available, falling back to direct launch");
        }
    }

    if let Some(run_keybase) = find_run_keybase_script() {
        info!(target: "zbase.service", path = %run_keybase.display(), "trying run_keybase script");
        match Command::new(&run_keybase).output() {
            Ok(output) if output.status.success() => return true,
            _ => {}
        }
    }

    try_start_service_direct()
}

#[cfg(target_os = "linux")]
fn find_run_keybase_script() -> Option<PathBuf> {
    for candidate in &["/usr/bin/run_keybase", "/usr/local/bin/run_keybase"] {
        let p = PathBuf::from(candidate);
        if p.exists() {
            return Some(p);
        }
    }
    None
}

#[cfg(target_os = "windows")]
fn try_start_service() -> bool {
    warn!(target: "zbase.service", "automatic service start not yet supported on Windows");
    false
}

/// Spawn `keybase service` as a detached background process. Works on both
/// macOS and Linux as a last-resort fallback.
#[cfg(not(target_os = "windows"))]
fn try_start_service_direct() -> bool {
    let binary = find_keybase_binary();
    let Some(binary) = binary else {
        warn!(target: "zbase.service", "could not locate keybase binary");
        return false;
    };

    info!(target: "zbase.service", binary = %binary.display(), "spawning keybase service directly");
    match Command::new(&binary)
        .args(["service"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
    {
        Ok(_child) => true,
        Err(error) => {
            warn!(target: "zbase.service", %error, "failed to spawn keybase service");
            false
        }
    }
}

fn find_keybase_binary() -> Option<PathBuf> {
    #[cfg(target_os = "macos")]
    {
        let app_binary =
            PathBuf::from("/Applications/Keybase.app/Contents/SharedSupport/bin/keybase");
        if app_binary.exists() {
            return Some(app_binary);
        }
    }

    for dir in env::var("PATH")
        .unwrap_or_default()
        .split(':')
        .filter(|d| !d.is_empty())
    {
        let candidate = PathBuf::from(dir).join("keybase");
        if candidate.exists() {
            return Some(candidate);
        }
    }

    None
}
