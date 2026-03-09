use std::{env, path::PathBuf};

const SOCKET_FILE: &str = "keybased.sock";
const SOCKET_OVERRIDE_ENV: &str = "KEYBASE_SOCKET_PATH";
const RUN_MODE_ENV: &str = "KEYBASE_RUN_MODE";
const HOME_ENV: &str = "HOME";

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
