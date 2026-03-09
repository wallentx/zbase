#[path = "../src/services/backends/keybase/notify_inventory.rs"]
mod notify_inventory;

use notify_inventory::KeybaseNotifyKind;
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

#[test]
fn inventory_covers_notify_protocol_methods() {
    let methods = discovered_notify_methods();
    let known_methods: BTreeSet<&str> = KeybaseNotifyKind::all_known_methods()
        .iter()
        .copied()
        .collect();

    let missing: Vec<String> = methods
        .iter()
        .filter(|method| !known_methods.contains(method.as_str()))
        .cloned()
        .collect();

    assert!(
        missing.is_empty(),
        "missing notify method mappings: {missing:?}"
    );
}

fn discovered_notify_methods() -> BTreeSet<String> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let avdl_root = manifest_dir
        .join("keybase-client")
        .join("protocol")
        .join("avdl");
    let mut methods = BTreeSet::new();

    for file in notify_avdl_files(&avdl_root) {
        if file.file_name().and_then(|value| value.to_str()) == Some("notify_ctl.avdl") {
            continue;
        }

        let Ok(contents) = fs::read_to_string(&file) else {
            continue;
        };
        let Some(namespace) = extract_namespace(&contents) else {
            continue;
        };
        let Some(protocol) = extract_protocol_name(&contents) else {
            continue;
        };

        for method in extract_void_methods(&contents) {
            methods.insert(format!("{namespace}.{protocol}.{method}"));
        }
    }

    methods
}

fn notify_avdl_files(avdl_root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();

    let chat_notify = avdl_root.join("chat1").join("notify.avdl");
    if chat_notify.exists() {
        files.push(chat_notify);
    }

    let stellar_notify = avdl_root.join("stellar1").join("notify.avdl");
    if stellar_notify.exists() {
        files.push(stellar_notify);
    }

    let keybase_notify_root = avdl_root.join("keybase1");
    if let Ok(entries) = fs::read_dir(keybase_notify_root) {
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
                continue;
            };
            if name.starts_with("notify_") && name.ends_with(".avdl") {
                files.push(path);
            }
        }
    }

    files.sort();
    files
}

fn extract_namespace(contents: &str) -> Option<String> {
    let marker = "@namespace(\"";
    let start = contents.find(marker)?;
    let tail = &contents[start + marker.len()..];
    let end = tail.find('"')?;
    Some(tail[..end].to_string())
}

fn extract_protocol_name(contents: &str) -> Option<String> {
    for line in contents.lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with("protocol ") {
            continue;
        }
        let name = trimmed
            .trim_start_matches("protocol ")
            .split_whitespace()
            .next()?
            .trim_end_matches('{')
            .to_string();
        return Some(name);
    }
    None
}

fn extract_void_methods(contents: &str) -> Vec<String> {
    let mut methods = Vec::new();

    for line in contents.lines() {
        let trimmed = line.trim();
        let Some(rest) = trimmed.strip_prefix("void ") else {
            continue;
        };
        let Some((method, _)) = rest.split_once('(') else {
            continue;
        };
        methods.push(method.trim().to_string());
    }

    methods
}
