use std::{env, path::PathBuf};

use directories::ProjectDirs;

const DATA_DIR_ENV: &str = "ZBASE_DATA_DIR";

pub fn data_root() -> PathBuf {
    if let Ok(override_dir) = env::var(DATA_DIR_ENV)
        && !override_dir.trim().is_empty() {
            return PathBuf::from(override_dir);
        }

    if let Some(project_dirs) = ProjectDirs::from("io", "zbase", "zbase") {
        return project_dirs.data_local_dir().to_path_buf();
    }

    env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join(".zbase")
}

pub fn keybase_store_root() -> PathBuf {
    data_root().join("keybase")
}

pub fn rocksdb_path() -> PathBuf {
    keybase_store_root().join("rocksdb")
}

pub fn tantivy_path() -> PathBuf {
    keybase_store_root().join("tantivy")
}

pub fn avatars_dir() -> PathBuf {
    keybase_store_root().join("avatars")
}

pub fn emojis_dir() -> PathBuf {
    keybase_store_root().join("emojis")
}

pub fn emoji_assets_dir() -> PathBuf {
    emojis_dir().join("assets")
}
