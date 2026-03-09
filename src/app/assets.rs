use gpui::{AssetSource, Result, SharedString};
use std::{borrow::Cow, fs, path::PathBuf};

pub struct KbuiAssets;

impl AssetSource for KbuiAssets {
    fn load(&self, path: &str) -> Result<Option<Cow<'static, [u8]>>> {
        let absolute = resolve_path(path);
        Ok(Some(fs::read(absolute)?.into()))
    }

    fn list(&self, path: &str) -> Result<Vec<SharedString>> {
        let absolute = resolve_path(path);
        Ok(fs::read_dir(absolute)?
            .filter_map(|entry| {
                let path = entry.ok()?.path();
                let relative = path
                    .strip_prefix(env!("CARGO_MANIFEST_DIR"))
                    .ok()?
                    .to_string_lossy()
                    .trim_start_matches('/')
                    .to_string();
                Some(relative.into())
            })
            .collect())
    }
}

fn resolve_path(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)
}
