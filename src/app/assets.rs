use gpui::{AssetSource, Result, SharedString};
use rust_embed::RustEmbed;
use std::borrow::Cow;

#[derive(RustEmbed)]
#[folder = "assets"]
#[prefix = "assets/"]
struct EmbeddedAssets;

pub struct KbuiAssets;

impl AssetSource for KbuiAssets {
    fn load(&self, path: &str) -> Result<Option<Cow<'static, [u8]>>> {
        Ok(EmbeddedAssets::get(path).map(|file| file.data))
    }

    fn list(&self, path: &str) -> Result<Vec<SharedString>> {
        let prefix = if path.ends_with('/') || path.is_empty() {
            path.to_string()
        } else {
            format!("{path}/")
        };
        Ok(EmbeddedAssets::iter()
            .filter(|name| name.starts_with(&prefix))
            .map(|name| SharedString::from(name.into_owned()))
            .collect())
    }
}
