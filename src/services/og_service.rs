use crate::domain::message::LinkPreview;
use crate::services::local_store::LocalStore;
use crate::services::local_store::paths::data_root;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, mpsc::Sender};

const STREAM_CHUNK_SIZE: usize = 16 * 1024;
const MAX_HEAD_BYTES: usize = 1024 * 1024;
const MAX_IMAGE_BYTES: usize = 5 * 1024 * 1024;
const FETCH_TIMEOUT_SECS: u64 = 10;
const MAX_CONCURRENT_FETCHES: usize = 4;

#[derive(Clone, Debug)]
pub struct OgFetchResult {
    pub url: String,
    pub preview: Option<LinkPreview>,
}

pub struct OgService {
    cache: HashMap<String, Option<LinkPreview>>,
    local_store: Arc<LocalStore>,
    pending: usize,
}

impl OgService {
    pub fn new(local_store: Arc<LocalStore>) -> Self {
        Self {
            cache: HashMap::new(),
            local_store,
            pending: 0,
        }
    }

    pub fn lookup(&self, url: &str) -> Option<Option<&LinkPreview>> {
        if let Some(cached) = self.cache.get(url) {
            return Some(cached.as_ref());
        }
        None
    }

    pub fn schedule_fetch(&mut self, url: &str, sender: &Sender<OgFetchResult>) {
        let key = url.to_string();
        if self.cache.contains_key(&key) {
            return;
        }
        if let Ok(Some(mut preview)) = self.local_store.get_og_preview(&key) {
            if let Some(ref thumb) = preview.thumbnail_asset {
                if !PathBuf::from(thumb).exists() {
                    preview.thumbnail_asset = None;
                    preview.media_width = None;
                    preview.media_height = None;
                }
            }
            self.cache.insert(key, Some(preview));
            return;
        }
        if self.pending >= MAX_CONCURRENT_FETCHES {
            return;
        }
        self.cache.insert(key.clone(), None);
        self.pending += 1;
        let sender = sender.clone();
        std::thread::spawn(move || {
            let preview = fetch_og_preview(&key);
            let _ = sender.send(OgFetchResult { url: key, preview });
        });
    }

    pub fn apply_result(&mut self, result: OgFetchResult) {
        self.pending = self.pending.saturating_sub(1);
        if let Some(ref preview) = result.preview {
            let _ = self.local_store.upsert_og_preview(&result.url, preview);
        }
        self.cache.insert(result.url.clone(), result.preview);
    }
}

fn fetch_og_preview(url: &str) -> Option<LinkPreview> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .ok()?;
    runtime.block_on(fetch_og_preview_async(url))
}

async fn fetch_og_preview_async(url: &str) -> Option<LinkPreview> {
    let normalized = if url.contains("://") {
        url.to_string()
    } else {
        format!("https://{url}")
    };

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(FETCH_TIMEOUT_SECS))
        .redirect(reqwest::redirect::Policy::limited(5))
        .build()
        .ok()?;

    let mut response = client
        .get(&normalized)
        .header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
        .header("Accept", "text/html")
        .send()
        .await
        .ok()?;

    if !response.status().is_success() {
        return None;
    }

    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v: &reqwest::header::HeaderValue| v.to_str().ok())
        .unwrap_or("");
    if !content_type.contains("text/html") && !content_type.contains("application/xhtml") {
        return None;
    }

    let head = stream_until_head_close(&mut response).await?;

    let title = extract_meta_content(&head, "og:title")
        .or_else(|| extract_meta_content(&head, "twitter:title"))
        .or_else(|| extract_title_tag(&head));
    let image = extract_meta_content(&head, "og:image")
        .or_else(|| extract_meta_content(&head, "twitter:image"));
    let description = extract_meta_content(&head, "og:description")
        .or_else(|| extract_meta_content(&head, "twitter:description"))
        .or_else(|| extract_meta_content(&head, "description"));
    let site_name = extract_meta_content(&head, "og:site_name");

    if title.is_none() && image.is_none() {
        return None;
    }

    let thumbnail_url = image
        .as_ref()
        .and_then(|img| resolve_image_url(&normalized, img));
    let (thumbnail_asset, media_dimensions) = if let Some(ref image_url) = thumbnail_url {
        download_thumbnail(&client, image_url)
            .await
            .map(|(path, dims)| (Some(path), dims))
            .unwrap_or((None, None))
    } else {
        (None, None)
    };
    let (media_width, media_height) = media_dimensions
        .map(|(w, h)| (Some(w), Some(h)))
        .unwrap_or((None, None));

    Some(LinkPreview {
        url: url.to_string(),
        video_url: None,
        title,
        site: site_name,
        description,
        thumbnail_asset,
        is_media: false,
        media_width,
        media_height,
        is_video: false,
    })
}

async fn stream_until_head_close(response: &mut reqwest::Response) -> Option<String> {
    let mut buf = Vec::with_capacity(STREAM_CHUNK_SIZE * 2);
    while let Some(chunk) = response.chunk().await.ok()? {
        buf.extend_from_slice(&chunk);
        let partial = String::from_utf8_lossy(&buf);
        if partial.to_ascii_lowercase().contains("</head") {
            return extract_head(&partial);
        }
        if buf.len() >= MAX_HEAD_BYTES {
            return extract_head(&partial);
        }
    }
    let partial = String::from_utf8_lossy(&buf);
    extract_head(&partial)
}

fn og_thumbnail_cache_dir() -> PathBuf {
    data_root().join("og_thumbnails")
}

fn thumbnail_cache_path(url: &str) -> PathBuf {
    use sha2::{Digest, Sha256};
    let hash = format!("{:x}", Sha256::digest(url.as_bytes()));
    let ext = url
        .rsplit('.')
        .next()
        .filter(|e| {
            matches!(
                e.to_ascii_lowercase().as_str(),
                "jpg" | "jpeg" | "png" | "gif" | "webp"
            )
        })
        .unwrap_or("jpg");
    og_thumbnail_cache_dir().join(format!("{hash}.{ext}"))
}

fn image_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    use std::io::Cursor;
    let cursor = Cursor::new(bytes);
    let reader = image::ImageReader::new(cursor).with_guessed_format().ok()?;
    reader.into_dimensions().ok()
}

async fn download_thumbnail(
    client: &reqwest::Client,
    url: &str,
) -> Option<(String, Option<(u32, u32)>)> {
    let cache_path = thumbnail_cache_path(url);
    if cache_path.exists() {
        // Dimensions unknown without re-reading; caller can still fit by box size.
        return Some((cache_path.to_string_lossy().to_string(), None));
    }
    let response = client.get(url).send().await.ok()?;
    if !response.status().is_success() {
        return None;
    }
    let bytes = response.bytes().await.ok()?;
    if bytes.is_empty() || bytes.len() > MAX_IMAGE_BYTES {
        return None;
    }
    let dims = image_dimensions(bytes.as_ref());
    let _ = std::fs::create_dir_all(og_thumbnail_cache_dir());
    std::fs::write(&cache_path, &bytes).ok()?;
    Some((cache_path.to_string_lossy().to_string(), dims))
}

fn extract_head(html: &str) -> Option<String> {
    let lower = html.to_ascii_lowercase();
    let head_start = lower.find("<head")?;
    let content_start = html[head_start..].find('>')? + head_start + 1;
    let head_end = lower[content_start..]
        .find("</head")
        .map(|i| i + content_start)
        .unwrap_or(html.len());
    Some(html[content_start..head_end].to_string())
}

fn extract_meta_content(head: &str, property: &str) -> Option<String> {
    let lower = head.to_ascii_lowercase();
    let mut search_from = 0;
    while let Some(meta_pos) = lower[search_from..].find("<meta") {
        let abs_pos = search_from + meta_pos;
        let tag_end = match lower[abs_pos..].find('>') {
            Some(i) => abs_pos + i + 1,
            None => break,
        };
        let tag = &head[abs_pos..tag_end];
        let tag_lower = &lower[abs_pos..tag_end];

        let has_property = tag_lower.contains(&format!("property=\"{property}\""))
            || tag_lower.contains(&format!("property='{property}'"))
            || tag_lower.contains(&format!("name=\"{property}\""))
            || tag_lower.contains(&format!("name='{property}'"));

        if has_property {
            if let Some(value) = extract_attribute_value(tag, "content") {
                let decoded = decode_html_entities(&value);
                if !decoded.trim().is_empty() {
                    return Some(decoded.trim().to_string());
                }
            }
        }
        search_from = tag_end;
    }
    None
}

fn extract_title_tag(head: &str) -> Option<String> {
    let lower = head.to_ascii_lowercase();
    let start = lower.find("<title")?;
    let content_start = head[start..].find('>')? + start + 1;
    let end = lower[content_start..].find("</title")? + content_start;
    let title = decode_html_entities(&head[content_start..end]);
    let trimmed = title.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn extract_attribute_value(tag: &str, attr: &str) -> Option<String> {
    let lower = tag.to_ascii_lowercase();
    let patterns = [format!("{attr}=\""), format!("{attr}='")];
    for pattern in &patterns {
        if let Some(start) = lower.find(pattern.as_str()) {
            let value_start = start + pattern.len();
            let quote = pattern.chars().last().unwrap();
            if let Some(end) = tag[value_start..].find(quote) {
                return Some(tag[value_start..value_start + end].to_string());
            }
        }
    }
    None
}

fn decode_html_entities(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'")
        .replace("&#x27;", "'")
        .replace("&#x2F;", "/")
        .replace("&nbsp;", " ")
}

fn resolve_image_url(page_url: &str, image: &str) -> Option<String> {
    let trimmed = image.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return Some(trimmed.to_string());
    }
    if trimmed.starts_with("//") {
        return Some(format!("https:{trimmed}"));
    }
    let base = page_url.find("://").map(|i| {
        let after_scheme = &page_url[i + 3..];
        let host_end = after_scheme.find('/').unwrap_or(after_scheme.len());
        &page_url[..i + 3 + host_end]
    })?;
    if trimmed.starts_with('/') {
        Some(format!("{base}{trimmed}"))
    } else {
        Some(format!("{base}/{trimmed}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_og_title_from_html() {
        let html = r#"<html><head><meta property="og:title" content="Hello World"><meta property="og:image" content="https://example.com/img.jpg"></head><body></body></html>"#;
        let head = extract_head(html).unwrap();
        assert_eq!(
            extract_meta_content(&head, "og:title").unwrap(),
            "Hello World"
        );
        assert_eq!(
            extract_meta_content(&head, "og:image").unwrap(),
            "https://example.com/img.jpg"
        );
    }

    #[test]
    fn extract_title_tag_fallback() {
        let html = "<html><head><title>Fallback Title</title></head><body></body></html>";
        let head = extract_head(html).unwrap();
        assert!(extract_meta_content(&head, "og:title").is_none());
        assert_eq!(extract_title_tag(&head).unwrap(), "Fallback Title");
    }

    #[test]
    fn resolve_relative_image() {
        assert_eq!(
            resolve_image_url("https://example.com/page", "/img/photo.jpg").unwrap(),
            "https://example.com/img/photo.jpg"
        );
        assert_eq!(
            resolve_image_url("https://example.com/page", "//cdn.example.com/img.jpg").unwrap(),
            "https://cdn.example.com/img.jpg"
        );
    }

    #[test]
    fn html_entity_decoding() {
        let html = r#"<html><head><meta property="og:title" content="Tom &amp; Jerry&#39;s"></head></html>"#;
        let head = extract_head(html).unwrap();
        assert_eq!(
            extract_meta_content(&head, "og:title").unwrap(),
            "Tom & Jerry's"
        );
    }
}
