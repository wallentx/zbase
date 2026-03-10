use std::path::Path;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttachmentKind {
    Image,
    Video,
    Audio,
    File,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttachmentSource {
    Url(String),
    LocalPath(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttachmentPreview {
    pub source: AttachmentSource,
    pub width: Option<u32>,
    pub height: Option<u32>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AttachmentSummary {
    pub name: String,
    pub kind: AttachmentKind,
    pub mime_type: Option<String>,
    pub size_bytes: u64,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub preview: Option<AttachmentPreview>,
    pub duration_ms: Option<u64>,
    pub waveform: Option<Vec<f32>>,
    pub source: Option<AttachmentSource>,
}

impl Default for AttachmentSummary {
    fn default() -> Self {
        Self {
            name: String::new(),
            kind: AttachmentKind::File,
            mime_type: None,
            size_bytes: 0,
            width: None,
            height: None,
            preview: None,
            duration_ms: None,
            waveform: None,
            source: None,
        }
    }
}

pub fn attachment_kind_from_path(path: &Path) -> AttachmentKind {
    let ext = path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.to_ascii_lowercase());
    match ext.as_deref() {
        Some("jpg" | "jpeg" | "png" | "gif" | "bmp" | "webp") => AttachmentKind::Image,
        Some("mp4" | "mov" | "avi" | "mkv") => AttachmentKind::Video,
        Some("mp3" | "m4a" | "ogg" | "wav" | "aac") => AttachmentKind::Audio,
        _ => AttachmentKind::File,
    }
}
