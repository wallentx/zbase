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
