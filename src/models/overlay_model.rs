use crate::domain::attachment::AttachmentSource;

#[derive(Clone, Debug)]
pub struct FullscreenImageOverlay {
    pub source: AttachmentSource,
    pub caption: Option<String>,
}

#[derive(Clone, Debug)]
pub struct OverlayModel {
    pub quick_switcher_open: bool,
    pub command_palette_open: bool,
    pub emoji_picker_open: bool,
    pub fullscreen_image: Option<FullscreenImageOverlay>,
    pub active_modal: Option<String>,
    pub active_context_menu: Option<String>,
}
