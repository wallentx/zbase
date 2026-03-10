use crate::domain::attachment::AttachmentSource;
use crate::domain::ids::{MessageId, UserId};
use crate::models::file_upload_model::FileUploadLightboxModel;

#[derive(Clone, Debug)]
pub struct SidebarHoverTooltip {
    pub text: String,
    pub anchor_x: f32,
    pub anchor_y: f32,
    pub width_px: f32,
}

#[derive(Clone, Debug)]
pub struct ReactionHoverTooltip {
    pub text: String,
    pub anchor_x: f32,
    pub anchor_y: f32,
    pub width_px: f32,
}

#[derive(Clone, Debug)]
pub struct FullscreenImageOverlay {
    pub source: AttachmentSource,
    pub caption: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
}

#[derive(Clone, Debug)]
pub struct OverlayModel {
    pub new_chat_open: bool,
    pub quick_switcher_open: bool,
    pub command_palette_open: bool,
    pub emoji_picker_open: bool,
    pub reaction_target_message_id: Option<MessageId>,
    pub fullscreen_image: Option<FullscreenImageOverlay>,
    pub file_upload_lightbox: Option<FileUploadLightboxModel>,
    pub active_modal: Option<String>,
    pub profile_card_user_id: Option<UserId>,
    pub profile_card_position: Option<(f32, f32)>,
    pub sidebar_hover_tooltip: Option<SidebarHoverTooltip>,
    pub reaction_hover_tooltip: Option<ReactionHoverTooltip>,
}
