pub mod app_window;
pub mod avatar;
pub mod calls;
pub mod composer;
pub mod conversation;
pub mod home;
pub mod inbox;
pub mod inline_markdown;
pub mod input;
pub mod main_panel;
pub mod overlays;
pub mod preferences;
pub mod profile;
pub mod right_pane;
pub mod search;
pub mod selectable_text;
pub mod sidebar;
pub mod splash;
pub mod timeline;

use gpui::{
    AnyElement, Background, BoxShadow, FontWeight, Hsla, ImageSource, IntoElement, ParentElement,
    SharedString, Styled, div, linear_color_stop, linear_gradient, point, px, rgb, svg,
};
use std::{cell::Cell, path::PathBuf};

use crate::{
    app::theme::ThemeVariant,
    domain::attachment::{AttachmentKind, AttachmentSource, AttachmentSummary},
};

thread_local! {
    static ACTIVE_THEME: Cell<ThemeVariant> = const { Cell::new(ThemeVariant::Light) };
}

#[derive(Clone, Copy)]
struct Palette {
    app_bg: u32,
    titlebar_bg: u32,
    titlebar_surface: u32,
    rail_bg: u32,
    rail_surface: u32,
    sidebar_bg: u32,
    main_bg: u32,
    panel_bg: u32,
    panel_alt_bg: u32,
    border: u32,
    text_primary: u32,
    text_secondary: u32,
    text_inverted: u32,
    text_muted_inverted: u32,
    accent: u32,
    accent_soft: u32,
    success: u32,
    success_soft: u32,
    warning: u32,
    warning_soft: u32,
    danger: u32,
    danger_soft: u32,
    affinity_positive: u32,
    affinity_positive_soft: u32,
    affinity_broken: u32,
    affinity_broken_soft: u32,
    mention: u32,
    mention_soft: u32,
    mention_you: u32,
    mention_you_soft: u32,
    selection: u32,
}

const LIGHT_PALETTE: Palette = Palette {
    app_bg: 0xf3f7fa,
    titlebar_bg: 0xf7fbfd,
    titlebar_surface: 0xe9eff4,
    rail_bg: 0xf4f8fb,
    rail_surface: 0xe5edf3,
    sidebar_bg: 0xf7fafc,
    main_bg: 0xfbfcfd,
    panel_bg: 0xffffff,
    panel_alt_bg: 0xf4f7fa,
    border: 0xdbe3ea,
    text_primary: 0x334155,
    text_secondary: 0x7a8897,
    text_inverted: 0xf7fbff,
    text_muted_inverted: 0x93a4b8,
    accent: 0x6f87ad,
    accent_soft: 0xebf0f6,
    success: 0x5e8f88,
    success_soft: 0xe9f3f1,
    warning: 0xa58558,
    warning_soft: 0xf8efdf,
    danger: 0xb47a74,
    danger_soft: 0xf7ecea,
    affinity_positive: 0x5e8f88,
    affinity_positive_soft: 0xe9f3f1,
    affinity_broken: 0xb47a74,
    affinity_broken_soft: 0xf7ecea,
    mention: 0x6588a7,
    mention_soft: 0xe9f0f5,
    mention_you: 0xa58558,
    mention_you_soft: 0xf8efdf,
    selection: 0x6f87ad26,
};

const DARK_PALETTE: Palette = Palette {
    app_bg: 0x0e1419,
    titlebar_bg: 0x131b22,
    titlebar_surface: 0x1a242d,
    rail_bg: 0x10171d,
    rail_surface: 0x18222b,
    sidebar_bg: 0x111920,
    main_bg: 0x0f161d,
    panel_bg: 0x141d25,
    panel_alt_bg: 0x1a242e,
    border: 0x283543,
    text_primary: 0xe4ebf3,
    text_secondary: 0x93a3b5,
    text_inverted: 0x081018,
    text_muted_inverted: 0x5d7086,
    accent: 0x8aa4c7,
    accent_soft: 0x243242,
    success: 0x79b2a7,
    success_soft: 0x1e302f,
    warning: 0xc9a978,
    warning_soft: 0x352b1d,
    danger: 0xc6918c,
    danger_soft: 0x362220,
    affinity_positive: 0x79b2a7,
    affinity_positive_soft: 0x1e302f,
    affinity_broken: 0xc6918c,
    affinity_broken_soft: 0x362220,
    mention: 0x8eb4d2,
    mention_soft: 0x1f3140,
    mention_you: 0xc9a978,
    mention_you_soft: 0x352b1d,
    selection: 0x8aa4c733,
};

fn palette() -> Palette {
    ACTIVE_THEME.with(|theme| match theme.get() {
        ThemeVariant::Light => LIGHT_PALETTE,
        ThemeVariant::Dark => DARK_PALETTE,
    })
}

pub fn with_theme<T>(theme: ThemeVariant, f: impl FnOnce() -> T) -> T {
    ACTIVE_THEME.with(|active| {
        let previous = active.replace(theme);
        let result = f();
        active.set(previous);
        result
    })
}

pub fn current_theme() -> ThemeVariant {
    ACTIVE_THEME.with(|theme| theme.get())
}

pub fn is_dark_theme() -> bool {
    current_theme() == ThemeVariant::Dark
}

pub fn app_bg() -> u32 {
    palette().app_bg
}

pub fn titlebar_bg() -> u32 {
    palette().titlebar_bg
}

pub fn titlebar_surface() -> u32 {
    palette().titlebar_surface
}

pub fn rail_bg() -> u32 {
    palette().rail_bg
}

pub fn rail_surface() -> u32 {
    palette().rail_surface
}

pub fn sidebar_bg() -> u32 {
    palette().sidebar_bg
}

pub fn main_bg() -> u32 {
    palette().main_bg
}

pub fn panel_bg() -> u32 {
    palette().panel_bg
}

pub fn panel_alt_bg() -> u32 {
    palette().panel_alt_bg
}

pub fn panel_surface() -> Hsla {
    tint(panel_bg(), if is_dark_theme() { 0.55 } else { 0.45 })
}

pub fn panel_alt_surface() -> Hsla {
    tint(panel_alt_bg(), if is_dark_theme() { 0.50 } else { 0.40 })
}

pub fn border() -> u32 {
    palette().border
}

pub fn text_primary() -> u32 {
    palette().text_primary
}

pub fn text_secondary() -> u32 {
    palette().text_secondary
}

pub fn text_inverted() -> u32 {
    palette().text_inverted
}

pub fn text_muted_inverted() -> u32 {
    palette().text_muted_inverted
}

pub fn accent() -> u32 {
    palette().accent
}

pub fn accent_soft() -> u32 {
    palette().accent_soft
}

pub fn success() -> u32 {
    palette().success
}

pub fn success_soft() -> u32 {
    palette().success_soft
}

pub fn warning() -> u32 {
    palette().warning
}

pub fn warning_soft() -> u32 {
    palette().warning_soft
}

pub fn danger() -> u32 {
    palette().danger
}

pub fn danger_soft() -> u32 {
    palette().danger_soft
}

pub fn affinity_positive() -> u32 {
    palette().affinity_positive
}

pub fn affinity_positive_soft() -> u32 {
    palette().affinity_positive_soft
}

pub fn affinity_broken() -> u32 {
    palette().affinity_broken
}

pub fn affinity_broken_soft() -> u32 {
    palette().affinity_broken_soft
}

pub fn mention() -> u32 {
    palette().mention
}

pub fn mention_soft() -> u32 {
    palette().mention_soft
}

pub fn mention_you() -> u32 {
    palette().mention_you
}

pub fn mention_you_soft() -> u32 {
    palette().mention_you_soft
}

pub fn selection() -> u32 {
    palette().selection
}

pub fn mention_colors_for_user(
    affinity_index: &std::collections::HashMap<
        crate::domain::ids::UserId,
        crate::domain::affinity::Affinity,
    >,
    current_user_id: Option<&crate::domain::ids::UserId>,
    user_id: &crate::domain::ids::UserId,
) -> (u32, u32) {
    if current_user_id.is_some_and(|me| me.0.eq_ignore_ascii_case(&user_id.0)) {
        return (mention_you(), mention_you_soft());
    }
    let affinity = affinity_index
        .get(user_id)
        .copied()
        .or_else(|| {
            let lower = user_id.0.to_ascii_lowercase();
            if lower == user_id.0 {
                None
            } else {
                affinity_index
                    .get(&crate::domain::ids::UserId::new(lower))
                    .copied()
            }
        })
        .unwrap_or(crate::domain::affinity::Affinity::None);
    match affinity {
        crate::domain::affinity::Affinity::None => (mention(), mention_soft()),
        crate::domain::affinity::Affinity::Positive => {
            (affinity_positive(), affinity_positive_soft())
        }
        crate::domain::affinity::Affinity::Broken => (affinity_broken(), affinity_broken_soft()),
    }
}

pub const WINDOW_MIN_WIDTH_PX: f32 = 780.0;
pub const WINDOW_MIN_HEIGHT_PX: f32 = 680.0;
pub const SHELL_HORIZONTAL_PADDING_PX: f32 = 0.0;
pub const SHELL_GAP_PX: f32 = 0.0;
pub const SIDEBAR_WIDTH_PX: f32 = 200.0;
pub const MAIN_PANEL_MIN_WIDTH_PX: f32 = 560.0;
pub const RIGHT_PANE_WIDTH_PX: f32 = 320.0;
pub const RIGHT_PANE_RESIZE_HANDLE_WIDTH_PX: f32 = 4.0;
pub const CUSTOM_EMOJI_REACTION_SIZE_PX: f32 = 16.0;
pub const CUSTOM_EMOJI_INLINE_SIZE_PX: f32 = 20.0;
pub const CUSTOM_EMOJI_EMOJI_ONLY_SIZE_PX: f32 = 48.0;

pub fn tint(color: u32, opacity: f32) -> Hsla {
    Hsla::from(rgb(color)).opacity(opacity)
}

pub fn app_backdrop() -> Background {
    if is_dark_theme() {
        linear_gradient(
            135.,
            linear_color_stop(tint(0x11181f, 0.72), 0.0),
            linear_color_stop(tint(0x17212a, 0.78), 1.0),
        )
    } else {
        linear_gradient(
            135.,
            linear_color_stop(tint(0xf0f5f9, 0.62), 0.0),
            linear_color_stop(tint(0xeaeff5, 0.66), 1.0),
        )
    }
}

pub fn glass_surface() -> Hsla {
    if is_dark_theme() {
        tint(0x121b22, 0.55)
    } else {
        tint(0xffffff, 0.45)
    }
}

pub fn glass_surface_strong() -> Hsla {
    if is_dark_theme() {
        tint(0x212f3e, 0.46)
    } else {
        tint(0xffffff, 0.88)
    }
}

pub fn glass_surface_dark() -> Hsla {
    if is_dark_theme() {
        tint(0x111920, 0.92)
    } else {
        tint(0xffffff, 0.76)
    }
}

pub fn modal_surface() -> Hsla {
    if is_dark_theme() {
        tint(0x131a22, 0.99)
    } else {
        tint(0xf8f9fa, 0.99)
    }
}

pub fn content_surface() -> Hsla {
    if is_dark_theme() {
        tint(0x182330, 0.65)
    } else {
        tint(0xffffff, 0.82)
    }
}

pub fn subtle_surface() -> Hsla {
    tint(panel_alt_bg(), if is_dark_theme() { 0.88 } else { 0.68 })
}

pub fn shell_border() -> Hsla {
    if is_dark_theme() {
        tint(0x8ea3b7, 0.22)
    } else {
        tint(0x8ea3b7, 0.28)
    }
}

pub fn shell_border_strong() -> Hsla {
    if is_dark_theme() {
        tint(0x8ea3b7, 0.34)
    } else {
        tint(0x8ea3b7, 0.44)
    }
}

pub fn floating_shadow() -> Vec<BoxShadow> {
    if is_dark_theme() {
        vec![
            BoxShadow {
                color: tint(0x000000, 0.28),
                offset: point(px(0.), px(18.)),
                blur_radius: px(42.),
                spread_radius: px(0.),
            },
            BoxShadow {
                color: tint(0x000000, 0.14),
                offset: point(px(0.), px(6.)),
                blur_radius: px(18.),
                spread_radius: px(0.),
            },
        ]
    } else {
        vec![
            BoxShadow {
                color: tint(0x2b3440, 0.07),
                offset: point(px(0.), px(18.)),
                blur_radius: px(42.),
                spread_radius: px(0.),
            },
            BoxShadow {
                color: tint(0x2b3440, 0.03),
                offset: point(px(0.), px(6.)),
                blur_radius: px(18.),
                spread_radius: px(0.),
            },
        ]
    }
}

pub fn card_shadow() -> Vec<BoxShadow> {
    if is_dark_theme() {
        vec![
            BoxShadow {
                color: tint(0x000000, 0.20),
                offset: point(px(0.), px(10.)),
                blur_radius: px(24.),
                spread_radius: px(0.),
            },
            BoxShadow {
                color: tint(0x000000, 0.10),
                offset: point(px(0.), px(2.)),
                blur_radius: px(8.),
                spread_radius: px(0.),
            },
        ]
    } else {
        vec![
            BoxShadow {
                color: tint(0x2b3440, 0.05),
                offset: point(px(0.), px(10.)),
                blur_radius: px(24.),
                spread_radius: px(0.),
            },
            BoxShadow {
                color: tint(0x2b3440, 0.02),
                offset: point(px(0.), px(2.)),
                blur_radius: px(8.),
                spread_radius: px(0.),
            },
        ]
    }
}

pub fn badge(label: impl Into<SharedString>, background: u32, foreground: u32) -> AnyElement {
    div()
        .px_2p5()
        .py_1()
        .rounded_full()
        .bg(rgb(background))
        .text_color(rgb(foreground))
        .text_xs()
        .font_weight(FontWeight::MEDIUM)
        .child(label.into())
        .into_any_element()
}

pub fn attachment_kind_tag(kind: &AttachmentKind) -> &'static str {
    match kind {
        AttachmentKind::Image => "IMG",
        AttachmentKind::Video => "VID",
        AttachmentKind::Audio => "AUD",
        AttachmentKind::File => "FILE",
    }
}

pub fn attachment_size_text(size_bytes: u64) -> Option<String> {
    if size_bytes == 0 {
        return None;
    }
    if size_bytes < 1024 {
        return Some(format!("{size_bytes} B"));
    }
    const UNITS: &[&str] = &["KB", "MB", "GB", "TB"];
    let mut value = size_bytes as f64 / 1024.0;
    let mut unit_index = 0usize;
    while value >= 1024.0 && unit_index + 1 < UNITS.len() {
        value /= 1024.0;
        unit_index += 1;
    }
    let unit = UNITS[unit_index];
    if value >= 10.0 {
        Some(format!("{value:.0} {unit}"))
    } else {
        Some(format!("{value:.1} {unit}"))
    }
}

pub fn attachment_display_label(attachment: &AttachmentSummary) -> String {
    let prefix = attachment_kind_tag(&attachment.kind);
    if let Some(size_label) = attachment_size_text(attachment.size_bytes) {
        format!("[{prefix}] {} ({size_label})", attachment.name)
    } else {
        format!("[{prefix}] {}", attachment.name)
    }
}

pub fn attachment_image_source(attachment: &AttachmentSummary) -> Option<ImageSource> {
    attachment
        .preview
        .as_ref()
        .map(|preview| preview.source.clone())
        .or_else(|| attachment.source.as_ref().cloned())
        .and_then(|source| image_source_from_attachment_source(&source))
}

pub fn attachment_lightbox_source(attachment: &AttachmentSummary) -> Option<AttachmentSource> {
    attachment
        .source
        .as_ref()
        .cloned()
        .or_else(|| {
            attachment
                .preview
                .as_ref()
                .map(|preview| preview.source.clone())
        })
        .and_then(|source| {
            attachment_source_to_image_source(&source)
                .is_some()
                .then_some(source)
        })
}

pub fn image_source_from_attachment_source(source: &AttachmentSource) -> Option<ImageSource> {
    attachment_source_to_image_source(source)
}

pub fn attachment_open_target(attachment: &AttachmentSummary) -> Option<String> {
    attachment
        .source
        .as_ref()
        .or_else(|| attachment.preview.as_ref().map(|preview| &preview.source))
        .and_then(attachment_source_to_open_target)
}

fn attachment_source_to_image_source(source: &AttachmentSource) -> Option<ImageSource> {
    match source {
        AttachmentSource::Url(url) => {
            (!is_unrenderable_keybase_asset_url(url)).then(|| ImageSource::from(url.clone()))
        }
        AttachmentSource::LocalPath(path) => {
            let normalized = normalize_local_source_path(path);
            (!normalized.is_empty()).then(|| ImageSource::from(PathBuf::from(normalized)))
        }
    }
}

fn attachment_source_to_open_target(source: &AttachmentSource) -> Option<String> {
    match source {
        AttachmentSource::Url(url) => {
            (!is_unrenderable_keybase_asset_url(url)).then(|| url.clone())
        }
        AttachmentSource::LocalPath(_) => None,
    }
}

fn is_unrenderable_keybase_asset_url(url: &str) -> bool {
    let lower = url.to_ascii_lowercase();
    let prefix = if lower.starts_with("https://s3.amazonaws.com/") {
        "https://s3.amazonaws.com/"
    } else if lower.starts_with("http://s3.amazonaws.com/") {
        "http://s3.amazonaws.com/"
    } else {
        return false;
    };
    if lower.contains("x-amz-signature=") || lower.contains("awsaccesskeyid=") {
        return false;
    }
    let path = &url[prefix.len()..];
    let Some(first_segment) = path.split('/').next() else {
        return false;
    };
    first_segment.len() == 64 && first_segment.chars().all(|ch| ch.is_ascii_hexdigit())
}

pub fn normalize_local_source_path(raw: &str) -> String {
    let value = raw.trim();
    if value.is_empty() {
        return String::new();
    }
    let Some(path) = value.strip_prefix("file://") else {
        return value.to_string();
    };
    let normalized = if let Some(rest) = path.strip_prefix("localhost/") {
        format!("/{rest}")
    } else {
        path.to_string()
    };
    percent_decode(&normalized)
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = String::with_capacity(value.len());
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len()
            && let (Some(hi), Some(lo)) = (hex(bytes[index + 1]), hex(bytes[index + 2])) {
                out.push((hi << 4 | lo) as char);
                index += 3;
                continue;
            }
        out.push(bytes[index] as char);
        index += 1;
    }
    out
}

fn hex(ch: u8) -> Option<u8> {
    match ch {
        b'0'..=b'9' => Some(ch - b'0'),
        b'a'..=b'f' => Some(ch - b'a' + 10),
        b'A'..=b'F' => Some(ch - b'A' + 10),
        _ => None,
    }
}

fn icon_asset(path: &'static str, size: f32, color: u32) -> AnyElement {
    svg()
        .path(path)
        .w(px(size))
        .h(px(size))
        .min_w(px(size))
        .min_h(px(size))
        .flex_shrink_0()
        .text_color(rgb(color))
        .into_any_element()
}

pub fn home_icon(color: u32) -> AnyElement {
    icon_asset("assets/icons/home.svg", 18., color)
}

pub fn dm_icon(color: u32) -> AnyElement {
    icon_asset("assets/icons/dm.svg", 18., color)
}

pub fn activity_icon(color: u32) -> AnyElement {
    icon_asset("assets/icons/activity.svg", 18., color)
}

pub fn search_icon(color: u32) -> AnyElement {
    icon_asset("assets/icons/search.svg", 18., color)
}

pub fn sliders_icon(color: u32) -> AnyElement {
    icon_asset("assets/icons/sliders.svg", 18., color)
}

pub fn plus_icon(color: u32) -> AnyElement {
    icon_asset("assets/icons/plus.svg", 18., color)
}

pub fn emoji_icon(color: u32) -> AnyElement {
    icon_asset("assets/icons/emoji.svg", 18., color)
}

pub fn paperclip_icon(color: u32) -> AnyElement {
    icon_asset("assets/icons/paperclip.svg", 18., color)
}

pub fn mention_icon(color: u32) -> AnyElement {
    icon_asset("assets/icons/mention.svg", 18., color)
}

pub fn format_icon(color: u32) -> AnyElement {
    icon_asset("assets/icons/format.svg", 18., color)
}

pub fn link_icon(color: u32) -> AnyElement {
    icon_asset("assets/icons/link.svg", 18., color)
}

pub fn video_icon(color: u32) -> AnyElement {
    icon_asset("assets/icons/video.svg", 18., color)
}

pub fn members_icon(color: u32) -> AnyElement {
    icon_asset("assets/icons/members.svg", 18., color)
}

pub fn pin_icon(color: u32) -> AnyElement {
    icon_asset("assets/icons/pin.svg", 18., color)
}

pub fn files_icon(color: u32) -> AnyElement {
    icon_asset("assets/icons/files.svg", 18., color)
}

pub fn thread_icon(color: u32) -> AnyElement {
    icon_asset("assets/icons/thread.svg", 18., color)
}

pub fn call_icon(color: u32) -> AnyElement {
    icon_asset("assets/icons/call.svg", 18., color)
}

pub fn more_icon(color: u32) -> AnyElement {
    icon_asset("assets/icons/more-horizontal.svg", 16., color)
}

pub fn arrow_left_icon(color: u32) -> AnyElement {
    icon_asset("assets/icons/arrow-left.svg", 16., color)
}

pub fn arrow_right_icon(color: u32) -> AnyElement {
    icon_asset("assets/icons/arrow-right.svg", 16., color)
}

pub fn chevron_right_icon(color: u32) -> AnyElement {
    icon_asset("assets/icons/chevron-right.svg", 14., color)
}

pub fn chevron_down_icon(color: u32) -> AnyElement {
    icon_asset("assets/icons/chevron-down.svg", 14., color)
}

pub fn hash_icon(color: u32) -> AnyElement {
    icon_asset("assets/icons/hash.svg", 16., color)
}

pub fn at_icon(color: u32) -> AnyElement {
    icon_asset("assets/icons/at-sign.svg", 16., color)
}

pub fn close_icon(color: u32) -> AnyElement {
    icon_asset("assets/icons/x.svg", 14., color)
}

pub fn crown_icon(color: u32) -> AnyElement {
    icon_asset("assets/icons/crown.svg", 14., color)
}

pub fn send_icon(color: u32) -> AnyElement {
    icon_asset("assets/icons/send.svg", 16., color)
}

pub fn composer_tool(icon: AnyElement, color: u32) -> AnyElement {
    div()
        .w(px(28.))
        .h(px(28.))
        .rounded_md()
        .bg(subtle_surface())
        .border_1()
        .border_color(shell_border())
        .flex()
        .items_center()
        .justify_center()
        .child(div().text_color(rgb(color)).child(icon))
        .into_any_element()
}
