use crate::views::{accent, panel_alt_bg, panel_surface};
use gpui::prelude::FluentBuilder;
use gpui::{
    AnyElement, ImageSource, IntoElement, ObjectFit, ParentElement, Styled, StyledImage, div, img,
    px, rgb,
};
use std::path::PathBuf;

pub const ME_AVATAR_ASSET: &str = "assets/avatars/me.svg";

pub struct Avatar;

impl Avatar {
    pub fn render(
        name: &str,
        asset: Option<&str>,
        size: f32,
        background: u32,
        foreground: u32,
    ) -> AnyElement {
        Self::render_shaped(name, asset, size, background, foreground, true)
    }

    pub fn render_square(
        name: &str,
        asset: Option<&str>,
        size: f32,
        background: u32,
        foreground: u32,
    ) -> AnyElement {
        Self::render_shaped(name, asset, size, background, foreground, false)
    }

    pub fn render_group(names: &[(&str, Option<&str>, u32, u32)], size: f32) -> AnyElement {
        if names.len() < 2 {
            if let Some((name, asset, bg, fg)) = names.first() {
                return Self::render(name, *asset, size, *bg, *fg);
            }
            return div().w(px(size)).h(px(size)).into_any_element();
        }

        let mini = size * 0.65;
        let offset = size - mini;

        let (name1, asset1, bg1, fg1) = &names[0];
        let (name2, asset2, bg2, fg2) = &names[1];

        div()
            .w(px(size))
            .h(px(size))
            .flex_shrink_0()
            .relative()
            .child(
                div()
                    .absolute()
                    .top_0()
                    .left_0()
                    .child(Self::render_shaped(name1, *asset1, mini, *bg1, *fg1, true)),
            )
            .child(
                div()
                    .absolute()
                    .top(px(offset))
                    .left(px(offset))
                    .child(Self::render_shaped(name2, *asset2, mini, *bg2, *fg2, true)),
            )
            .into_any_element()
    }

    fn render_shaped(
        name: &str,
        asset: Option<&str>,
        size: f32,
        background: u32,
        foreground: u32,
        circular: bool,
    ) -> AnyElement {
        match asset {
            Some(asset_path) => image_source_for_asset(asset_path)
                .map(|source| Self::image(name, source, size, background, foreground, circular))
                .unwrap_or_else(|| Self::fallback(name, size, background, foreground, circular)),
            None => Self::fallback(name, size, background, foreground, circular),
        }
    }

    pub fn fallback(
        name: &str,
        size: f32,
        background: u32,
        foreground: u32,
        circular: bool,
    ) -> AnyElement {
        let initials = initials_for_name(name);

        div()
            .w(px(size))
            .h(px(size))
            .when(circular, |d| d.rounded_full())
            .when(!circular, |d| d.rounded_lg())
            .bg(rgb(background))
            .text_color(rgb(foreground))
            .flex()
            .items_center()
            .justify_center()
            .text_sm()
            .child(initials)
            .into_any_element()
    }

    fn image(
        name: &str,
        image_source: ImageSource,
        size: f32,
        background: u32,
        foreground: u32,
        circular: bool,
    ) -> AnyElement {
        let fallback_name = name.to_string();

        div()
            .w(px(size))
            .h(px(size))
            .flex_shrink_0()
            .when(circular, |d| d.rounded_full())
            .when(!circular, |d| d.rounded_lg())
            .overflow_hidden()
            .bg(panel_surface())
            .child(
                img(image_source)
                    .w(px(size))
                    .h(px(size))
                    .when(circular, |d| d.rounded_full())
                    .when(!circular, |d| d.rounded_lg())
                    .object_fit(ObjectFit::Cover)
                    .with_fallback(move || {
                        Self::fallback(&fallback_name, size, background, foreground, circular)
                    }),
            )
            .into_any_element()
    }
}

pub fn demo_avatar_asset(name: &str) -> Option<&'static str> {
    match name {
        "Alice Johnson" => Some("assets/avatars/alice.svg"),
        "Sam Rivera" => Some("assets/avatars/sam.svg"),
        "You" => Some(ME_AVATAR_ASSET),
        _ => None,
    }
}

pub fn default_avatar_background(name: &str) -> u32 {
    match name {
        "Alice Johnson" => 0xe7eef7,
        "Sam Rivera" => 0xe7f2ee,
        "You" => accent(),
        _ => panel_alt_bg(),
    }
}

fn initials_for_name(name: &str) -> String {
    let mut parts = name
        .split_whitespace()
        .filter_map(|part| part.chars().next())
        .take(2)
        .collect::<String>();

    if parts.is_empty() {
        parts.push('?');
    }

    parts
}

fn image_source_for_asset(asset_path: &str) -> Option<ImageSource> {
    let trimmed = asset_path.trim();
    if trimmed.is_empty() {
        return None;
    }
    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with("http://") || lower.starts_with("https://") || lower.starts_with("assets/")
    {
        return Some(ImageSource::from(trimmed.to_string()));
    }
    let normalized = crate::views::normalize_local_source_path(trimmed);
    if normalized.is_empty() {
        return None;
    }
    Some(ImageSource::from(PathBuf::from(normalized)))
}
