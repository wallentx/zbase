use crate::{
    domain::{
        message::LinkPreview,
        search::{SearchFilter, SearchResult},
    },
    models::search_model::SearchModel,
    util::formatting::message_timestamp_label,
    views::{
        accent, accent_soft,
        app_window::{AppWindow, video_preview_cache_key},
        badge, border,
        input::TextField,
        panel_alt_bg, panel_alt_surface, panel_bg, panel_surface, text_primary, text_secondary,
        tint,
    },
};
use gpui::prelude::FluentBuilder;
use gpui::{
    AnyElement, Context, Entity, FontWeight, ImageSource, InteractiveElement, IntoElement,
    ObjectFit, ParentElement, RenderImage, SharedString, StatefulInteractiveElement, Styled,
    StyledImage, div, img, px, rgb,
};
use std::{collections::HashMap, sync::Arc};

#[derive(Default)]
pub struct SearchView;

impl SearchView {
    pub fn render(
        &self,
        search: &SearchModel,
        video_render_cache: &HashMap<String, Arc<RenderImage>>,
        search_input: &Entity<TextField>,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        div()
            .flex_1()
            .m_4()
            .rounded_lg()
            .border_1()
            .border_color(rgb(border()))
            .bg(panel_surface())
            .p_6()
            .flex()
            .flex_col()
            .gap_4()
            .overflow_hidden()
            .child(div().font_weight(FontWeight::SEMIBOLD).child("Search"))
            .child(
                div()
                    .rounded_lg()
                    .border_1()
                    .border_color(rgb(border()))
                    .bg(panel_alt_surface())
                    .px_3()
                    .py_2p5()
                    .id("search-page-input")
                    .on_click(cx.listener(AppWindow::focus_search_input))
                    .child(search_input.clone()),
            )
            .child(
                div().flex().gap_2().children([
                    Self::filter_chip(
                        "From Alice",
                        search
                            .filters
                            .contains(&SearchFilter::FromUser("alice".to_string())),
                        cx,
                        SearchFilter::FromUser("alice".to_string()),
                    ),
                    Self::filter_chip(
                        "In #design",
                        search
                            .filters
                            .contains(&SearchFilter::InChannel("design".to_string())),
                        cx,
                        SearchFilter::InChannel("design".to_string()),
                    ),
                    Self::filter_chip(
                        "Has file",
                        search.filters.contains(&SearchFilter::HasFile),
                        cx,
                        SearchFilter::HasFile,
                    ),
                    Self::filter_chip(
                        "Mentions me",
                        search.filters.contains(&SearchFilter::MentionsMe),
                        cx,
                        SearchFilter::MentionsMe,
                    ),
                ]),
            )
            .child(
                div()
                    .rounded_lg()
                    .border_1()
                    .border_color(rgb(border()))
                    .p_4()
                    .text_sm()
                    .text_color(rgb(text_secondary()))
                    .child(format!(
                        "{} results{}",
                        search.results.len(),
                        if search.is_loading { " · loading" } else { "" }
                    )),
            )
            .child(
                div()
                    .flex_1()
                    .w_full()
                    .min_w(px(0.))
                    .id("search-results-scroll")
                    .overflow_y_scroll()
                    .scrollbar_width(px(8.))
                    .pr_2()
                    .when(search.results.is_empty(), |container| {
                        container.child(
                            div()
                                .rounded_lg()
                                .bg(panel_alt_surface())
                                .p_4()
                                .text_sm()
                                .text_color(rgb(text_secondary()))
                                .child("No results match this query yet."),
                        )
                    })
                    .children(search.results.iter().enumerate().map(|(index, result)| {
                        Self::render_result(
                            index,
                            result,
                            search.highlighted_index == Some(index),
                            video_render_cache,
                            cx,
                        )
                    })),
            )
            .into_any_element()
    }

    fn filter_chip(
        label: &'static str,
        active: bool,
        cx: &mut Context<AppWindow>,
        filter: SearchFilter,
    ) -> AnyElement {
        div()
            .id(SharedString::from(format!("search-filter-{label}")))
            .on_click(cx.listener(move |this, _, _, cx| {
                this.toggle_search_filter_click(filter.clone(), cx);
            }))
            .child(badge(
                label,
                if active { accent() } else { panel_alt_bg() },
                if active { panel_bg() } else { text_primary() },
            ))
            .into_any_element()
    }

    fn render_result(
        index: usize,
        result: &SearchResult,
        highlighted: bool,
        video_render_cache: &HashMap<String, Arc<RenderImage>>,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        let snippet = compact_snippet(&result.snippet, 160);

        div()
            .id(SharedString::from(format!("search-result-{index}")))
            .rounded_lg()
            .border_1()
            .border_color(rgb(if highlighted { accent() } else { border() }))
            .bg(if highlighted {
                tint(accent_soft(), 0.80)
            } else {
                panel_surface()
            })
            .p_4()
            .w_full()
            .min_w(px(0.))
            .overflow_hidden()
            .flex()
            .flex_col()
            .gap_1()
            .on_click(cx.listener(move |this, _, window, cx| {
                this.open_search_result_at(index, window, cx);
            }))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .child(
                        div()
                            .text_xs()
                            .text_color(rgb(text_secondary()))
                            .child(message_timestamp_label(result.message.timestamp_ms)),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(rgb(text_secondary()))
                            .child(format!(
                                "{} · {}",
                                result.route.label(),
                                result.conversation_id.0
                            )),
                    )
                    .when(result.message.thread_reply_count > 0, |d| {
                        d.child(badge(
                            format!("{} replies", result.message.thread_reply_count),
                            panel_alt_bg(),
                            text_primary(),
                        ))
                    }),
            )
            .child(
                div()
                    .w_full()
                    .min_w(px(0.))
                    .overflow_hidden()
                    .text_sm()
                    .font_weight(FontWeight::MEDIUM)
                    .child(snippet),
            )
            .when(!result.message.link_previews.is_empty(), |container| {
                container.child(render_link_previews(
                    index,
                    &result.message.link_previews,
                    video_render_cache,
                    cx,
                ))
            })
            .into_any_element()
    }
}

fn compact_snippet(raw: &str, max_chars: usize) -> String {
    let collapsed: String = raw
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" · ");
    if collapsed.chars().count() <= max_chars {
        collapsed
    } else {
        let truncated: String = collapsed.chars().take(max_chars).collect();
        format!("{truncated}…")
    }
}

fn render_link_previews(
    result_index: usize,
    previews: &[LinkPreview],
    video_render_cache: &HashMap<String, Arc<RenderImage>>,
    cx: &mut Context<AppWindow>,
) -> AnyElement {
    const MAX_VISIBLE: usize = 1;
    let hidden = previews.len().saturating_sub(MAX_VISIBLE);
    let visible = previews
        .iter()
        .take(MAX_VISIBLE)
        .cloned()
        .collect::<Vec<_>>();

    div()
        .flex()
        .flex_col()
        .items_start()
        .gap_1()
        .children(visible.into_iter().enumerate().map(|(index, preview)| {
            let url = preview.url.clone();
            let element_id =
                SharedString::from(format!("search-link-preview-{result_index}-{index}"));
            let image_element_id =
                SharedString::from(format!("search-link-preview-image-{result_index}-{index}"));
            let is_giphy = {
                let lower = preview.url.to_ascii_lowercase();
                lower.contains("giphy.com") || lower.contains("gph.is")
            };
            if preview.is_media {
                let media_source = preview
                    .thumbnail_asset
                    .clone()
                    .unwrap_or_else(|| preview.url.clone());
                let (media_width, media_height) =
                    media_frame_size(preview.media_width, preview.media_height, 260.0, 180.0);
                return div()
                    .id(element_id.clone())
                    .on_click(cx.listener(move |this, _, window, cx| {
                        this.open_url_or_deep_link(&url, window, cx);
                    }))
                    .cursor(gpui::CursorStyle::PointingHand)
                    .rounded_md()
                    .overflow_hidden()
                    .w(px(media_width))
                    .h(px(media_height))
                    .relative()
                    .child({
                        let decoded_video = if preview.is_video {
                            let key_url =
                                preview.video_url.as_deref().unwrap_or(preview.url.as_str());
                            let cache_key = video_preview_cache_key(key_url);
                            video_render_cache.get(&cache_key).cloned()
                        } else {
                            None
                        };
                        if let Some(render_image) = decoded_video {
                            img(ImageSource::Render(render_image))
                                .id(image_element_id.clone())
                                .w(px(media_width))
                                .h(px(media_height))
                                .object_fit(ObjectFit::Contain)
                        } else {
                            img(SharedString::from(media_source))
                                .id(image_element_id.clone())
                                .w(px(media_width))
                                .h(px(media_height))
                                .object_fit(ObjectFit::Contain)
                                .with_fallback({
                                    let site =
                                        preview.site.clone().unwrap_or_else(|| "media".to_string());
                                    move || {
                                        div()
                                            .text_xs()
                                            .text_color(rgb(text_secondary()))
                                            .child(site.clone())
                                            .into_any_element()
                                    }
                                })
                        }
                    })
                    .when(preview.is_video, |container| {
                        let key_url = preview.video_url.as_deref().unwrap_or(preview.url.as_str());
                        let cache_key = video_preview_cache_key(key_url);
                        let has_decoded_video = video_render_cache.contains_key(&cache_key);
                        if has_decoded_video {
                            container
                        } else {
                            container.child(
                                div()
                                    .absolute()
                                    .right_1()
                                    .bottom_1()
                                    .px_1()
                                    .py_0p5()
                                    .rounded_sm()
                                    .bg(panel_alt_surface())
                                    .border_1()
                                    .border_color(rgb(border()))
                                    .text_xs()
                                    .text_color(rgb(text_primary()))
                                .child("video"),
                            )
                        }
                    })
                    .into_any_element();
            }
            let site = preview.site.clone().unwrap_or_else(|| "link".to_string());
            let has_title = preview.title.is_some();
            let title = if is_giphy {
                preview
                    .title
                    .clone()
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or_else(|| "GIPHY".to_string())
            } else {
                preview.title.unwrap_or_else(|| preview.url.clone())
            };
            let thumbnail = preview.thumbnail_asset.clone();
            div()
                .id(element_id)
                .on_click(cx.listener(move |this, _, window, cx| {
                    this.open_url_or_deep_link(&url, window, cx);
                }))
                .cursor(gpui::CursorStyle::PointingHand)
                .rounded_md()
                .border_1()
                .border_color(rgb(border()))
                .bg(panel_alt_surface())
                .px_2()
                .py_1()
                .flex()
                .flex_col()
                .gap_0p5()
                .child(
                    div()
                        .text_xs()
                        .text_color(rgb(text_secondary()))
                        .child(site),
                )
                .child(div().text_sm().child(title))
                .when(has_title && !is_giphy, |container| {
                    container.child(div().text_xs().text_color(rgb(accent())).child(preview.url.clone()))
                })
                .when_some(thumbnail, |container, thumb_path| {
                    let (tw, th) = if let (Some(w), Some(h)) = (preview.media_width, preview.media_height)
                        && w > 0
                        && h > 0
                    {
                        let w = w as f32;
                        let h = h as f32;
                        let scale = (260.0 / w).min(150.0 / h).min(1.0);
                        (w * scale, h * scale)
                    } else {
                        (260.0, 150.0)
                    };
                    container.child(
                        img(ImageSource::from(std::path::PathBuf::from(thumb_path)))
                            .id(image_element_id)
                            .mt_0p5()
                            .w(px(tw))
                            .h(px(th))
                            .rounded_md()
                            .object_fit(ObjectFit::Contain),
                    )
                })
                .into_any_element()
        }))
        .when(hidden > 0, |container| {
            container.child(
                div()
                    .text_xs()
                    .text_color(rgb(text_secondary()))
                    .child(format!("+{} more links", hidden)),
            )
        })
        .into_any_element()
}

fn media_frame_size(
    width: Option<u32>,
    height: Option<u32>,
    max_width: f32,
    max_height: f32,
) -> (f32, f32) {
    let mut width = width.unwrap_or(240) as f32;
    let mut height = height.unwrap_or(160) as f32;
    if width <= 1.0 || height <= 1.0 {
        return (max_width.min(240.0), max_height.min(160.0));
    }
    let scale = (max_width / width).min(max_height / height).min(1.0);
    width *= scale;
    height *= scale;
    (width.max(96.0), height.max(72.0))
}
