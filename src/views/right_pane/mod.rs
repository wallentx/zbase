use crate::{
    domain::{
        affinity::Affinity,
        attachment::{AttachmentKind, AttachmentSource},
        backend::BackendCapabilities,
        ids::{MessageId, UserId},
        message::{BroadcastKind, LinkPreview, MessageFragment, MessageRecord, MessageSendState},
        presence::{Availability, Presence},
        route::Route,
        user::UserSummary,
    },
    models::{
        conversation_model::ConversationModel,
        navigation_model::{NavigationModel, RightPaneMode},
        profile_panel_model::ProfilePanelModel,
        search_model::SearchModel,
        thread_pane_model::ThreadPaneModel,
        timeline_model::{InlineEmojiRender, MessageRow, TimelineModel, TimelineRow},
    },
    util::formatting::message_timestamp_label,
    views::{
        CUSTOM_EMOJI_EMOJI_ONLY_SIZE_PX, CUSTOM_EMOJI_INLINE_SIZE_PX,
        RIGHT_PANE_RESIZE_HANDLE_WIDTH_PX, accent, accent_soft,
        app_window::{AppWindow, video_preview_cache_key},
        attachment_display_label, attachment_header_row, attachment_image_source,
        attachment_lightbox_source, attachment_local_path, attachment_open_target,
        avatar::{Avatar, default_avatar_background},
        badge, border, chevron_down_icon, close_icon, crown_icon, danger, danger_soft, emoji_icon,
        format_duration_ms, glass_surface_strong,
        inline_markdown::{InlineMarkdownConfig, apply_inline_markdown, remap_source_byte_range},
        input::TextField,
        is_dark_theme, mention_colors_for_user, mention_soft, mono_font_family, panel_alt_bg,
        panel_alt_surface, play_icon,
        selectable_text::{
            InlineAttachment, LinkRange, SelectableText, StyledRange, resolve_selectable_text,
            resolve_selectable_text_inline, resolve_selectable_text_with_attachments,
        },
        subtle_surface, text_primary, text_secondary,
        timeline::TimelineList,
        tint, warning,
    },
};
use gpui::prelude::FluentBuilder;
use gpui::{
    AnyElement, AppContext, Context, CursorStyle, Entity, ExternalPaths, FontWeight, ImageSource,
    InteractiveElement, IntoElement, MouseButton, ObjectFit, ParentElement, Render, RenderImage,
    ScrollHandle, SharedString, StatefulInteractiveElement, Styled, StyledImage, Window, div, img,
    px, rgb,
};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, OnceLock},
};

#[derive(Default)]
pub struct RightPaneHost;

#[derive(Clone, Debug)]
pub(crate) struct RightPaneResizeDrag;

#[derive(Default)]
struct RightPaneResizeDragPreview;

impl Render for RightPaneResizeDragPreview {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div().w(px(0.)).h(px(0.))
    }
}

#[derive(Default)]
pub struct ThreadPane;

impl RightPaneHost {
    pub fn render(
        &self,
        navigation: &NavigationModel,
        thread: &ThreadPaneModel,
        conversation: &ConversationModel,
        profile_panel: &ProfilePanelModel,
        search: &SearchModel,
        timeline: &TimelineModel,
        video_render_cache: &HashMap<String, Arc<RenderImage>>,
        failed_video_urls: &HashSet<String>,
        code_highlight_cache: &mut crate::views::code_highlight::CodeHighlightCache,
        selectable_texts: &mut HashMap<String, Entity<SelectableText>>,
        reply_input: &Entity<TextField>,
        thread_scroll: &ScrollHandle,
        profile_scroll: &ScrollHandle,
        profile_social_scroll: &ScrollHandle,
        unseen_reply_count: usize,
        capabilities: &BackendCapabilities,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        let content = match navigation.right_pane {
            RightPaneMode::Hidden => div().into_any_element(),
            RightPaneMode::Thread => ThreadPane.render(
                thread,
                timeline,
                video_render_cache,
                failed_video_urls,
                code_highlight_cache,
                selectable_texts,
                reply_input,
                thread_scroll,
                unseen_reply_count,
                cx,
            ),
            RightPaneMode::Details => {
                render_details_panel(conversation, timeline, capabilities, cx)
            }
            RightPaneMode::Members => render_members_panel(conversation, cx),
            RightPaneMode::Files => render_files_panel(timeline, cx),
            RightPaneMode::Search => render_search_panel(
                conversation,
                search,
                video_render_cache,
                failed_video_urls,
                cx,
            ),
            RightPaneMode::Profile(_) => crate::views::profile::render_profile_panel(
                profile_panel,
                profile_scroll,
                profile_social_scroll,
                cx,
            ),
        };

        div()
            // Use the resizable width for all right-pane modes (thread, profile, etc.)
            // so the resize handle always does something visible.
            .w(px(thread.width_px))
            .flex_shrink_0()
            .h_full()
            .flex()
            .overflow_hidden()
            .child(
                div()
                    .w(px(RIGHT_PANE_RESIZE_HANDLE_WIDTH_PX))
                    .h_full()
                    .bg(glass_surface_strong())
                    .cursor(CursorStyle::ResizeColumn)
                    .id("thread-pane-resize-handle")
                    .on_drag(RightPaneResizeDrag, |_, _, _, cx| {
                        cx.new(|_| RightPaneResizeDragPreview)
                    })
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(AppWindow::begin_thread_resize),
                    )
                    .flex()
                    .items_center()
                    .justify_center()
                    .group("resize-handle")
                    .child(
                        div()
                            .w(px(2.))
                            .h(px(640.))
                            .rounded_full()
                            .opacity(0.)
                            .group_hover("resize-handle", |s| s.opacity(1.))
                            .bg(rgb(border())),
                    ),
            )
            .child(
                div()
                    .flex_1()
                    .h_full()
                    .bg(glass_surface_strong())
                    .text_color(rgb(text_primary()))
                    .overflow_hidden()
                    .child(content),
            )
            .into_any_element()
    }
}

impl ThreadPane {
    pub fn render(
        &self,
        thread: &ThreadPaneModel,
        timeline: &TimelineModel,
        video_render_cache: &HashMap<String, Arc<RenderImage>>,
        failed_video_urls: &HashSet<String>,
        code_highlight_cache: &mut crate::views::code_highlight::CodeHighlightCache,
        selectable_texts: &mut HashMap<String, Entity<SelectableText>>,
        reply_input: &Entity<TextField>,
        thread_scroll: &ScrollHandle,
        unseen_reply_count: usize,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        let thread_rows = build_thread_message_rows(thread, timeline);
        let thread_content_width = (thread.width_px - RIGHT_PANE_RESIZE_HANDLE_WIDTH_PX).max(0.0);
        // Keep media within the usable message-content column in the thread pane:
        // row padding (32) + avatar/gap (44) + scrollbar gutter/right padding (32) + guard (8).
        let thread_media_max_width = (thread_content_width - 116.0).clamp(72.0, 280.0);

        div()
            .size_full()
            .flex()
            .flex_col()
            .overflow_hidden()
            .text_color(rgb(text_primary()))
            .drag_over::<ExternalPaths>(|style, _, _, _| {
                style
                    .bg(if is_dark_theme() {
                        tint(0x0b1117, 0.72)
                    } else {
                        tint(0xe8eef5, 0.70)
                    })
                    .border_2()
                    .border_color(rgb(accent()))
            })
            .on_drop(cx.listener(|this, paths: &ExternalPaths, _, cx| {
                let file_paths = paths.paths().to_vec();
                if !file_paths.is_empty() {
                    this.open_thread_file_upload_with_paths(file_paths, cx);
                }
            }))
            .child(
                div()
                    .px_4()
                    .py_4()
                    .flex()
                    .items_center()
                    .justify_between()
                    .child(
                        div()
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(rgb(text_primary()))
                            .child("Thread"),
                    )
                    .child(
                        div()
                            .id("thread-pane-close")
                            .w(px(22.))
                            .h(px(22.))
                            .rounded_full()
                            .flex()
                            .items_center()
                            .justify_center()
                            .hover(|s| s.bg(subtle_surface()))
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.close_right_pane(window, cx);
                            }))
                            .child(close_icon(text_secondary())),
                    ),
            )
            .child(
                div()
                    .flex_1()
                    .min_h(px(0.))
                    .id("thread-pane-scroll")
                    .overflow_y_scroll()
                    .scrollbar_width(px(8.))
                    .track_scroll(thread_scroll)
                    .on_scroll_wheel(cx.listener(AppWindow::thread_scrolled))
                    .pr_8()
                    .child(div().mt_4().pb_4().flex().flex_col().gap_1().children(
                        thread_rows.into_iter().map(|row| {
                            let timeline_row = TimelineRow::Message(row);
                            TimelineList::render_thread_row(
                                timeline,
                                &timeline_row,
                                thread_media_max_width,
                                video_render_cache,
                                failed_video_urls,
                                code_highlight_cache,
                                selectable_texts,
                                cx,
                            )
                        }),
                    )),
            )
            .when(unseen_reply_count > 0, |container| {
                container.child(
                    div().pb_2().flex().justify_center().child(
                        div()
                            .id("thread-jump-latest")
                            .on_click(cx.listener(AppWindow::scroll_thread_to_bottom))
                            .rounded_full()
                            .bg(rgb(accent()))
                            .px_3()
                            .py_1p5()
                            .flex()
                            .items_center()
                            .gap_2()
                            .text_xs()
                            .text_color(rgb(0xffffff))
                            .child(chevron_down_icon(0xffffff))
                            .child(format!(
                                "{} new repl{}",
                                unseen_reply_count,
                                if unseen_reply_count == 1 { "y" } else { "ies" }
                            )),
                    ),
                )
            })
            .child(
                div()
                    .mx_4()
                    .mb_4()
                    .flex_shrink_0()
                    .rounded_md()
                    .bg(panel_alt_surface())
                    .text_sm()
                    .line_height(px(22.))
                    .text_color(rgb(text_primary()))
                    .relative()
                    .id("thread-pane-input-surface")
                    .on_click(cx.listener(AppWindow::focus_thread_input))
                    .child(div().px_3().py_2().pr(px(36.)).child(reply_input.clone()))
                    .child(
                        div().absolute().bottom_1().right_1().child(
                            div()
                                .id("thread-emoji-inline")
                                .w(px(24.))
                                .h(px(24.))
                                .rounded_md()
                                .flex()
                                .items_center()
                                .justify_center()
                                .cursor(gpui::CursorStyle::PointingHand)
                                .hover(|s| s.bg(rgb(0x00000010)))
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.toggle_emoji_picker(cx);
                                }))
                                .child(emoji_icon(text_secondary())),
                        ),
                    ),
            )
            .into_any_element()
    }
}

fn resolve_thread_reply_author(
    timeline: &TimelineModel,
    author_id: &crate::domain::ids::UserId,
) -> (String, Option<String>) {
    for row in &timeline.rows {
        let TimelineRow::Message(message) = row else {
            continue;
        };
        if message.author.id == *author_id {
            return (
                message.author.display_name.clone(),
                message.author.avatar_asset.clone(),
            );
        }
    }
    (author_id.0.clone(), None)
}

fn plain_text_from_fragments(fragments: &[MessageFragment]) -> String {
    let mut text = String::new();
    for fragment in fragments {
        match fragment {
            MessageFragment::Text(value)
            | MessageFragment::InlineCode(value)
            | MessageFragment::Code { text: value, .. }
            | MessageFragment::Quote(value) => text.push_str(value),
            MessageFragment::Emoji { alias, .. } => text.push_str(&format!(":{alias}:")),
            MessageFragment::Mention(user_id) => text.push_str(&format!("@{}", user_id.0)),
            MessageFragment::ChannelMention { name } => text.push_str(&format!("#{name}")),
            MessageFragment::BroadcastMention(BroadcastKind::Here) => text.push_str("@here"),
            MessageFragment::BroadcastMention(BroadcastKind::All) => text.push_str("@channel"),
            MessageFragment::Link { display, .. } => text.push_str(display),
        }
    }
    text
}

fn render_parent_message(
    author_name: String,
    avatar_asset: Option<String>,
    message: MessageRecord,
    affinity_index: &HashMap<UserId, Affinity>,
    current_user_id: Option<&UserId>,
    emoji_index: &HashMap<String, InlineEmojiRender>,
    emoji_source_index: &HashMap<String, InlineEmojiRender>,
    video_render_cache: &HashMap<String, Arc<RenderImage>>,
    failed_video_urls: &HashSet<String>,
    selectable_texts: &mut HashMap<String, Entity<SelectableText>>,
    cx: &mut Context<AppWindow>,
) -> AnyElement {
    div()
        .mx_4()
        .mt_4()
        .p_3()
        .rounded_lg()
        .bg(panel_alt_surface())
        .flex()
        .gap_3()
        .items_start()
        .child(Avatar::render(
            &author_name,
            avatar_asset.as_deref(),
            28.,
            default_avatar_background(&author_name),
            text_primary(),
        ))
        .child(
            div()
                .flex()
                .flex_col()
                .gap_1()
                .child(
                    div()
                        .flex()
                        .items_center()
                        .gap_2()
                        .child(
                            div()
                                .font_weight(FontWeight::MEDIUM)
                                .text_color(rgb(text_primary()))
                                .child(author_name),
                        )
                        .child(
                            div()
                                .text_xs()
                                .text_color(rgb(text_secondary()))
                                .child(message_timestamp_label(message.timestamp_ms)),
                        ),
                )
                .child({
                    let image_attachment_message = !message.attachments.is_empty()
                        && message
                            .attachments
                            .iter()
                            .all(|attachment| attachment.kind == AttachmentKind::Image);
                    let media_link_only_message = !image_attachment_message
                        && crate::views::is_media_link_only_message(
                            &message.fragments,
                            &message.link_previews,
                        );
                    if image_attachment_message || media_link_only_message {
                        div().into_any_element()
                    } else {
                        render_message_fragments(
                            &message,
                            affinity_index,
                            current_user_id,
                            emoji_index,
                            emoji_source_index,
                            selectable_texts,
                            cx,
                        )
                    }
                })
                .when(!message.link_previews.is_empty(), |container| {
                    container.child(render_link_previews(
                        &message.id.0,
                        &message.link_previews,
                        video_render_cache,
                        failed_video_urls,
                        cx,
                    ))
                })
                .when(!message.attachments.is_empty(), |container| {
                    let image_attachment_message = !message.attachments.is_empty()
                        && message
                            .attachments
                            .iter()
                            .all(|attachment| attachment.kind == AttachmentKind::Image);
                    let caption_text = if image_attachment_message && !message.fragments.is_empty()
                    {
                        message_caption_text(&message)
                    } else {
                        None
                    };
                    container.child(
                        div().flex().flex_col().gap_1().children(
                            message
                                .attachments
                                .iter()
                                .enumerate()
                                .map(|(index, attachment)| {
                                    let attachment_label = attachment_display_label(attachment);
                                    let local_path = attachment_local_path(attachment);
                                    let header = {
                                        let base = attachment_header_row(attachment);
                                        if let Some(ref path) = local_path {
                                            let p = path.clone();
                                            let fname = attachment.name.clone();
                                            div()
                                                .id(SharedString::from(format!(
                                                    "rp-att-hdr-{}-{index}",
                                                    message.id.0
                                                )))
                                                .cursor(gpui::CursorStyle::PointingHand)
                                                .on_click(cx.listener(move |this, _, _, cx| {
                                                    this.save_attachment_copy(&p, &fname, cx);
                                                }))
                                                .child(base)
                                                .into_any_element()
                                        } else {
                                            base
                                        }
                                    };
                                    let mut card = div().flex().flex_col().gap_0p5().child(header);

                                    if attachment.kind == AttachmentKind::Image
                                        && let Some(media_source) =
                                            attachment_image_source(attachment)
                                    {
                                        let preview_width = attachment
                                            .preview
                                            .as_ref()
                                            .and_then(|preview| preview.width)
                                            .or(attachment.width);
                                        let preview_height = attachment
                                            .preview
                                            .as_ref()
                                            .and_then(|preview| preview.height)
                                            .or(attachment.height);
                                        let (media_width, media_height) = media_frame_size(
                                            preview_width,
                                            preview_height,
                                            260.0,
                                            220.0,
                                        );
                                        let lightbox_source =
                                            attachment_lightbox_source(attachment);
                                        let lightbox_caption = caption_text.clone();
                                        let lightbox_width = preview_width;
                                        let lightbox_height = preview_height;
                                        card = card.child(
                                            div()
                                                .id(SharedString::from(format!(
                                                    "right-pane-attachment-image-{}-{index}",
                                                    message.id.0
                                                )))
                                                .w(px(media_width))
                                                .h(px(media_height))
                                                .when_some(
                                                    lightbox_source,
                                                    |thumb, lightbox_source| {
                                                        let caption_text = lightbox_caption.clone();
                                                        thumb
                                                            .cursor(CursorStyle::PointingHand)
                                                            .on_click(cx.listener(
                                                                move |this, _, _, cx| {
                                                                    this.open_image_lightbox(
                                                                        lightbox_source.clone(),
                                                                        caption_text.clone(),
                                                                        lightbox_width,
                                                                        lightbox_height,
                                                                        cx,
                                                                    );
                                                                },
                                                            ))
                                                    },
                                                )
                                                .child(
                                                    img(media_source)
                                                        .id(SharedString::from(format!(
                                                            "rp-img-{}-{index}",
                                                            message.id.0
                                                        )))
                                                        .w(px(media_width))
                                                        .h(px(media_height))
                                                        .rounded_md()
                                                        .object_fit(ObjectFit::Contain)
                                                        .flex_shrink_0()
                                                        .min_w(px(16.))
                                                        .min_h(px(16.))
                                                        .with_fallback({
                                                            let label = attachment_label.clone();
                                                            move || {
                                                                div()
                                                                    .text_xs()
                                                                    .text_color(rgb(
                                                                        text_secondary(),
                                                                    ))
                                                                    .child(label.clone())
                                                                    .into_any_element()
                                                            }
                                                        }),
                                                ),
                                        );
                                        if let Some(ref caption) = caption_text {
                                            card = card.child(
                                                div()
                                                    .text_xs()
                                                    .text_color(rgb(text_secondary()))
                                                    .child(caption.clone()),
                                            );
                                        }
                                    } else if attachment.kind == AttachmentKind::Video
                                        && let Some(thumb_source) =
                                            attachment_image_source(attachment)
                                    {
                                        let preview_width = attachment
                                            .preview
                                            .as_ref()
                                            .and_then(|p| p.width)
                                            .or(attachment.width);
                                        let preview_height = attachment
                                            .preview
                                            .as_ref()
                                            .and_then(|p| p.height)
                                            .or(attachment.height);
                                        let (media_width, media_height) = media_frame_size(
                                            preview_width,
                                            preview_height,
                                            260.0,
                                            220.0,
                                        );
                                        let video_path =
                                            attachment.source.as_ref().and_then(|s| match s {
                                                AttachmentSource::LocalPath(p) => Some(p.clone()),
                                                _ => None,
                                            });
                                        let duration_label =
                                            attachment.duration_ms.map(format_duration_ms);
                                        card = card.child(
                                            div()
                                                .id(SharedString::from(format!(
                                                    "right-pane-attachment-video-{}-{index}",
                                                    message.id.0
                                                )))
                                                .relative()
                                                .w(px(media_width))
                                                .h(px(media_height))
                                                .rounded_md()
                                                .overflow_hidden()
                                                .flex_shrink_0()
                                                .cursor(CursorStyle::PointingHand)
                                                .when_some(video_path, |el, path| {
                                                    el.on_click(cx.listener(move |_, _, _, _| {
                                                        AppWindow::open_video_in_native_player(
                                                            &path,
                                                        );
                                                    }))
                                                })
                                                .child(
                                                    img(thumb_source)
                                                        .size_full()
                                                        .object_fit(ObjectFit::Cover),
                                                )
                                                .child(
                                                    div()
                                                        .absolute()
                                                        .inset_0()
                                                        .flex()
                                                        .items_center()
                                                        .justify_center()
                                                        .bg(tint(0x000000, 0.3))
                                                        .child(
                                                            div()
                                                                .w(px(40.))
                                                                .h(px(40.))
                                                                .rounded_full()
                                                                .bg(tint(0x000000, 0.5))
                                                                .flex()
                                                                .items_center()
                                                                .justify_center()
                                                                .child(play_icon(0xFFFFFF)),
                                                        ),
                                                )
                                                .when_some(duration_label, |el, label| {
                                                    el.child(
                                                        div()
                                                            .absolute()
                                                            .bottom_1()
                                                            .right_1()
                                                            .px_1p5()
                                                            .py_0p5()
                                                            .rounded_sm()
                                                            .bg(tint(0x000000, 0.6))
                                                            .text_xs()
                                                            .text_color(rgb(0xFFFFFF))
                                                            .child(label),
                                                    )
                                                }),
                                        );
                                    }

                                    card.into_any_element()
                                }),
                        ),
                    )
                }),
        )
        .into_any_element()
}

fn message_caption_text(message: &MessageRecord) -> Option<String> {
    let caption = message
        .fragments
        .iter()
        .map(|fragment| match fragment {
            MessageFragment::Text(text)
            | MessageFragment::Code { text, .. }
            | MessageFragment::Quote(text) => text.clone(),
            MessageFragment::InlineCode(text) => format!("`{text}`"),
            MessageFragment::Emoji { alias, .. } => format!(":{alias}:"),
            MessageFragment::Mention(user_id) => format!("@{}", user_id.0),
            MessageFragment::ChannelMention { name } => format!("#{name}"),
            MessageFragment::BroadcastMention(BroadcastKind::Here) => "@here".to_string(),
            MessageFragment::BroadcastMention(BroadcastKind::All) => "@channel".to_string(),
            MessageFragment::Link { display, .. } => display.clone(),
        })
        .collect::<Vec<_>>()
        .join("\n");
    let caption = caption.trim();
    (!caption.is_empty()).then(|| caption.to_string())
}

fn render_message_fragments(
    message: &MessageRecord,
    affinity_index: &HashMap<UserId, Affinity>,
    current_user_id: Option<&UserId>,
    emoji_index: &HashMap<String, InlineEmojiRender>,
    emoji_source_index: &HashMap<String, InlineEmojiRender>,
    selectable_texts: &mut HashMap<String, Entity<SelectableText>>,
    cx: &mut Context<AppWindow>,
) -> AnyElement {
    if message.fragments.is_empty() {
        return div().into_any_element();
    }
    let emoji_only = message_is_emoji_only(&message.fragments, !message.attachments.is_empty());
    let inline_layout = message.fragments.iter().all(|fragment| {
        matches!(
            fragment,
            MessageFragment::Text(_)
                | MessageFragment::InlineCode(_)
                | MessageFragment::Emoji { .. }
                | MessageFragment::Mention(_)
                | MessageFragment::ChannelMention { .. }
                | MessageFragment::BroadcastMention(_)
                | MessageFragment::Link { .. }
        )
    });
    if inline_layout {
        return render_inline_fragments(
            &message.id.0,
            format!("right-pane-{}-inline", message.id.0),
            &message.fragments,
            affinity_index,
            current_user_id,
            emoji_index,
            emoji_source_index,
            emoji_only,
            selectable_texts,
            cx,
        );
    }

    let mut rendered: Vec<AnyElement> = Vec::new();
    let mut inline_group: Vec<MessageFragment> = Vec::new();
    let mut group_ix = 0usize;

    for (index, fragment) in message.fragments.iter().enumerate() {
        let is_inline_fragment = matches!(
            fragment,
            MessageFragment::Text(_)
                | MessageFragment::InlineCode(_)
                | MessageFragment::Emoji { .. }
                | MessageFragment::Mention(_)
                | MessageFragment::ChannelMention { .. }
                | MessageFragment::BroadcastMention(_)
                | MessageFragment::Link { .. }
        );

        if is_inline_fragment {
            inline_group.push(fragment.clone());
            continue;
        }

        if !inline_group.is_empty() {
            let key = format!("right-pane-{}-inline-group-{}", message.id.0, group_ix);
            rendered.push(render_inline_fragments(
                &message.id.0,
                key,
                &inline_group,
                affinity_index,
                current_user_id,
                emoji_index,
                emoji_source_index,
                emoji_only,
                selectable_texts,
                cx,
            ));
            inline_group.clear();
            group_ix = group_ix.saturating_add(1);
        }

        rendered.push(render_fragment(
            &message.id.0,
            index,
            fragment,
            false,
            affinity_index,
            current_user_id,
            emoji_index,
            emoji_source_index,
            emoji_only,
            selectable_texts,
            cx,
        ));
    }
    if !inline_group.is_empty() {
        let key = format!("right-pane-{}-inline-group-{}", message.id.0, group_ix);
        rendered.push(render_inline_fragments(
            &message.id.0,
            key,
            &inline_group,
            affinity_index,
            current_user_id,
            emoji_index,
            emoji_source_index,
            emoji_only,
            selectable_texts,
            cx,
        ));
    }

    div()
        .flex()
        .flex_col()
        .gap_1()
        .children(rendered)
        .into_any_element()
}

fn render_link_previews(
    message_key: &str,
    previews: &[LinkPreview],
    video_render_cache: &HashMap<String, Arc<RenderImage>>,
    failed_video_urls: &HashSet<String>,
    cx: &mut Context<AppWindow>,
) -> AnyElement {
    const MAX_VISIBLE: usize = 2;
    let hidden = previews.len().saturating_sub(MAX_VISIBLE);
    let visible = previews
        .iter()
        .take(MAX_VISIBLE)
        .cloned()
        .collect::<Vec<_>>();

    div()
        .flex()
        .flex_col()
        .w_full()
        .min_w(px(0.))
        .items_start()
        .gap_1()
        .children(visible.into_iter().enumerate().map(|(index, preview)| {
            let url = preview.url.clone();
            let element_id =
                SharedString::from(format!("thread-link-preview-{message_key}-{index}"));
            let image_element_id =
                SharedString::from(format!("thread-link-preview-image-{message_key}-{index}"));
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
                    media_frame_size(preview.media_width, preview.media_height, 280.0, 200.0);
                return div()
                    .id(element_id)
                    .on_click(cx.listener(move |this, _, window, cx| {
                        this.open_url_or_deep_link(&url, window, cx);
                    }))
                    .cursor(CursorStyle::PointingHand)
                    .rounded_md()
                    .overflow_hidden()
                    .w_full()
                    .min_w(px(0.))
                    .max_w(px(media_width))
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
                                .size_full()
                                .rounded_md()
                                .object_fit(ObjectFit::Contain)
                                .flex_shrink_0()
                                .min_w(px(16.))
                                .min_h(px(16.))
                                .into_any_element()
                        } else {
                            let failed = failed_video_urls.contains(&media_source);
                            let image_source = (!failed).then(|| ImageSource::from(media_source));

                            div()
                                .size_full()
                                .when_some(image_source, |d, source| {
                                    d.child(
                                        img(source)
                                            .id(image_element_id.clone())
                                            .size_full()
                                            .rounded_md()
                                            .object_fit(ObjectFit::Contain)
                                            .flex_shrink_0()
                                            .min_w(px(16.))
                                            .min_h(px(16.))
                                            .with_fallback({
                                                let site = preview
                                                    .site
                                                    .clone()
                                                    .unwrap_or_else(|| "media".to_string());
                                                move || {
                                                    div()
                                                        .text_xs()
                                                        .text_color(rgb(text_secondary()))
                                                        .child(site.clone())
                                                        .into_any_element()
                                                }
                                            }),
                                    )
                                })
                                .when(failed, |d| {
                                    let site =
                                        preview.site.clone().unwrap_or_else(|| "media".to_string());
                                    d.child(
                                        div()
                                            .size_full()
                                            .flex()
                                            .items_center()
                                            .justify_center()
                                            .text_xs()
                                            .text_color(rgb(text_secondary()))
                                            .child(site),
                                    )
                                })
                                .into_any_element()
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
                .cursor(CursorStyle::PointingHand)
                .rounded_md()
                .border_1()
                .border_color(rgb(border()))
                .bg(panel_alt_surface())
                .w_full()
                .min_w(px(0.))
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
                .child(div().text_sm().text_color(rgb(text_primary())).child(title))
                .when(has_title && !is_giphy, |container| {
                    container.child(
                        div()
                            .text_xs()
                            .text_color(rgb(accent()))
                            .child(preview.url.clone()),
                    )
                })
                .when_some(thumbnail, |container, thumb_path| {
                    let (tw, th) = if let (Some(w), Some(h)) =
                        (preview.media_width, preview.media_height)
                        && w > 0
                        && h > 0
                    {
                        let w = w as f32;
                        let h = h as f32;
                        let scale = (280.0 / w).min(160.0 / h).min(1.0);
                        (w * scale, h * scale)
                    } else {
                        (280.0, 160.0)
                    };
                    container.child(
                        img(ImageSource::from(std::path::PathBuf::from(thumb_path)))
                            .id(image_element_id)
                            .mt_0p5()
                            .w_full()
                            .max_w(px(tw))
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
    let mut width = width.unwrap_or(260) as f32;
    let mut height = height.unwrap_or(180) as f32;
    if width <= 1.0 || height <= 1.0 {
        return (max_width.min(260.0), max_height.min(180.0));
    }
    let scale = (max_width / width).min(max_height / height).min(1.0);
    width *= scale;
    height *= scale;
    (width.max(100.0), height.max(80.0))
}

fn pane_header(title: &str, close_id: &'static str, cx: &mut Context<AppWindow>) -> AnyElement {
    div()
        .flex()
        .items_center()
        .justify_between()
        .child(
            div()
                .font_weight(FontWeight::SEMIBOLD)
                .text_color(rgb(text_primary()))
                .child(title.to_string()),
        )
        .child(
            div()
                .id(close_id)
                .on_click(cx.listener(|this, _, window, cx| {
                    this.close_right_pane(window, cx);
                }))
                .w(px(22.))
                .h(px(22.))
                .rounded_full()
                .flex()
                .items_center()
                .justify_center()
                .hover(|s| s.bg(subtle_surface()))
                .child(close_icon(text_secondary())),
        )
        .into_any_element()
}

fn render_details_panel(
    conversation: &ConversationModel,
    timeline: &TimelineModel,
    _capabilities: &BackendCapabilities,
    cx: &mut Context<AppWindow>,
) -> AnyElement {
    let details = conversation.details.as_ref();
    let shared_attachments = timeline
        .rows
        .iter()
        .filter_map(|row| match row {
            TimelineRow::Message(message) => Some(message.message.attachments.iter().cloned()),
            _ => None,
        })
        .flatten()
        .collect::<Vec<_>>();
    let files_count = shared_attachments.len();
    let member_count = details
        .map(|details| details.member_count)
        .unwrap_or(conversation.member_count);
    let members = details
        .map(|details| details.members.clone())
        .unwrap_or_default();
    let (bots, people): (Vec<_>, Vec<_>) = members
        .into_iter()
        .partition(|member| member.user_id.0.to_ascii_lowercase().ends_with("bot"));
    let has_members = !people.is_empty();
    let has_bots = !bots.is_empty();
    let member_count = if member_count == 0 {
        (people.len() + bots.len()) as u32
    } else {
        member_count
    };
    let group_name = details
        .and_then(|details| details.group.as_ref())
        .map(|group| group.display_name.clone())
        .or_else(|| {
            conversation
                .summary
                .group
                .as_ref()
                .map(|group| group.display_name.clone())
        });

    div()
        .size_full()
        .id("right-pane-details-scroll")
        .overflow_y_scroll()
        .scrollbar_width(px(8.))
        .p_4()
        .flex()
        .flex_col()
        .gap_4()
        .child(pane_header("Details", "details-pane-close", cx))
        .child(
            div()
                .rounded_lg()
                .bg(panel_alt_surface())
                .p_4()
                .flex()
                .flex_col()
                .gap_1()
                .child(
                    div()
                        .font_weight(FontWeight::SEMIBOLD)
                        .text_color(rgb(text_primary()))
                        .child(format!("#{}", conversation.summary.title)),
                )
                .when_some(group_name, |d, group| {
                    d.child(
                        div()
                            .text_sm()
                            .text_color(rgb(text_secondary()))
                            .child(group),
                    )
                }),
        )
        .when(has_members, |d| {
            d.child(
                div()
                    .rounded_lg()
                    .bg(panel_alt_surface())
                    .p_4()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(rgb(text_primary()))
                            .child(format!("Members ({member_count})")),
                    )
                    .children(people.into_iter().enumerate().map(|(index, member)| {
                        let member_user_id = member.user_id.clone();
                        let member_name = member.display_name.clone();
                        div()
                            .id(("details-member-entry", index))
                            .rounded_md()
                            .bg(rgb(panel_alt_bg()))
                            .px_2()
                            .py_2()
                            .flex()
                            .items_center()
                            .justify_between()
                            .cursor(CursorStyle::PointingHand)
                            .hover(|s| s.bg(subtle_surface()))
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.open_user_profile_panel(member_user_id.clone(), window, cx);
                            }))
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .gap_2()
                                    .child(Avatar::render(
                                        &member_name,
                                        member.avatar_asset.as_deref(),
                                        28.,
                                        default_avatar_background(&member_name),
                                        text_primary(),
                                    ))
                                    .child(
                                        div()
                                            .text_sm()
                                            .text_color(rgb(member_name_color(member.affinity)))
                                            .child(member_name),
                                    ),
                            )
                            .when(member.is_team_admin_or_owner, |row| {
                                row.child(
                                    div()
                                        .relative()
                                        .top(px(-1.))
                                        .flex()
                                        .items_center()
                                        .text_color(rgb(warning()))
                                        .child(crown_icon(warning())),
                                )
                            })
                            .into_any_element()
                    })),
            )
        })
        .when(has_bots, |d| {
            d.child(
                div()
                    .rounded_lg()
                    .bg(panel_alt_surface())
                    .p_4()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(rgb(text_primary()))
                            .child(format!("Bots ({})", bots.len())),
                    )
                    .children(bots.into_iter().enumerate().map(|(index, member)| {
                        let member_user_id = member.user_id.clone();
                        let member_name = member.display_name.clone();
                        div()
                            .id(("details-bot-entry", index))
                            .rounded_md()
                            .bg(rgb(panel_alt_bg()))
                            .px_2()
                            .py_2()
                            .flex()
                            .items_center()
                            .justify_between()
                            .cursor(CursorStyle::PointingHand)
                            .hover(|s| s.bg(subtle_surface()))
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.open_user_profile_panel(member_user_id.clone(), window, cx);
                            }))
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .gap_2()
                                    .child(Avatar::render(
                                        &member_name,
                                        member.avatar_asset.as_deref(),
                                        28.,
                                        default_avatar_background(&member_name),
                                        text_primary(),
                                    ))
                                    .child(
                                        div()
                                            .text_sm()
                                            .text_color(rgb(member_name_color(member.affinity)))
                                            .child(member_name),
                                    ),
                            )
                            .when(member.is_team_admin_or_owner, |row| {
                                row.child(
                                    div()
                                        .relative()
                                        .top(px(-1.))
                                        .flex()
                                        .items_center()
                                        .text_color(rgb(warning()))
                                        .child(crown_icon(warning())),
                                )
                            })
                            .into_any_element()
                    })),
            )
        })
        .when(
            !has_members
                && matches!(
                    conversation.summary.kind,
                    crate::domain::conversation::ConversationKind::Channel
                ),
            |d| {
                d.child(
                    div()
                        .rounded_lg()
                        .bg(panel_alt_surface())
                        .p_4()
                        .flex()
                        .flex_col()
                        .gap_2()
                        .child(
                            div()
                                .text_sm()
                                .font_weight(FontWeight::SEMIBOLD)
                                .text_color(rgb(text_primary()))
                                .child("Members"),
                        )
                        .child(
                            div()
                                .rounded_md()
                                .bg(rgb(panel_alt_bg()))
                                .px_2()
                                .py_2()
                                .text_sm()
                                .text_color(rgb(text_secondary()))
                                .child("Loading members…"),
                        ),
                )
            },
        )
        .child(
            div()
                .rounded_lg()
                .bg(panel_alt_surface())
                .p_4()
                .flex()
                .flex_col()
                .gap_2()
                .child(
                    div()
                        .text_sm()
                        .font_weight(FontWeight::SEMIBOLD)
                        .text_color(rgb(text_primary()))
                        .child(format!("Shared files ({files_count})")),
                )
                .when(files_count == 0, |section| {
                    section.child(
                        div()
                            .rounded_md()
                            .bg(rgb(panel_alt_bg()))
                            .px_2()
                            .py_1p5()
                            .text_xs()
                            .text_color(rgb(text_secondary()))
                            .child("No files shared yet."),
                    )
                })
                .children(
                    shared_attachments
                        .into_iter()
                        .enumerate()
                        .map(|(index, attachment)| {
                            let open_target = attachment_open_target(&attachment);
                            let fallback_open_target = attachment
                                .source
                                .as_ref()
                                .or_else(|| {
                                    attachment.preview.as_ref().map(|preview| &preview.source)
                                })
                                .and_then(|source| match source {
                                    AttachmentSource::Url(url) => Some(url.clone()),
                                    AttachmentSource::LocalPath(_) => None,
                                });
                            let save_source = attachment
                                .source
                                .as_ref()
                                .or_else(|| {
                                    attachment.preview.as_ref().map(|preview| &preview.source)
                                })
                                .and_then(|source| match source {
                                    AttachmentSource::LocalPath(path) => Some(path.clone()),
                                    _ => None,
                                });
                            let file_name = if attachment.name.trim().is_empty() {
                                "attachment".to_string()
                            } else {
                                attachment.name.clone()
                            };
                            let truncated_name = truncate_with_ellipsis(&file_name, 56);
                            div()
                                .id(("details-shared-file", index))
                                .rounded_md()
                                .bg(rgb(panel_alt_bg()))
                                .px_2()
                                .py_1p5()
                                .flex()
                                .items_center()
                                .min_w(px(0.))
                                .cursor(CursorStyle::PointingHand)
                                .hover(|s| s.bg(subtle_surface()))
                                .on_click(cx.listener(move |this, _, window, cx| {
                                    if let Some(source_path) = save_source.clone() {
                                        this.save_attachment_copy(&source_path, &file_name, cx);
                                    } else if let Some(target) =
                                        open_target.clone().or_else(|| fallback_open_target.clone())
                                    {
                                        if target.starts_with("http://")
                                            || target.starts_with("https://")
                                        {
                                            this.download_attachment_and_open(
                                                &target, &file_name, cx,
                                            );
                                        } else {
                                            this.open_url_or_deep_link(&target, window, cx);
                                        }
                                    } else {
                                        this.notify_attachment_not_ready(cx);
                                    }
                                }))
                                .child(
                                    div()
                                        .id(("details-shared-file-open", index))
                                        .flex_1()
                                        .min_w(px(0.))
                                        .overflow_hidden()
                                        .text_xs()
                                        .text_color(rgb(accent()))
                                        .child(truncated_name),
                                )
                                .into_any_element()
                        }),
                ),
        )
        .into_any_element()
}

fn member_name_color(affinity: Affinity) -> u32 {
    match affinity {
        Affinity::None => text_primary(),
        Affinity::Positive => crate::views::affinity_positive(),
        Affinity::Broken => crate::views::affinity_broken(),
    }
}

fn truncate_with_ellipsis(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    let mut truncated = value.chars().take(max_chars).collect::<String>();
    truncated.push('…');
    truncated
}

fn render_members_panel(
    conversation: &ConversationModel,
    cx: &mut Context<AppWindow>,
) -> AnyElement {
    let details = conversation.details.as_ref();
    let members = details.map(|d| d.members.as_slice()).unwrap_or(&[]);
    let (bots, people): (Vec<_>, Vec<_>) = members
        .iter()
        .cloned()
        .partition(|member| member.user_id.0.to_ascii_lowercase().ends_with("bot"));

    div()
        .size_full()
        .id("right-pane-members-scroll")
        .overflow_y_scroll()
        .scrollbar_width(px(8.))
        .p_4()
        .flex()
        .flex_col()
        .gap_3()
        .child(pane_header("Members", "members-pane-close", cx))
        .when(members.is_empty(), |container| {
            container.child(
                div()
                    .rounded_lg()
                    .bg(panel_alt_surface())
                    .p_4()
                    .text_sm()
                    .text_color(rgb(text_secondary()))
                    .child("No member list available yet."),
            )
        })
        .children(people.into_iter().enumerate().map(|(index, member)| {
            let user_id = member.user_id.clone();
            let label = member.display_name.clone();
            div()
                .id(("members-pane-row", index))
                .rounded_lg()
                .bg(panel_alt_surface())
                .px_3()
                .py_2()
                .flex()
                .items_center()
                .gap_2()
                .cursor(CursorStyle::PointingHand)
                .on_click(cx.listener(move |this, _, _, cx| {
                    this.open_user_profile_card(user_id.clone(), cx);
                }))
                .child(Avatar::render(
                    &label,
                    member.avatar_asset.as_deref(),
                    24.,
                    default_avatar_background(&label),
                    text_primary(),
                ))
                .child(
                    div()
                        .flex()
                        .items_center()
                        .gap_1()
                        .text_sm()
                        .text_color(rgb(member_name_color(member.affinity)))
                        .child(label),
                )
                .when(member.is_team_admin_or_owner, |row| {
                    row.child(
                        div()
                            .relative()
                            .top(px(-1.))
                            .flex()
                            .items_center()
                            .text_color(rgb(warning()))
                            .child(crown_icon(warning())),
                    )
                })
                .into_any_element()
        }))
        .when(!bots.is_empty(), |container| {
            container
                .child(
                    div()
                        .pt_2()
                        .text_sm()
                        .font_weight(FontWeight::SEMIBOLD)
                        .text_color(rgb(text_primary()))
                        .child(format!("Bots ({})", bots.len())),
                )
                .children(bots.into_iter().enumerate().map(|(index, member)| {
                    let user_id = member.user_id.clone();
                    let label = member.display_name.clone();
                    div()
                        .id(("members-pane-bot-row", index))
                        .rounded_lg()
                        .bg(panel_alt_surface())
                        .px_3()
                        .py_2()
                        .flex()
                        .items_center()
                        .gap_2()
                        .cursor(CursorStyle::PointingHand)
                        .on_click(cx.listener(move |this, _, _, cx| {
                            this.open_user_profile_card(user_id.clone(), cx);
                        }))
                        .child(Avatar::render(
                            &label,
                            member.avatar_asset.as_deref(),
                            24.,
                            default_avatar_background(&label),
                            text_primary(),
                        ))
                        .child(
                            div()
                                .flex()
                                .items_center()
                                .gap_1()
                                .text_sm()
                                .text_color(rgb(member_name_color(member.affinity)))
                                .child(label),
                        )
                        .when(member.is_team_admin_or_owner, |row| {
                            row.child(
                                div()
                                    .relative()
                                    .top(px(-1.))
                                    .flex()
                                    .items_center()
                                    .text_color(rgb(warning()))
                                    .child(crown_icon(warning())),
                            )
                        })
                        .into_any_element()
                }))
        })
        .into_any_element()
}

fn render_files_panel(timeline: &TimelineModel, cx: &mut Context<AppWindow>) -> AnyElement {
    let attachments = timeline
        .rows
        .iter()
        .filter_map(|row| match row {
            TimelineRow::Message(message) => Some(message.message.attachments.iter().cloned()),
            _ => None,
        })
        .flatten()
        .collect::<Vec<_>>();

    div()
        .size_full()
        .id("right-pane-files-scroll")
        .overflow_y_scroll()
        .scrollbar_width(px(8.))
        .p_4()
        .flex()
        .flex_col()
        .gap_3()
        .child(pane_header("Files", "files-pane-close", cx))
        .when(attachments.is_empty(), |container| {
            container.child(
                div()
                    .rounded_lg()
                    .bg(panel_alt_surface())
                    .p_4()
                    .text_sm()
                    .text_color(rgb(text_secondary()))
                    .child("No files yet in this conversation."),
            )
        })
        .children(
            attachments
                .into_iter()
                .enumerate()
                .map(|(index, attachment)| {
                    let attachment_label = attachment_display_label(&attachment);
                    div()
                        .id(("right-pane-file", index))
                        .rounded_lg()
                        .bg(panel_alt_surface())
                        .px_3()
                        .py_2()
                        .flex()
                        .items_center()
                        .justify_between()
                        .child(attachment_label)
                        .into_any_element()
                }),
        )
        .into_any_element()
}

fn render_search_panel(
    conversation: &ConversationModel,
    search: &SearchModel,
    _video_render_cache: &HashMap<String, Arc<RenderImage>>,
    _failed_video_urls: &HashSet<String>,
    cx: &mut Context<AppWindow>,
) -> AnyElement {
    let scoped_results = search
        .results
        .iter()
        .enumerate()
        .filter(|(_, result)| match &result.route {
            Route::Channel { channel_id, .. } => {
                (conversation.summary.title == "general" && channel_id.0 == "general")
                    || (conversation.summary.title == "design" && channel_id.0 == "design")
            }
            Route::DirectMessage { .. } => conversation.summary.title == "Alice Johnson",
            _ => false,
        })
        .collect::<Vec<_>>();

    div()
        .size_full()
        .id("right-pane-search-scroll")
        .overflow_y_scroll()
        .scrollbar_width(px(8.))
        .p_4()
        .flex()
        .flex_col()
        .gap_3()
        .child(pane_header("Search", "search-pane-close", cx))
        .child(
            div()
                .text_sm()
                .text_color(rgb(text_secondary()))
                .child(format!("Scoped to {}", conversation.summary.title)),
        )
        .children(scoped_results.into_iter().map(|(result_index, result)| {
            div()
                .id(("search-pane-result", result_index))
                .rounded_lg()
                .bg(panel_alt_surface())
                .px_3()
                .py_2()
                .flex()
                .flex_col()
                .gap_1()
                .on_click(cx.listener(move |this, _, window, cx| {
                    this.open_search_result_at(result_index, window, cx);
                }))
                .child(result.snippet.clone())
                .child(
                    div()
                        .text_xs()
                        .text_color(rgb(text_secondary()))
                        .child(message_timestamp_label(result.message.timestamp_ms)),
                )
                .into_any_element()
        }))
        .into_any_element()
}

fn find_message(
    timeline: &TimelineModel,
    root_id: &crate::domain::ids::MessageId,
) -> Option<(String, Option<String>, MessageRecord)> {
    timeline.rows.iter().find_map(|row| {
        let TimelineRow::Message(message_row) = row else {
            return None;
        };
        (&message_row.message.id == root_id).then_some((
            message_row.author.display_name.clone(),
            message_row.author.avatar_asset.clone(),
            message_row.message.clone(),
        ))
    })
}

fn render_inline_fragments(
    _message_key: &str,
    render_key: String,
    fragments: &[MessageFragment],
    affinity_index: &HashMap<UserId, Affinity>,
    current_user_id: Option<&UserId>,
    emoji_index: &HashMap<String, InlineEmojiRender>,
    emoji_source_index: &HashMap<String, InlineEmojiRender>,
    emoji_only: bool,
    selectable_texts: &mut HashMap<String, Entity<SelectableText>>,
    cx: &mut Context<AppWindow>,
) -> AnyElement {
    let mut combined = String::new();
    let mut link_ranges = Vec::new();
    let mut styled_ranges = Vec::new();
    let mut inline_attachments = Vec::new();

    for fragment in fragments {
        match fragment {
            MessageFragment::Text(text) => {
                for segment in inline_text_segments(text) {
                    match segment {
                        InlineTextSegment::Text(value) => combined.push_str(&value),
                        InlineTextSegment::InlineCode(code) => {
                            push_inline_code_span(&mut combined, &mut styled_ranges, &code);
                        }
                        InlineTextSegment::Link(url) => {
                            let start = combined.len();
                            combined.push_str(&url);
                            link_ranges.push(LinkRange {
                                byte_range: start..combined.len(),
                                url,
                            });
                        }
                    }
                }
            }
            MessageFragment::Code { text, .. } | MessageFragment::Quote(text) => {
                combined.push_str(text);
            }
            MessageFragment::InlineCode(code) => {
                push_inline_code_span(&mut combined, &mut styled_ranges, code);
            }
            MessageFragment::Emoji { alias, source_ref } => {
                if let Some(render) = resolved_emoji_render(
                    alias,
                    source_ref.as_ref(),
                    emoji_index,
                    emoji_source_index,
                ) {
                    if let Some(unicode) = render.unicode.as_ref() {
                        combined.push_str(unicode);
                        continue;
                    }
                    if let Some(asset_path) = render.asset_path.as_ref() {
                        push_inline_asset_emoji_attachment(
                            &mut combined,
                            &mut inline_attachments,
                            alias,
                            asset_path,
                            emoji_only,
                        );
                        continue;
                    }
                }
                combined.push_str(&inline_emoji_text(
                    alias,
                    source_ref.as_ref(),
                    emoji_index,
                    emoji_source_index,
                ));
            }
            MessageFragment::Mention(user_id) => {
                let label = format!("@{}", user_id.0);
                let start = combined.len();
                let (foreground, background) =
                    mention_colors_for_user(affinity_index, current_user_id, user_id);
                push_styled_span(
                    &mut combined,
                    &mut styled_ranges,
                    &label,
                    foreground,
                    background,
                );
                link_ranges.push(LinkRange {
                    byte_range: start..combined.len(),
                    url: format!("zbase-mention:{}", user_id.0),
                });
            }
            MessageFragment::ChannelMention { name } => {
                let label = format!("#{name}");
                let start = combined.len();
                push_styled_span(
                    &mut combined,
                    &mut styled_ranges,
                    &label,
                    accent(),
                    accent_soft(),
                );
                link_ranges.push(LinkRange {
                    byte_range: start..combined.len(),
                    url: format!("zbase-channel:{name}"),
                });
            }
            MessageFragment::BroadcastMention(BroadcastKind::Here) => {
                push_styled_span(
                    &mut combined,
                    &mut styled_ranges,
                    "@here",
                    danger(),
                    danger_soft(),
                );
            }
            MessageFragment::BroadcastMention(BroadcastKind::All) => {
                push_styled_span(
                    &mut combined,
                    &mut styled_ranges,
                    "@channel",
                    danger(),
                    danger_soft(),
                );
            }
            MessageFragment::Link { url, display } => {
                let start = combined.len();
                combined.push_str(display);
                link_ranges.push(LinkRange {
                    byte_range: start..combined.len(),
                    url: url.clone(),
                });
            }
        }
    }

    let parsed = apply_inline_markdown(
        &combined,
        &link_ranges,
        &styled_ranges,
        InlineMarkdownConfig {
            spoiler_foreground: Some(text_secondary()),
            spoiler_background: Some(panel_alt_bg()),
        },
    );
    let remapped_inline_attachments = inline_attachments
        .into_iter()
        .filter_map(|attachment| {
            remap_source_byte_range(
                &parsed.source_to_output_mapping,
                combined.len(),
                &attachment.byte_range,
            )
            .map(|byte_range| InlineAttachment {
                byte_range,
                ..attachment
            })
        })
        .collect::<Vec<_>>();

    div()
        .w_full()
        .min_w(px(0.))
        .text_color(rgb(text_primary()))
        .when(emoji_only, |container| {
            container.text_size(px(48.)).line_height(px(52.))
        })
        .when(!emoji_only, |container| {
            container.text_sm().line_height(px(22.))
        })
        .child(resolve_selectable_text_with_attachments(
            selectable_texts,
            render_key,
            parsed.text,
            parsed.link_ranges,
            parsed.styled_ranges,
            remapped_inline_attachments,
            cx,
        ))
        .into_any_element()
}

fn render_inline_fragments_with_asset_emojis(
    message_key: &str,
    fragments: &[MessageFragment],
    affinity_index: &HashMap<UserId, Affinity>,
    current_user_id: Option<&UserId>,
    emoji_index: &HashMap<String, InlineEmojiRender>,
    emoji_source_index: &HashMap<String, InlineEmojiRender>,
    emoji_only: bool,
    selectable_texts: &mut HashMap<String, Entity<SelectableText>>,
    cx: &mut Context<AppWindow>,
) -> AnyElement {
    let mut combined = String::new();
    let mut link_ranges = Vec::new();
    let mut styled_ranges = Vec::new();
    let mut asset_emojis: Vec<AnyElement> = Vec::new();

    for fragment in fragments {
        if fragment_requires_asset_emoji_box(fragment, emoji_index, emoji_source_index) {
            let MessageFragment::Emoji { alias, source_ref } = fragment else {
                continue;
            };
            if let Some(render) =
                resolved_emoji_render(alias, source_ref.as_ref(), emoji_index, emoji_source_index)
                && let Some(asset_path) = render.asset_path.as_ref()
            {
                let size = custom_emoji_size_px(emoji_only);
                asset_emojis.push(
                    div()
                        .w(px(size))
                        .h(px(size))
                        .flex_shrink_0()
                        .overflow_hidden()
                        .child(
                            img(ImageSource::from(std::path::PathBuf::from(
                                crate::views::normalize_local_source_path(asset_path),
                            )))
                            .w(px(size))
                            .h(px(size))
                            .object_fit(ObjectFit::Contain)
                            .with_fallback({
                                let alias = alias.clone();
                                move || {
                                    div()
                                        .text_sm()
                                        .line_height(px(22.))
                                        .child(format!(":{alias}:"))
                                        .into_any_element()
                                }
                            }),
                        )
                        .into_any_element(),
                );
            }
            continue;
        }

        match fragment {
            MessageFragment::Text(text) => {
                for segment in inline_text_segments(text) {
                    match segment {
                        InlineTextSegment::Text(value) => combined.push_str(&value),
                        InlineTextSegment::InlineCode(code) => {
                            push_inline_code_span(&mut combined, &mut styled_ranges, &code);
                        }
                        InlineTextSegment::Link(url) => {
                            let start = combined.len();
                            combined.push_str(&url);
                            link_ranges.push(LinkRange {
                                byte_range: start..combined.len(),
                                url,
                            });
                        }
                    }
                }
            }
            MessageFragment::InlineCode(code) => {
                push_inline_code_span(&mut combined, &mut styled_ranges, code);
            }
            MessageFragment::Emoji { alias, source_ref } => {
                combined.push_str(&inline_emoji_text(
                    alias,
                    source_ref.as_ref(),
                    emoji_index,
                    emoji_source_index,
                ));
            }
            MessageFragment::Mention(user_id) => {
                let label = format!("@{}", user_id.0);
                let start = combined.len();
                let (foreground, background) =
                    mention_colors_for_user(affinity_index, current_user_id, user_id);
                push_styled_span(
                    &mut combined,
                    &mut styled_ranges,
                    &label,
                    foreground,
                    background,
                );
                link_ranges.push(LinkRange {
                    byte_range: start..combined.len(),
                    url: format!("zbase-mention:{}", user_id.0),
                });
            }
            MessageFragment::ChannelMention { name } => {
                let label = format!("#{name}");
                push_styled_span(
                    &mut combined,
                    &mut styled_ranges,
                    &label,
                    accent(),
                    accent_soft(),
                );
            }
            MessageFragment::BroadcastMention(BroadcastKind::Here) => {
                push_styled_span(
                    &mut combined,
                    &mut styled_ranges,
                    "@here",
                    danger(),
                    danger_soft(),
                );
            }
            MessageFragment::BroadcastMention(BroadcastKind::All) => {
                push_styled_span(
                    &mut combined,
                    &mut styled_ranges,
                    "@channel",
                    danger(),
                    danger_soft(),
                );
            }
            MessageFragment::Link { url, display } => {
                let start = combined.len();
                combined.push_str(display);
                link_ranges.push(LinkRange {
                    byte_range: start..combined.len(),
                    url: url.clone(),
                });
            }
            MessageFragment::Code { text, .. } | MessageFragment::Quote(text) => {
                combined.push_str(text);
            }
        }
    }

    let parsed = apply_inline_markdown(
        &combined,
        &link_ranges,
        &styled_ranges,
        InlineMarkdownConfig {
            spoiler_foreground: Some(text_secondary()),
            spoiler_background: Some(panel_alt_bg()),
        },
    );

    let has_text = !parsed.text.is_empty();

    if asset_emojis.is_empty() {
        div()
            .w_full()
            .min_w(px(0.))
            .text_color(rgb(text_primary()))
            .when(emoji_only, |container| {
                container.text_size(px(48.)).line_height(px(52.))
            })
            .when(!emoji_only, |container| {
                container.text_sm().line_height(px(22.))
            })
            .child(resolve_selectable_text(
                selectable_texts,
                format!("right-pane-{message_key}-inline-hybrid"),
                parsed.text,
                parsed.link_ranges,
                parsed.styled_ranges,
                cx,
            ))
            .into_any_element()
    } else if !has_text {
        div()
            .flex()
            .items_center()
            .gap_1()
            .children(asset_emojis)
            .into_any_element()
    } else {
        let text_el = div()
            .flex_1()
            .min_w(px(0.))
            .text_color(rgb(text_primary()))
            .when(emoji_only, |container| {
                container.text_size(px(48.)).line_height(px(52.))
            })
            .when(!emoji_only, |container| {
                container.text_sm().line_height(px(22.))
            })
            .child(resolve_selectable_text(
                selectable_texts,
                format!("right-pane-{message_key}-inline-hybrid"),
                parsed.text,
                parsed.link_ranges,
                parsed.styled_ranges,
                cx,
            ))
            .into_any_element();

        let mut children = Vec::with_capacity(1 + asset_emojis.len());
        children.push(text_el);
        children.extend(asset_emojis);
        div()
            .flex()
            .items_end()
            .children(children)
            .into_any_element()
    }
}

fn render_fragment(
    message_key: &str,
    index: usize,
    fragment: &MessageFragment,
    compact_inline: bool,
    affinity_index: &HashMap<UserId, Affinity>,
    current_user_id: Option<&UserId>,
    emoji_index: &HashMap<String, InlineEmojiRender>,
    emoji_source_index: &HashMap<String, InlineEmojiRender>,
    emoji_only: bool,
    selectable_texts: &mut HashMap<String, Entity<SelectableText>>,
    cx: &mut Context<AppWindow>,
) -> AnyElement {
    match fragment {
        MessageFragment::Text(text) => {
            let mut combined = String::new();
            let mut link_ranges: Vec<LinkRange> = Vec::new();
            let mut styled_ranges: Vec<StyledRange> = Vec::new();
            for segment in inline_text_segments(text) {
                match segment {
                    InlineTextSegment::Text(value) => combined.push_str(&value),
                    InlineTextSegment::InlineCode(code) => {
                        push_inline_code_span(&mut combined, &mut styled_ranges, &code);
                    }
                    InlineTextSegment::Link(url) => {
                        let start = combined.len();
                        combined.push_str(&url);
                        link_ranges.push(LinkRange {
                            byte_range: start..combined.len(),
                            url,
                        });
                    }
                }
            }
            let parsed = apply_inline_markdown(
                &combined,
                &link_ranges,
                &styled_ranges,
                InlineMarkdownConfig {
                    spoiler_foreground: Some(text_secondary()),
                    spoiler_background: Some(panel_alt_bg()),
                },
            );
            let selectable = if compact_inline {
                resolve_selectable_text_inline(
                    selectable_texts,
                    format!("right-pane-{message_key}-{index}"),
                    parsed.text,
                    parsed.link_ranges,
                    parsed.styled_ranges,
                    cx,
                )
            } else {
                resolve_selectable_text(
                    selectable_texts,
                    format!("right-pane-{message_key}-{index}"),
                    parsed.text,
                    parsed.link_ranges,
                    parsed.styled_ranges,
                    cx,
                )
            };
            div()
                .when(compact_inline, |container| container.flex_1().min_w(px(0.)))
                .text_sm()
                .line_height(px(22.))
                .text_color(rgb(text_primary()))
                .child(selectable)
                .into_any_element()
        }
        MessageFragment::Code { text, .. } => {
            let selectable = if compact_inline {
                resolve_selectable_text_inline(
                    selectable_texts,
                    format!("right-pane-{message_key}-{index}"),
                    text.clone(),
                    Vec::new(),
                    Vec::new(),
                    cx,
                )
            } else {
                resolve_selectable_text(
                    selectable_texts,
                    format!("right-pane-{message_key}-{index}"),
                    text.clone(),
                    Vec::new(),
                    Vec::new(),
                    cx,
                )
            };
            div()
                .text_sm()
                .font_family(mono_font_family())
                .line_height(px(22.))
                .text_color(rgb(text_primary()))
                .child(selectable)
                .into_any_element()
        }
        MessageFragment::Quote(text) => {
            let selectable = if compact_inline {
                resolve_selectable_text_inline(
                    selectable_texts,
                    format!("right-pane-{message_key}-{index}"),
                    text.clone(),
                    Vec::new(),
                    Vec::new(),
                    cx,
                )
            } else {
                resolve_selectable_text(
                    selectable_texts,
                    format!("right-pane-{message_key}-{index}"),
                    text.clone(),
                    Vec::new(),
                    Vec::new(),
                    cx,
                )
            };
            div()
                .text_sm()
                .line_height(px(22.))
                .text_color(rgb(text_primary()))
                .child(selectable)
                .into_any_element()
        }
        MessageFragment::InlineCode(code) => {
            let selectable = if compact_inline {
                resolve_selectable_text_inline(
                    selectable_texts,
                    format!("right-pane-{message_key}-{index}-inline-code"),
                    code.clone(),
                    Vec::new(),
                    Vec::new(),
                    cx,
                )
            } else {
                resolve_selectable_text(
                    selectable_texts,
                    format!("right-pane-{message_key}-{index}-inline-code"),
                    code.clone(),
                    Vec::new(),
                    Vec::new(),
                    cx,
                )
            };
            div()
                .px_1()
                .py_0p5()
                .rounded_sm()
                .bg(subtle_surface())
                .text_sm()
                .font_family(mono_font_family())
                .line_height(px(22.))
                .text_color(rgb(text_primary()))
                .child(selectable)
                .into_any_element()
        }
        MessageFragment::Emoji { alias, source_ref } => {
            if let Some(render) =
                resolved_emoji_render(alias, source_ref.as_ref(), emoji_index, emoji_source_index)
            {
                if let Some(unicode) = render.unicode.as_ref() {
                    return div()
                        .text_color(rgb(text_primary()))
                        .when(emoji_only, |container| {
                            container.text_size(px(48.)).line_height(px(52.))
                        })
                        .when(!emoji_only, |container| {
                            container.text_sm().line_height(px(22.))
                        })
                        .child(unicode.clone())
                        .into_any_element();
                }
                if let Some(asset_path) = render.asset_path.as_ref() {
                    let size = custom_emoji_size_px(emoji_only);
                    return div()
                        .w(px(size))
                        .h(px(size))
                        .flex_shrink_0()
                        .overflow_hidden()
                        .child(
                            img(ImageSource::from(std::path::PathBuf::from(
                                crate::views::normalize_local_source_path(asset_path),
                            )))
                            .w(px(size))
                            .h(px(size))
                            .object_fit(ObjectFit::Contain)
                            .with_fallback({
                                let alias = alias.clone();
                                move || {
                                    div()
                                        .text_color(rgb(text_primary()))
                                        .when(emoji_only, |container| {
                                            container.text_size(px(48.)).line_height(px(52.))
                                        })
                                        .when(!emoji_only, |container| {
                                            container.text_sm().line_height(px(22.))
                                        })
                                        .child(format!(":{alias}:"))
                                        .into_any_element()
                                }
                            }),
                        )
                        .into_any_element();
                }
            }
            if let Some(unicode) = standard_emoji_for_alias(alias) {
                return div()
                    .text_color(rgb(text_primary()))
                    .when(emoji_only, |container| {
                        container.text_size(px(48.)).line_height(px(52.))
                    })
                    .when(!emoji_only, |container| {
                        container.text_sm().line_height(px(22.))
                    })
                    .child(unicode.to_string())
                    .into_any_element();
            }
            div()
                .text_color(rgb(text_primary()))
                .when(emoji_only, |container| {
                    container.text_size(px(48.)).line_height(px(52.))
                })
                .when(!emoji_only, |container| {
                    container.text_sm().line_height(px(22.))
                })
                .child(format!(":{alias}:"))
                .into_any_element()
        }
        MessageFragment::Mention(user_id) => {
            let label = format!("@{}", user_id.0);
            let (foreground, background) =
                mention_colors_for_user(affinity_index, current_user_id, user_id);
            render_linked_styled_fragment_text(
                message_key,
                index,
                &label,
                format!("zbase-mention:{}", user_id.0),
                foreground,
                background,
                compact_inline,
                selectable_texts,
                cx,
            )
        }
        MessageFragment::ChannelMention { name } => {
            let label = format!("#{name}");
            render_linked_styled_fragment_text(
                message_key,
                index,
                &label,
                format!("zbase-channel:{name}"),
                accent(),
                accent_soft(),
                compact_inline,
                selectable_texts,
                cx,
            )
        }
        MessageFragment::BroadcastMention(BroadcastKind::Here) => render_styled_fragment_text(
            message_key,
            index,
            "@here".to_string(),
            danger(),
            danger_soft(),
            compact_inline,
            selectable_texts,
            cx,
        ),
        MessageFragment::BroadcastMention(BroadcastKind::All) => render_styled_fragment_text(
            message_key,
            index,
            "@channel".to_string(),
            danger(),
            danger_soft(),
            compact_inline,
            selectable_texts,
            cx,
        ),
        MessageFragment::Link { url, display } => {
            let selectable = if compact_inline {
                resolve_selectable_text_inline(
                    selectable_texts,
                    format!("right-pane-{message_key}-{index}-link"),
                    display.clone(),
                    vec![LinkRange {
                        byte_range: 0..display.len(),
                        url: url.clone(),
                    }],
                    Vec::new(),
                    cx,
                )
            } else {
                resolve_selectable_text(
                    selectable_texts,
                    format!("right-pane-{message_key}-{index}-link"),
                    display.clone(),
                    vec![LinkRange {
                        byte_range: 0..display.len(),
                        url: url.clone(),
                    }],
                    Vec::new(),
                    cx,
                )
            };
            div()
                .when(compact_inline, |container| container.flex_1().min_w(px(0.)))
                .text_sm()
                .line_height(px(22.))
                .text_color(rgb(text_primary()))
                .child(selectable)
                .into_any_element()
        }
    }
}

fn render_send_state(send_state: &MessageSendState) -> AnyElement {
    match send_state {
        MessageSendState::Sent => div().into_any_element(),
        MessageSendState::Pending => badge("Sending", accent_soft(), accent()),
        MessageSendState::Failed => badge("Failed", danger_soft(), danger()),
    }
}

fn render_styled_fragment_text(
    message_key: &str,
    index: usize,
    value: String,
    foreground: u32,
    background: u32,
    compact_inline: bool,
    selectable_texts: &mut HashMap<String, Entity<SelectableText>>,
    cx: &mut Context<AppWindow>,
) -> AnyElement {
    let mut styled_ranges = Vec::new();
    if !value.is_empty() {
        styled_ranges.push(StyledRange {
            byte_range: 0..value.len(),
            color: Some(foreground),
            background_color: Some(background),
            bold: true,
            italic: false,
            strikethrough: false,
        });
    }
    let selectable = if compact_inline {
        resolve_selectable_text_inline(
            selectable_texts,
            format!("right-pane-{message_key}-{index}-styled"),
            value,
            Vec::new(),
            styled_ranges,
            cx,
        )
    } else {
        resolve_selectable_text(
            selectable_texts,
            format!("right-pane-{message_key}-{index}-styled"),
            value,
            Vec::new(),
            styled_ranges,
            cx,
        )
    };
    div()
        .when(compact_inline, |container| container.flex_1().min_w(px(0.)))
        .text_sm()
        .line_height(px(22.))
        .text_color(rgb(text_primary()))
        .child(selectable)
        .into_any_element()
}

fn render_linked_styled_fragment_text(
    message_key: &str,
    index: usize,
    value: &str,
    url: String,
    foreground: u32,
    background: u32,
    compact_inline: bool,
    selectable_texts: &mut HashMap<String, Entity<SelectableText>>,
    cx: &mut Context<AppWindow>,
) -> AnyElement {
    let link_ranges = if value.is_empty() {
        Vec::new()
    } else {
        vec![LinkRange {
            byte_range: 0..value.len(),
            url,
        }]
    };
    let mut styled_ranges = Vec::new();
    if !value.is_empty() {
        styled_ranges.push(StyledRange {
            byte_range: 0..value.len(),
            color: Some(foreground),
            background_color: Some(background),
            bold: true,
            italic: false,
            strikethrough: false,
        });
    }
    let selectable = if compact_inline {
        resolve_selectable_text_inline(
            selectable_texts,
            format!("right-pane-{message_key}-{index}-channel"),
            value.to_string(),
            link_ranges,
            styled_ranges,
            cx,
        )
    } else {
        resolve_selectable_text(
            selectable_texts,
            format!("right-pane-{message_key}-{index}-channel"),
            value.to_string(),
            link_ranges,
            styled_ranges,
            cx,
        )
    };
    div()
        .when(compact_inline, |container| container.flex_1().min_w(px(0.)))
        .text_sm()
        .line_height(px(22.))
        .text_color(rgb(text_primary()))
        .child(selectable)
        .into_any_element()
}

fn push_styled_span(
    combined: &mut String,
    styled_ranges: &mut Vec<StyledRange>,
    text: &str,
    foreground: u32,
    background: u32,
) {
    let start = combined.len();
    combined.push_str(text);
    let end = combined.len();
    if start < end {
        styled_ranges.push(StyledRange {
            byte_range: start..end,
            color: Some(foreground),
            background_color: Some(background),
            bold: true,
            italic: false,
            strikethrough: false,
        });
    }
}

fn push_inline_code_span(combined: &mut String, styled_ranges: &mut Vec<StyledRange>, code: &str) {
    let start = combined.len();
    combined.push_str(code);
    let end = combined.len();
    if start < end {
        styled_ranges.push(StyledRange {
            byte_range: start..end,
            color: Some(text_primary()),
            background_color: Some(mention_soft()),
            bold: false,
            italic: false,
            strikethrough: false,
        });
    }
}

fn inline_attachment_placeholder(_emoji_only: bool) -> &'static str {
    "\u{2003}"
}

fn push_inline_asset_emoji_attachment(
    combined: &mut String,
    inline_attachments: &mut Vec<InlineAttachment>,
    alias: &str,
    asset_path: &str,
    emoji_only: bool,
) {
    let start = combined.len();
    combined.push_str(inline_attachment_placeholder(emoji_only));
    let end = combined.len();
    if start < end {
        inline_attachments.push(InlineAttachment {
            byte_range: start..end,
            source: asset_path.to_string(),
            fallback_text: format!(":{alias}:"),
            size_px: custom_emoji_size_px(emoji_only).round() as u16,
        });
    }
}

fn resolved_emoji_render<'a>(
    alias: &str,
    source_ref: Option<&crate::domain::message::EmojiSourceRef>,
    emoji_index: &'a HashMap<String, InlineEmojiRender>,
    emoji_source_index: &'a HashMap<String, InlineEmojiRender>,
) -> Option<&'a InlineEmojiRender> {
    if let Some(source_ref) = source_ref
        && let Some(render) = emoji_source_index.get(&source_ref.cache_key())
    {
        return Some(render);
    }
    let key = alias.to_ascii_lowercase();
    emoji_index.get(&key)
}

fn inline_emoji_text(
    alias: &str,
    source_ref: Option<&crate::domain::message::EmojiSourceRef>,
    emoji_index: &HashMap<String, InlineEmojiRender>,
    emoji_source_index: &HashMap<String, InlineEmojiRender>,
) -> String {
    if let Some(unicode) = resolved_emoji_render(alias, source_ref, emoji_index, emoji_source_index)
        .and_then(|render| render.unicode.clone())
    {
        return unicode;
    }
    if let Some(unicode) = standard_emoji_for_alias(alias) {
        return unicode.to_string();
    }
    format!(":{alias}:")
}

fn fragment_requires_asset_emoji_box(
    fragment: &MessageFragment,
    emoji_index: &HashMap<String, InlineEmojiRender>,
    emoji_source_index: &HashMap<String, InlineEmojiRender>,
) -> bool {
    let MessageFragment::Emoji { alias, source_ref } = fragment else {
        return false;
    };
    resolved_emoji_render(alias, source_ref.as_ref(), emoji_index, emoji_source_index)
        .map(|render| render.unicode.is_none() && render.asset_path.is_some())
        .unwrap_or(false)
}

#[derive(Clone)]
enum InlineTextSegment {
    Text(String),
    InlineCode(String),
    Link(String),
}

fn inline_text_segments(text: &str) -> Vec<InlineTextSegment> {
    let chars = text.chars().collect::<Vec<_>>();
    if chars.is_empty() {
        return vec![InlineTextSegment::Text(String::new())];
    }

    let mut segments = Vec::new();
    let mut current_text = String::new();
    let mut index = 0usize;
    while index < chars.len() {
        if chars[index] == '`' {
            let start = index + 1;
            if let Some(end) = chars[start..].iter().position(|&ch| ch == '`') {
                let end = start + end;
                if end > start {
                    if !current_text.is_empty() {
                        segments.push(InlineTextSegment::Text(std::mem::take(&mut current_text)));
                    }
                    let code: String = chars[start..end].iter().collect();
                    segments.push(InlineTextSegment::InlineCode(code));
                    index = end + 1;
                    continue;
                }
            }
        }

        if let Some(url_len) = detect_url(&chars, index) {
            if !current_text.is_empty() {
                segments.push(InlineTextSegment::Text(std::mem::take(&mut current_text)));
            }
            let url: String = chars[index..index + url_len].iter().collect();
            segments.push(InlineTextSegment::Link(url));
            index += url_len;
            continue;
        }

        current_text.push(chars[index]);
        index += 1;
    }

    if !current_text.is_empty() {
        segments.push(InlineTextSegment::Text(current_text));
    }
    if segments.is_empty() {
        segments.push(InlineTextSegment::Text(text.to_string()));
    }
    segments
}

fn detect_url(chars: &[char], start: usize) -> Option<usize> {
    const HTTPS_PREFIX: [char; 8] = ['h', 't', 't', 'p', 's', ':', '/', '/'];
    const HTTP_PREFIX: [char; 7] = ['h', 't', 't', 'p', ':', '/', '/'];
    const KEYBASE_PREFIX: [char; 10] = ['k', 'e', 'y', 'b', 'a', 's', 'e', ':', '/', '/'];

    let (prefix_len, min_required_end) = if has_prefix(chars, start, &HTTPS_PREFIX) {
        (HTTPS_PREFIX.len(), start + HTTPS_PREFIX.len())
    } else if has_prefix(chars, start, &HTTP_PREFIX) {
        (HTTP_PREFIX.len(), start + HTTP_PREFIX.len())
    } else if has_prefix(chars, start, &KEYBASE_PREFIX) {
        (KEYBASE_PREFIX.len(), start + KEYBASE_PREFIX.len() + 2)
    } else {
        return None;
    };

    let mut end = start + prefix_len;
    while end < chars.len() && !chars[end].is_whitespace() {
        end += 1;
    }

    while end > start && matches!(chars[end - 1], '.' | ',' | ')' | ']' | ';') {
        end -= 1;
    }

    if end > min_required_end {
        Some(end - start)
    } else {
        None
    }
}

fn has_prefix(chars: &[char], start: usize, prefix: &[char]) -> bool {
    if start + prefix.len() > chars.len() {
        return false;
    }
    chars[start..start + prefix.len()]
        .iter()
        .zip(prefix.iter())
        .all(|(left, right)| left == right)
}

fn message_is_emoji_only(fragments: &[MessageFragment], has_attachments: bool) -> bool {
    !fragments.is_empty()
        && !has_attachments
        && fragments.iter().all(|fragment| match fragment {
            MessageFragment::Emoji { .. } => true,
            MessageFragment::Text(text) => is_emoji_only(text),
            _ => false,
        })
}

fn is_emoji_only(text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return false;
    }
    let mut has_emoji = false;
    for ch in trimmed.chars() {
        if ch.is_whitespace() {
            continue;
        }
        if ch.is_ascii_alphanumeric() || ch.is_ascii_punctuation() {
            return false;
        }
        has_emoji = true;
    }
    has_emoji
}

fn custom_emoji_size_px(emoji_only: bool) -> f32 {
    if emoji_only {
        CUSTOM_EMOJI_EMOJI_ONLY_SIZE_PX
    } else {
        CUSTOM_EMOJI_INLINE_SIZE_PX
    }
}

fn standard_emoji_for_alias(alias: &str) -> Option<&'static str> {
    let normalized = normalize_emoji_alias(alias);
    if normalized.is_empty() {
        return None;
    }
    static LOOKUP: OnceLock<HashMap<String, &'static str>> = OnceLock::new();
    let lookup = LOOKUP.get_or_init(|| {
        let mut map = HashMap::new();
        for emoji in emojis::iter() {
            map.entry(normalize_emoji_alias(emoji.name()))
                .or_insert(emoji.as_str());
            for shortcode in emoji.shortcodes() {
                map.entry(normalize_emoji_alias(shortcode))
                    .or_insert(emoji.as_str());
            }
        }
        map
    });
    lookup.get(&normalized).copied()
}

fn normalize_emoji_alias(alias: &str) -> String {
    let mut out = String::with_capacity(alias.len());
    let mut previous_underscore = false;
    for ch in alias.trim().trim_matches(':').chars() {
        let normalized = if ch.is_ascii_alphanumeric() {
            previous_underscore = false;
            Some(ch.to_ascii_lowercase())
        } else if ch == '+' {
            previous_underscore = false;
            Some('+')
        } else if matches!(ch, '_' | '-' | ' ') {
            if previous_underscore {
                None
            } else {
                previous_underscore = true;
                Some('_')
            }
        } else {
            None
        };
        if let Some(normalized) = normalized {
            out.push(normalized);
        }
    }
    while out.ends_with('_') {
        out.pop();
    }
    out
}

fn build_thread_message_rows(
    thread: &ThreadPaneModel,
    timeline: &TimelineModel,
) -> Vec<MessageRow> {
    const NON_TEXT_PLACEHOLDER_BODY: &str = "<non-text message>";
    fn is_non_text_placeholder_message(message: &MessageRecord) -> bool {
        if message.event.is_some() {
            return false;
        }
        if !message.attachments.is_empty() || !message.link_previews.is_empty() {
            return false;
        }
        if message.fragments.len() != 1 {
            return false;
        }
        matches!(
            message.fragments.first(),
            Some(MessageFragment::Text(text)) if text.trim() == NON_TEXT_PLACEHOLDER_BODY
        )
    }

    let mut root_message_fallback = None;
    if let Some(root_id) = thread.root_message_id.as_ref()
        && !thread.replies.iter().any(|message| &message.id == root_id)
    {
        root_message_fallback = find_message_record(timeline, root_id);
    }

    let mut messages = thread.replies.iter().collect::<Vec<_>>();
    if let Some(root_message) = root_message_fallback.as_ref() {
        messages.push(root_message);
    }

    messages.sort_by_key(|message| message.id.0.parse::<u64>().unwrap_or(0));
    let mut seen_ids = HashSet::new();
    messages.retain(|message| seen_ids.insert(message.id.clone()));
    messages.retain(|message| !is_non_text_placeholder_message(message));

    let author_lookup = build_thread_author_lookup(timeline);
    let mut rows = Vec::with_capacity(messages.len());
    let mut previous_message: Option<(UserId, Option<i64>)> = None;
    for message in messages {
        let show_header =
            previous_message
                .as_ref()
                .is_none_or(|(previous_author_id, previous_timestamp_ms)| {
                    if previous_author_id != &message.author_id {
                        return true;
                    }
                    let (Some(previous_timestamp_ms), Some(current_timestamp_ms)) =
                        (*previous_timestamp_ms, message.timestamp_ms)
                    else {
                        return true;
                    };
                    if current_timestamp_ms <= previous_timestamp_ms {
                        return true;
                    }
                    const GROUP_WINDOW_MS: i64 = 15 * 60 * 1000;
                    current_timestamp_ms.saturating_sub(previous_timestamp_ms) > GROUP_WINDOW_MS
                });

        rows.push(MessageRow {
            author: author_lookup
                .get(&message.author_id)
                .cloned()
                .unwrap_or_else(|| fallback_thread_author_summary(&message.author_id)),
            message: message.clone(),
            show_header,
        });
        previous_message = Some((message.author_id.clone(), message.timestamp_ms));
    }
    rows
}

fn build_thread_author_lookup(timeline: &TimelineModel) -> HashMap<UserId, UserSummary> {
    let mut lookup = HashMap::new();
    for row in &timeline.rows {
        let TimelineRow::Message(message_row) = row else {
            continue;
        };
        let author = message_row.author.clone();
        lookup
            .entry(author.id.clone())
            .or_insert_with(|| author.clone());
        let normalized_author_id = UserId::new(author.id.0.to_ascii_lowercase());
        lookup.entry(normalized_author_id).or_insert(author);
    }
    lookup
}

fn fallback_thread_author_summary(author_id: &UserId) -> UserSummary {
    UserSummary {
        id: author_id.clone(),
        display_name: author_id.0.clone(),
        title: String::new(),
        avatar_asset: None,
        presence: Presence {
            availability: Availability::Offline,
            status_text: None,
        },
        affinity: Affinity::None,
    }
}

fn find_message_record(timeline: &TimelineModel, message_id: &MessageId) -> Option<MessageRecord> {
    timeline.rows.iter().find_map(|row| {
        let TimelineRow::Message(message_row) = row else {
            return None;
        };
        (&message_row.message.id == message_id).then_some(message_row.message.clone())
    })
}
