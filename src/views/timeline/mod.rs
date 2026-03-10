use crate::{
    domain::{
        affinity::Affinity,
        attachment::AttachmentKind,
        ids::UserId,
        message::{BroadcastKind, LinkPreview, MessageFragment, MessageSendState},
    },
    models::timeline_model::{
        EventSpan, InlineEmojiRender, MessageReactionRender, MessageRow, SystemEventIcon,
        TeamAuthorRole, TimelineModel, TimelineRow,
    },
    util::formatting::message_timestamp_label,
    views::{
        CUSTOM_EMOJI_EMOJI_ONLY_SIZE_PX, CUSTOM_EMOJI_INLINE_SIZE_PX,
        CUSTOM_EMOJI_REACTION_SIZE_PX, accent, accent_soft, activity_icon, affinity_broken,
        affinity_positive,
        app_window::{AppWindow, video_preview_cache_key},
        arrow_left_icon, arrow_right_icon, attachment_display_label, attachment_image_source,
        attachment_lightbox_source,
        avatar::{Avatar, default_avatar_background},
        badge, close_icon, crown_icon, danger, glass_surface_dark, hash_icon,
        inline_markdown::{InlineMarkdownConfig, apply_inline_markdown, remap_source_byte_range},
        link_icon, mention_soft, panel_alt_bg, panel_bg, pin_icon, plus_icon,
        selectable_text::{
            InlineAttachment, LinkRange, SelectableText, StyledRange, resolve_selectable_text,
            resolve_selectable_text_inline, resolve_selectable_text_with_attachments,
        },
        shell_border, sliders_icon, subtle_surface, text_primary, text_secondary, thread_icon,
        tint, warning, warning_soft,
    },
};
use gpui::prelude::FluentBuilder;
use gpui::{
    AnyElement, Bounds, Context, Entity, FontWeight, ImageSource, InteractiveElement, IntoElement,
    LayoutId, ListState, ObjectFit, ParentElement, Pixels, RenderImage, SharedString,
    StatefulInteractiveElement, Styled, StyledImage, Window, deferred, div, img, list, px,
    relative, rgb,
};
use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{Arc, OnceLock},
};

#[derive(Clone)]
struct HoveredMessageBoundsReporter {
    owner: gpui::WeakEntity<AppWindow>,
    message_id: crate::domain::ids::MessageId,
    is_thread: bool,
}

impl gpui::IntoElement for HoveredMessageBoundsReporter {
    type Element = Self;

    fn into_element(self) -> Self::Element {
        self
    }
}

impl gpui::Element for HoveredMessageBoundsReporter {
    type RequestLayoutState = ();
    type PrepaintState = ();

    fn id(&self) -> Option<gpui::ElementId> {
        None
    }

    fn source_location(&self) -> Option<&'static core::panic::Location<'static>> {
        None
    }

    fn request_layout(
        &mut self,
        _id: Option<&gpui::GlobalElementId>,
        _inspector_id: Option<&gpui::InspectorElementId>,
        window: &mut Window,
        cx: &mut gpui::App,
    ) -> (LayoutId, Self::RequestLayoutState) {
        let mut style = gpui::Style::default();
        style.size.width = relative(1.).into();
        style.size.height = relative(1.).into();
        (window.request_layout(style, std::iter::empty(), cx), ())
    }

    fn prepaint(
        &mut self,
        _id: Option<&gpui::GlobalElementId>,
        _inspector_id: Option<&gpui::InspectorElementId>,
        bounds: Bounds<Pixels>,
        _request_layout: &mut Self::RequestLayoutState,
        _window: &mut Window,
        cx: &mut gpui::App,
    ) -> Self::PrepaintState {
        let owner = self.owner.clone();
        let message_id = self.message_id.clone();
        let is_thread = self.is_thread;
        let left: f32 = bounds.origin.x.into();
        let width: f32 = bounds.size.width.into();
        let _ = owner.update(cx, |this, cx| {
            this.record_hovered_message_layout(message_id, is_thread, left, width, cx);
        });
    }

    fn paint(
        &mut self,
        _id: Option<&gpui::GlobalElementId>,
        _inspector_id: Option<&gpui::InspectorElementId>,
        _bounds: Bounds<Pixels>,
        _request_layout: &mut Self::RequestLayoutState,
        _prepaint: &mut Self::PrepaintState,
        _window: &mut Window,
        _cx: &mut gpui::App,
    ) {
    }
}

#[derive(Default)]
pub struct TimelineList;

const ROW_RENDER_CACHE_MAX_ENTRIES: usize = 512;

const QUICK_REACT_EMOJI: &[(&str, &str)] = &[
    ("+1", "\u{1F44D}"),
    ("heart", "\u{2764}\u{FE0F}"),
    ("joy", "\u{1F602}"),
    ("tada", "\u{1F389}"),
    ("eyes", "\u{1F440}"),
];

#[derive(Default)]
pub(crate) struct TimelineRowRenderCache {
    entries: HashMap<usize, TimelineRowRenderCacheEntry>,
    use_tick: u64,
}

struct TimelineRowRenderCacheEntry {
    row_ptr: usize,
    last_used_tick: u64,
    memo: MessageRenderMemo,
}

struct MessageRenderMemo {
    text_segments: Vec<Option<Vec<InlineTextSegment>>>,
    sorted_reactions: Vec<MessageReactionRender>,
}

impl TimelineRowRenderCache {
    pub(crate) fn invalidate(&mut self) {
        self.entries.clear();
        self.use_tick = 0;
    }

    fn memo_for_message<'a>(
        &'a mut self,
        row_index: usize,
        row: &TimelineRow,
        message: &crate::domain::message::MessageRecord,
        timeline: &TimelineModel,
    ) -> &'a MessageRenderMemo {
        self.use_tick = self.use_tick.wrapping_add(1);
        let row_ptr = row as *const TimelineRow as usize;
        let tick = self.use_tick;

        let needs_rebuild = self
            .entries
            .get(&row_index)
            .is_none_or(|entry| entry.row_ptr != row_ptr);

        if needs_rebuild {
            self.entries.insert(
                row_index,
                TimelineRowRenderCacheEntry {
                    row_ptr,
                    last_used_tick: tick,
                    memo: MessageRenderMemo::build(message, timeline),
                },
            );
            self.evict_lru_if_needed();
        }

        let entry = self
            .entries
            .get_mut(&row_index)
            .expect("timeline row render cache entry exists");
        entry.last_used_tick = tick;
        &entry.memo
    }

    fn drop_row(&mut self, row_index: usize) {
        self.entries.remove(&row_index);
    }

    fn evict_lru_if_needed(&mut self) {
        while self.entries.len() > ROW_RENDER_CACHE_MAX_ENTRIES {
            let Some((&oldest_index, _)) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used_tick)
            else {
                break;
            };
            self.entries.remove(&oldest_index);
        }
    }
}

impl MessageRenderMemo {
    fn build(message: &crate::domain::message::MessageRecord, timeline: &TimelineModel) -> Self {
        let text_segments = message
            .fragments
            .iter()
            .map(|fragment| match fragment {
                MessageFragment::Text(text) => Some(inline_text_segments(text)),
                _ => None,
            })
            .collect();

        let mut sorted_reactions = timeline
            .reaction_index
            .get(&message.id)
            .cloned()
            .unwrap_or_default();
        sorted_reactions.sort_by(|left, right| left.emoji.cmp(&right.emoji));

        Self {
            text_segments,
            sorted_reactions,
        }
    }
}

impl TimelineList {
    pub fn render(
        &self,
        _timeline: &TimelineModel,
        list_state: &ListState,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        div()
            .flex_1()
            .min_h(px(0.))
            .id("timeline-scroll")
            .py_2()
            .flex()
            .flex_col()
            .gap_1()
            .overflow_hidden()
            .on_scroll_wheel(cx.listener(AppWindow::timeline_scrolled))
            .pb_4()
            .child(
                list(
                    list_state.clone(),
                    cx.processor(|this, ix: usize, _window, cx| {
                        this.render_timeline_row_virtualized(ix, cx)
                    }),
                )
                .h_full()
                .w_full(),
            )
            .into_any_element()
    }

    pub(crate) fn render_row(
        timeline: &TimelineModel,
        row: &TimelineRow,
        video_render_cache: &HashMap<String, Arc<RenderImage>>,
        selectable_texts: &mut HashMap<String, Entity<SelectableText>>,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        Self::render_row_inner(
            timeline,
            row,
            None,
            true,
            false,
            None,
            video_render_cache,
            selectable_texts,
            cx,
        )
    }

    pub(crate) fn render_thread_row(
        timeline: &TimelineModel,
        row: &TimelineRow,
        video_render_cache: &HashMap<String, Arc<RenderImage>>,
        selectable_texts: &mut HashMap<String, Entity<SelectableText>>,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        Self::render_row_inner(
            timeline,
            row,
            None,
            false,
            true,
            None,
            video_render_cache,
            selectable_texts,
            cx,
        )
    }

    pub(crate) fn render_row_cached(
        timeline: &TimelineModel,
        row_index: usize,
        row: &TimelineRow,
        row_render_cache: &mut TimelineRowRenderCache,
        find_query: Option<&str>,
        video_render_cache: &HashMap<String, Arc<RenderImage>>,
        selectable_texts: &mut HashMap<String, Entity<SelectableText>>,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        if let TimelineRow::Message(message_row) = row {
            let message_memo =
                row_render_cache.memo_for_message(row_index, row, &message_row.message, timeline);
            return Self::render_row_inner(
                timeline,
                row,
                Some(message_memo),
                true,
                false,
                find_query,
                video_render_cache,
                selectable_texts,
                cx,
            );
        }

        row_render_cache.drop_row(row_index);
        Self::render_row_inner(
            timeline,
            row,
            None,
            true,
            false,
            find_query,
            video_render_cache,
            selectable_texts,
            cx,
        )
    }

    fn render_row_inner(
        timeline: &TimelineModel,
        row: &TimelineRow,
        message_memo: Option<&MessageRenderMemo>,
        show_thread_reply_badge: bool,
        is_thread: bool,
        find_query: Option<&str>,
        video_render_cache: &HashMap<String, Arc<RenderImage>>,
        selectable_texts: &mut HashMap<String, Entity<SelectableText>>,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        let row_element = match row {
            TimelineRow::DateDivider(label) => div()
                .px_4()
                .py_2()
                .flex()
                .items_center()
                .justify_center()
                .child(badge(label.clone(), panel_alt_bg(), text_secondary()))
                .into_any_element(),
            TimelineRow::UnreadDivider(label) => div()
                .px_4()
                .py_1()
                .flex()
                .items_center()
                .justify_center()
                .child(badge(label.clone(), warning_soft(), warning()))
                .into_any_element(),
            TimelineRow::SystemEvent(row) => {
                let icon = match row.icon {
                    SystemEventIcon::Join => arrow_right_icon(text_secondary()),
                    SystemEventIcon::Leave => arrow_left_icon(text_secondary()),
                    SystemEventIcon::Add => plus_icon(text_secondary()),
                    SystemEventIcon::Remove => activity_icon(text_secondary()),
                    SystemEventIcon::Pin => pin_icon(text_secondary()),
                    SystemEventIcon::Description => hash_icon(text_secondary()),
                    SystemEventIcon::Settings => sliders_icon(text_secondary()),
                    SystemEventIcon::Info => activity_icon(text_secondary()),
                };
                div()
                    .w_full()
                    .min_w(px(0.))
                    .flex()
                    .items_center()
                    .gap_2()
                    .py_1()
                    .px_4()
                    .child(icon)
                    .child(
                        div()
                            .min_w(px(0.))
                            .flex()
                            .flex_wrap()
                            .items_center()
                            .gap_0p5()
                            .text_xs()
                            .text_color(rgb(text_secondary()))
                            .children(row.spans.iter().map(|span| {
                                match span {
                                    EventSpan::Actor(name) => div()
                                        .font_weight(FontWeight::MEDIUM)
                                        .child(name.clone())
                                        .into_any_element(),
                                    EventSpan::Text(text) => {
                                        div().child(text.clone()).into_any_element()
                                    }
                                }
                            })),
                    )
                    .into_any_element()
            }
            TimelineRow::LoadingIndicator(label) => div()
                .w_full()
                .min_w(px(0.))
                .flex()
                .items_center()
                .gap_2()
                .py_1()
                .px_4()
                .text_xs()
                .text_color(rgb(text_secondary()))
                .child(activity_icon(accent()))
                .child(label.clone())
                .into_any_element(),
            TimelineRow::Message(message_row) => {
                let is_last_message = timeline
                    .rows
                    .iter()
                    .rev()
                    .find_map(|r| {
                        if let TimelineRow::Message(m) = r {
                            Some(&m.message.id)
                        } else {
                            None
                        }
                    })
                    .is_some_and(|last_id| last_id == &message_row.message.id);
                Self::render_message_row(
                    timeline,
                    message_row,
                    message_memo,
                    show_thread_reply_badge,
                    is_thread,
                    is_last_message,
                    find_query,
                    video_render_cache,
                    selectable_texts,
                    cx,
                )
            }
        };

        div()
            .w_full()
            .min_w(px(0.))
            .child(row_element)
            .into_any_element()
    }

    fn render_message_row(
        timeline: &TimelineModel,
        row: &MessageRow,
        message_memo: Option<&MessageRenderMemo>,
        show_thread_reply_badge: bool,
        is_thread: bool,
        is_last_message: bool,
        find_query: Option<&str>,
        video_render_cache: &HashMap<String, Arc<RenderImage>>,
        selectable_texts: &mut HashMap<String, Entity<SelectableText>>,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        let timestamp = message_timestamp_label(row.message.timestamp_ms);
        let author_user_id = row.author.id.clone();
        let is_hovered = timeline
            .hovered_message_id
            .as_ref()
            .is_some_and(|id| id == &row.message.id)
            && timeline.hovered_message_is_thread == Some(is_thread);
        let hover_anchor_x = is_hovered
            .then_some(timeline.hovered_message_anchor_x)
            .flatten();
        let hover_window_left = is_hovered
            .then_some(timeline.hovered_message_window_left)
            .flatten();
        let hover_window_width = is_hovered
            .then_some(timeline.hovered_message_window_width)
            .flatten();
        let is_thread_reply_stub = !is_thread && row.message.reply_to.is_some();
        let show_header = row.show_header || is_thread_reply_stub || is_thread;
        let normalized_author_id = UserId::new(row.author.id.0.to_ascii_lowercase());
        let author_role = timeline
            .author_role_index
            .get(&row.author.id)
            .copied()
            .or_else(|| {
                timeline
                    .author_role_index
                    .get(&normalized_author_id)
                    .copied()
            });
        let mut content = div().flex_1().min_w(px(0.)).flex().flex_col().gap_0p5();

        if show_header {
            let is_you = timeline
                .current_user_id
                .as_ref()
                .is_some_and(|current_user_id| {
                    current_user_id.0.eq_ignore_ascii_case(&row.author.id.0)
                });
            let mut header_children = vec![
                div()
                    .id(SharedString::from(format!(
                        "timeline-author-name-{}",
                        row.message.id.0
                    )))
                    .on_click(cx.listener({
                        let author_user_id = author_user_id.clone();
                        move |this, _, _, cx| {
                            this.open_user_profile_card(author_user_id.clone(), cx);
                        }
                    }))
                    .cursor(gpui::CursorStyle::PointingHand)
                    .text_sm()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(rgb(Self::author_name_color(row.author.affinity, is_you)))
                    .child(row.author.display_name.clone())
                    .into_any_element(),
            ];
            if let Some(role) = author_role {
                header_children.push(author_role_crown_badge(role));
            }
            header_children.push(
                div()
                    .text_xs()
                    .text_color(rgb(text_secondary()))
                    .child(timestamp)
                    .into_any_element(),
            );
            content = content.child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .children(header_children),
            );
        }

        if !is_thread
            && let Some(reply_to_id) = &row.message.reply_to {
                let thread_target = row
                    .message
                    .thread_root_id
                    .clone()
                    .unwrap_or_else(|| reply_to_id.clone());

                let reply_indicator = div()
                    .id(SharedString::from(format!(
                        "reply-indicator-{}",
                        row.message.id.0
                    )))
                    .on_click(cx.listener(move |this, _, window, cx| {
                        this.open_thread(thread_target.clone(), window, cx);
                    }))
                    .cursor(gpui::CursorStyle::PointingHand)
                    .flex()
                    .items_center()
                    .gap_1()
                    .child(thread_icon(text_secondary()));

                content = content.child(
                    reply_indicator.child(
                        div()
                            .text_xs()
                            .text_color(rgb(text_secondary()))
                            .child("Replied in thread"),
                    ),
                );
            }

        let is_highlighted = timeline.highlighted_message_id.as_ref() == Some(&row.message.id)
            || timeline.editing_message_id.as_ref() == Some(&row.message.id);
        if is_thread_reply_stub {
            content = content.child(Self::render_send_state(
                &row.message.id,
                &row.message.send_state,
                cx,
            ));
        } else {
            content = content.child(Self::render_message(
                &row.message,
                is_hovered,
                is_highlighted,
                find_query,
                &timeline.affinity_index,
                timeline.current_user_id.as_ref(),
                &timeline.emoji_index,
                &timeline.emoji_source_index,
                &timeline.reaction_index,
                show_thread_reply_badge,
                is_thread,
                is_last_message,
                hover_anchor_x,
                hover_window_left,
                hover_window_width,
                row.message.edited.is_some(),
                video_render_cache,
                message_memo,
                selectable_texts,
                cx,
            ));
        }

        let hover_msg_id = row.message.id.clone();
        let row_wrapper = if show_header {
            let hover_msg_id_for_anchor = row.message.id.clone();
            div()
                .id(SharedString::from(format!(
                    "timeline-row-{}",
                    row.message.id.0
                )))
                .on_mouse_move(
                    cx.listener(move |this, event: &gpui::MouseMoveEvent, _, cx| {
                        let cursor_x: f32 = event.position.x.into();
                        this.clear_reaction_hover_tooltip(cx);
                        this.set_hovered_message_with_cursor_anchor(
                            hover_msg_id_for_anchor.clone(),
                            cursor_x,
                            is_thread,
                            cx,
                        );
                        cx.stop_propagation();
                    }),
                )
                .w_full()
                .min_w(px(0.))
                .px_4()
                .flex()
                .gap_3()
                .items_start()
                .pt_0p5()
                .when(is_hovered && !is_highlighted, |d| {
                    d.bg(tint(panel_alt_bg(), 0.25))
                })
                .child(
                    div()
                        .id(SharedString::from(format!(
                            "timeline-author-avatar-{}",
                            row.message.id.0
                        )))
                        .cursor(gpui::CursorStyle::PointingHand)
                        .on_click(cx.listener({
                            let author_user_id = author_user_id.clone();
                            move |this, _, _, cx| {
                                this.open_user_profile_card(author_user_id.clone(), cx);
                            }
                        }))
                        .flex_shrink_0()
                        .child(Avatar::render(
                            &row.author.display_name,
                            row.author.avatar_asset.as_deref(),
                            32.,
                            default_avatar_background(&row.author.display_name),
                            text_primary(),
                        )),
                )
                .child(content)
        } else {
            let hover_msg_id_for_anchor = hover_msg_id;
            div()
                .id(SharedString::from(format!(
                    "timeline-row-{}",
                    row.message.id.0
                )))
                .on_mouse_move(
                    cx.listener(move |this, event: &gpui::MouseMoveEvent, _, cx| {
                        let cursor_x: f32 = event.position.x.into();
                        this.clear_reaction_hover_tooltip(cx);
                        this.set_hovered_message_with_cursor_anchor(
                            hover_msg_id_for_anchor.clone(),
                            cursor_x,
                            is_thread,
                            cx,
                        );
                        cx.stop_propagation();
                    }),
                )
                .w_full()
                .min_w(px(0.))
                .px_4()
                .flex()
                .gap_3()
                .items_start()
                .when(is_hovered && !is_highlighted, |d| {
                    d.bg(tint(panel_alt_bg(), 0.25))
                })
                .child(div().w(px(32.)).flex_shrink_0())
                .child(content)
        };
        row_wrapper.into_any_element()
    }

    fn author_name_color(affinity: Affinity, is_you: bool) -> u32 {
        if is_you {
            return text_primary();
        }
        match affinity {
            Affinity::None => text_primary(),
            Affinity::Positive => affinity_positive(),
            Affinity::Broken => affinity_broken(),
        }
    }

    fn mention_colors_for_user(
        affinity_index: &HashMap<UserId, Affinity>,
        current_user_id: Option<&UserId>,
        user_id: &UserId,
    ) -> (u32, u32) {
        super::mention_colors_for_user(affinity_index, current_user_id, user_id)
    }

    fn render_message(
        message: &crate::domain::message::MessageRecord,
        is_hovered: bool,
        highlighted: bool,
        find_query: Option<&str>,
        affinity_index: &HashMap<UserId, Affinity>,
        current_user_id: Option<&UserId>,
        emoji_index: &HashMap<String, InlineEmojiRender>,
        emoji_source_index: &HashMap<String, InlineEmojiRender>,
        reaction_index: &HashMap<crate::domain::ids::MessageId, Vec<MessageReactionRender>>,
        show_thread_reply_badge: bool,
        _is_thread: bool,
        is_last_message: bool,
        hover_anchor_x: Option<f32>,
        hover_window_left: Option<f32>,
        hover_window_width: Option<f32>,
        show_edited_badge: bool,
        video_render_cache: &HashMap<String, Arc<RenderImage>>,
        message_memo: Option<&MessageRenderMemo>,
        selectable_texts: &mut HashMap<String, Entity<SelectableText>>,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        let image_attachment_message = !message.attachments.is_empty()
            && message
                .attachments
                .iter()
                .all(|attachment| attachment.kind == AttachmentKind::Image);

        div()
            .id(SharedString::from(format!(
                "timeline-message-{}",
                message.id.0
            )))
            .w_full()
            .min_w(px(0.))
            .flex()
            .flex_col()
            .gap_0p5()
            .when(highlighted, |container| {
                container.rounded_md().bg(tint(accent_soft(), 0.55))
            })
            .pb_0p5()
            .relative()
            .child({
                let message_id_for_bounds = message.id.clone();
                let owner = cx.weak_entity();
                div()
                    .absolute()
                    .inset_0()
                    .child(HoveredMessageBoundsReporter {
                        owner,
                        message_id: message_id_for_bounds,
                        is_thread: _is_thread,
                    })
            })
            .child({
                if image_attachment_message {
                    div().into_any_element()
                } else {
                    Self::render_message_fragments(
                        message,
                        find_query,
                        affinity_index,
                        current_user_id,
                        emoji_index,
                        emoji_source_index,
                        message_memo,
                        selectable_texts,
                        cx,
                    )
                }
            })
            .when(!message.link_previews.is_empty(), |container| {
                container.child(Self::render_link_previews(
                    &message.id.0,
                    &message.link_previews,
                    message.attachments.is_empty() && message.thread_reply_count == 0,
                    video_render_cache,
                    cx,
                ))
            })
            .when(!message.attachments.is_empty(), |container| {
                container.child(
                    div().flex().flex_col().gap_1().children(
                        message
                            .attachments
                            .iter()
                            .enumerate()
                            .map(|(index, attachment)| {
                                let attachment_label = attachment_display_label(attachment);
                                if attachment.kind == AttachmentKind::Image
                                    && let Some(media_source) = attachment_image_source(attachment)
                                {
                                    let (max_width, max_height) = if show_thread_reply_badge {
                                        (360.0, 300.0)
                                    } else {
                                        (280.0, 240.0)
                                    };
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
                                    let (media_width, media_height) = Self::media_frame_size(
                                        preview_width,
                                        preview_height,
                                        max_width,
                                        max_height,
                                    );
                                    let lightbox_source = attachment_lightbox_source(attachment);
                                    let caption_text = Self::message_caption_text(message);
                                    let lightbox_width = preview_width;
                                    let lightbox_height = preview_height;
                                    return div()
                                        .id(SharedString::from(format!(
                                            "timeline-attachment-image-{}-{index}",
                                            message.id.0
                                        )))
                                        .flex()
                                        .flex_col()
                                        .gap_1()
                                        .when_some(lightbox_source, |image, lightbox_source| {
                                            let caption_text = caption_text.clone();
                                            image.cursor(gpui::CursorStyle::PointingHand).on_click(
                                                cx.listener(move |this, _, _, cx| {
                                                    this.open_image_lightbox(
                                                        lightbox_source.clone(),
                                                        caption_text.clone(),
                                                        lightbox_width,
                                                        lightbox_height,
                                                        cx,
                                                    );
                                                }),
                                            )
                                        })
                                        .child(
                                            img(media_source)
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
                                                            .text_color(rgb(text_secondary()))
                                                            .child(label.clone())
                                                            .into_any_element()
                                                    }
                                                }),
                                        )
                                        .into_any_element();
                                }
                                div()
                                    .id(("timeline-file-open", index))
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.open_files_pane(cx);
                                    }))
                                    .text_xs()
                                    .text_color(rgb(accent()))
                                    .child(attachment_label)
                                    .into_any_element()
                            }),
                    ),
                )
            })
            .when(
                image_attachment_message && !message.fragments.is_empty(),
                |container| {
                    container.child(Self::render_message_fragments(
                        message,
                        find_query,
                        affinity_index,
                        current_user_id,
                        emoji_index,
                        emoji_source_index,
                        message_memo,
                        selectable_texts,
                        cx,
                    ))
                },
            )
            .when(show_edited_badge, |container| {
                container.child(
                    div()
                        .pt_0p5()
                        .text_xs()
                        .italic()
                        .text_color(rgb(text_secondary()))
                        .child("(edited)"),
                )
            })
            .child({
                let has_reactions = reaction_index
                    .get(&message.id)
                    .is_some_and(|reactions| !reactions.is_empty());

                let reaction_chips: Vec<AnyElement> = if has_reactions {
                    let reactions: Cow<'_, [MessageReactionRender]> =
                        if let Some(memo) = message_memo {
                            Cow::Borrowed(&memo.sorted_reactions)
                        } else {
                            let mut reactions =
                                reaction_index.get(&message.id).cloned().unwrap_or_default();
                            reactions.sort_by(|left, right| left.emoji.cmp(&right.emoji));
                            Cow::Owned(reactions)
                        };
                    reactions
                        .iter()
                        .cloned()
                        .enumerate()
                        .map(|(reaction_index, reaction)| {
                            let message_id = message.id.clone();
                            let reaction_emoji = reaction.emoji.clone();
                            let alias = reaction.emoji.trim_matches(':').to_ascii_lowercase();
                            let resolved = Self::resolved_emoji_render(
                                &alias,
                                reaction.source_ref.as_ref(),
                                emoji_index,
                                emoji_source_index,
                            );
                            let reaction_chip_id = SharedString::from(format!(
                                "reaction-chip-{}-{}-{}",
                                message.id.0,
                                reaction_index,
                                normalize_emoji_alias(&reaction.emoji)
                            ));
                            let hover_text = reaction_hover_text(&reaction);

                            let emoji_element: AnyElement = if let Some(render) = resolved {
                                if let Some(unicode) = &render.unicode {
                                    div().text_sm().child(unicode.clone()).into_any_element()
                                } else if let Some(asset_path) = &render.asset_path {
                                    div()
                                        .w(px(CUSTOM_EMOJI_REACTION_SIZE_PX))
                                        .h(px(CUSTOM_EMOJI_REACTION_SIZE_PX))
                                        .flex_shrink_0()
                                        .rounded_sm()
                                        .overflow_hidden()
                                        .child(
                                            img(ImageSource::from(std::path::PathBuf::from(
                                                crate::views::normalize_local_source_path(
                                                    asset_path,
                                                ),
                                            )))
                                            .w(px(CUSTOM_EMOJI_REACTION_SIZE_PX))
                                            .h(px(CUSTOM_EMOJI_REACTION_SIZE_PX))
                                            .object_fit(ObjectFit::Contain)
                                            .with_fallback({
                                                let alias = reaction.emoji.clone();
                                                move || {
                                                    div()
                                                        .text_xs()
                                                        .child(alias.clone())
                                                        .into_any_element()
                                                }
                                            }),
                                        )
                                        .into_any_element()
                                } else {
                                    div()
                                        .text_xs()
                                        .child(reaction.emoji.clone())
                                        .into_any_element()
                                }
                            } else if let Some(standard) = standard_emoji_for_alias(&alias) {
                                div()
                                    .text_sm()
                                    .child(standard.to_string())
                                    .into_any_element()
                            } else {
                                div()
                                    .text_xs()
                                    .child(reaction.emoji.clone())
                                    .into_any_element()
                            };

                            let chip = div()
                                .px_1p5()
                                .py_0p5()
                                .rounded_full()
                                .border_1()
                                .border_color(if reaction.reacted_by_me {
                                    tint(accent(), 1.0)
                                } else {
                                    shell_border()
                                })
                                .when(reaction.reacted_by_me, |chip| {
                                    chip.bg(tint(accent_soft(), 0.35))
                                })
                                .flex()
                                .items_center()
                                .gap_1()
                                .child(emoji_element)
                                .child(
                                    div()
                                        .text_xs()
                                        .text_color(rgb(text_secondary()))
                                        .child(format!("{}", reaction.count)),
                                );
                            let chip_with_hover = div()
                                .id(reaction_chip_id)
                                .cursor(gpui::CursorStyle::PointingHand)
                                .on_mouse_move(cx.listener({
                                    let message_id = message_id.clone();
                                    let hover_text = hover_text.clone();
                                    move |this, event: &gpui::MouseMoveEvent, _, cx| {
                                        let cursor_x: f32 = event.position.x.into();
                                        let cursor_y: f32 = event.position.y.into();
                                        this.set_hovered_message_with_cursor_anchor(
                                            message_id.clone(),
                                            cursor_x,
                                            _is_thread,
                                            cx,
                                        );
                                        if let Some(hover_text) = hover_text.as_ref() {
                                            this.show_reaction_hover_tooltip(
                                                hover_text.clone(),
                                                cursor_x,
                                                cursor_y,
                                                cx,
                                            );
                                        } else {
                                            this.clear_reaction_hover_tooltip(cx);
                                        }
                                        cx.stop_propagation();
                                    }
                                }))
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    this.quick_react(
                                        message_id.clone(),
                                        reaction_emoji.clone(),
                                        cx,
                                    );
                                }))
                                .child(chip);
                            chip_with_hover.into_any_element()
                        })
                        .collect()
                } else {
                    Vec::new()
                };

                if has_reactions {
                    div()
                        .flex()
                        .flex_wrap()
                        .items_center()
                        .gap_1()
                        .pt_1()
                        .children(reaction_chips)
                        .into_any_element()
                } else {
                    div().into_any_element()
                }
            })
            .child({
                let quick_react_buttons: Vec<AnyElement> = QUICK_REACT_EMOJI
                    .iter()
                    .map(|(alias, unicode)| {
                        let msg_id = message.id.clone();
                        let unicode_str = unicode.to_string();
                        div()
                            .id(SharedString::from(format!(
                                "quick-react-{alias}-{}",
                                message.id.0
                            )))
                            .on_click(cx.listener(move |this, _, _, cx| {
                                this.quick_react(msg_id.clone(), unicode_str.clone(), cx);
                            }))
                            .w(px(26.))
                            .h(px(26.))
                            .rounded_md()
                            .flex()
                            .items_center()
                            .justify_center()
                            .hover(|s| s.bg(subtle_surface()))
                            .cursor(gpui::CursorStyle::PointingHand)
                            .text_sm()
                            .child(unicode.to_string())
                            .into_any_element()
                    })
                    .collect();

                let message_id_for_picker = message.id.clone();
                let message_id_for_thread_btn = message.id.clone();
                let message_id_for_copy_link_btn = message.id.clone();
                let message_id_for_delete_btn = message.id.clone();
                let message_id_for_hover = message.id.clone();
                let is_own_message = current_user_id
                    .is_some_and(|id| id.0.eq_ignore_ascii_case(&message.author_id.0));
                let has_reactions = reaction_index
                    .get(&message.id)
                    .is_some_and(|reactions| !reactions.is_empty());
                let has_thread_badge = show_thread_reply_badge && message.thread_reply_count > 0;
                let has_blocking_inline_actions = has_reactions || has_thread_badge;

                const TOOLBAR_W: f32 = 260.0;
                const BLOCKING_LEFT: f32 = 260.0;

                let inline_toolbar = div()
                    .flex()
                    .items_center()
                    .rounded_md()
                    .bg(rgb(panel_bg()))
                    .border_1()
                    .border_color(shell_border())
                    .px_0p5()
                    .py_0p5()
                    .gap_0p5()
                    .children(quick_react_buttons)
                    .child(Self::toolbar_icon_button(
                        "message-react-more",
                        &message.id.0,
                        plus_icon(text_secondary()),
                        cx.listener(move |this, _, _, cx| {
                            this.react_to_message(message_id_for_picker.clone(), cx);
                        }),
                    ))
                    .child(
                        div()
                            .w(px(1.))
                            .h(px(16.))
                            .bg(shell_border())
                            .flex_shrink_0(),
                    )
                    .child(Self::toolbar_icon_button(
                        "message-thread",
                        &message.id.0,
                        thread_icon(text_secondary()),
                        cx.listener(move |this, _, window, cx| {
                            this.open_thread(message_id_for_thread_btn.clone(), window, cx);
                        }),
                    ))
                    .child(Self::toolbar_icon_button(
                        "message-copy-link",
                        &message.id.0,
                        link_icon(text_secondary()),
                        cx.listener(move |this, _, _, cx| {
                            this.copy_message_link(message_id_for_copy_link_btn.clone(), cx);
                        }),
                    ))
                    .when(is_own_message, |toolbar| {
                        toolbar.child(Self::toolbar_icon_button(
                            "message-delete",
                            &message.id.0,
                            close_icon(danger()),
                            cx.listener(move |this, _, _, cx| {
                                this.delete_message(message_id_for_delete_btn.clone(), cx);
                            }),
                        ))
                    });

                let cursor_left = if has_blocking_inline_actions {
                    None
                } else {
                    hover_anchor_x
                        .zip(hover_window_left)
                        .zip(hover_window_width)
                        .map(|((anchor_x, window_left), window_width)| {
                            let local_x = anchor_x - window_left;
                            let mut left = local_x - (TOOLBAR_W * 0.5);
                            if window_width.is_finite() && window_width > 0.0 {
                                let max_left = (window_width - TOOLBAR_W).max(0.0);
                                if left < 0.0 {
                                    left = 0.0;
                                } else if left > max_left {
                                    left = max_left;
                                }
                            } else if left < 0.0 {
                                left = 0.0;
                            }
                            left
                        })
                };

                let blocking_left = hover_window_width
                    .filter(|w| w.is_finite() && *w > 0.0)
                    .map(|window_width| {
                        let max_left = (window_width - TOOLBAR_W).max(0.0);
                        BLOCKING_LEFT.min(max_left)
                    })
                    .unwrap_or(BLOCKING_LEFT);

                let toolbar_wrapper = div()
                    .absolute()
                    // If the row already has clickable inline actions (reactions or thread badge),
                    // keep the picker at a fixed left offset so we don't cover them.
                    .when(has_blocking_inline_actions, |d| d.left(px(blocking_left)))
                    // Otherwise, place it near the cursor when we have enough geometry.
                    .when_some(cursor_left, |d, left| d.left(px(left)))
                    // If we don't yet have enough geometry, keep it hidden (avoids visible "jump").
                    .when(!has_blocking_inline_actions && cursor_left.is_none(), |d| {
                        d.right_0().opacity(0.)
                    })
                    .block_mouse_except_scroll()
                    .on_mouse_move(
                        cx.listener(move |this, event: &gpui::MouseMoveEvent, _, cx| {
                            this.clear_reaction_hover_tooltip(cx);
                            let cursor_x: f32 = event.position.x.into();
                            this.set_hovered_message_with_cursor_anchor(
                                message_id_for_hover.clone(),
                                cursor_x,
                                _is_thread,
                                cx,
                            );
                            cx.stop_propagation();
                        }),
                    )
                    .when(!is_hovered, |d| d.opacity(0.))
                    .child(inline_toolbar);
                if is_last_message {
                    deferred(toolbar_wrapper.top(px(-14.)))
                        .priority(10)
                        .into_any_element()
                } else {
                    deferred(toolbar_wrapper.bottom(px(-14.)))
                        .priority(10)
                        .into_any_element()
                }
            })
            .when(
                show_thread_reply_badge && message.thread_reply_count > 0,
                |container| {
                    let thread_root_id = message.id.clone();
                    container.child(
                        div()
                            .id(SharedString::from(format!(
                                "timeline-thread-badge-{}",
                                message.id.0
                            )))
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.open_thread(thread_root_id.clone(), window, cx);
                            }))
                            .cursor(gpui::CursorStyle::PointingHand)
                            .text_xs()
                            .text_color(rgb(accent()))
                            .flex()
                            .items_center()
                            .gap_1()
                            .child(thread_icon(accent()))
                            .child("View thread"),
                    )
                },
            )
            .child(Self::render_send_state(
                &message.id,
                &message.send_state,
                cx,
            ))
            .into_any_element()
    }

    fn message_caption_text(message: &crate::domain::message::MessageRecord) -> Option<String> {
        let caption = message
            .fragments
            .iter()
            .map(|fragment| match fragment {
                MessageFragment::Text(text)
                | MessageFragment::Code(text)
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
        message: &crate::domain::message::MessageRecord,
        find_query: Option<&str>,
        affinity_index: &HashMap<UserId, Affinity>,
        current_user_id: Option<&UserId>,
        emoji_index: &HashMap<String, InlineEmojiRender>,
        emoji_source_index: &HashMap<String, InlineEmojiRender>,
        message_memo: Option<&MessageRenderMemo>,
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
            return Self::render_inline_fragments(
                &message.id.0,
                format!("timeline-{}-inline", message.id.0),
                &message.fragments,
                message_memo,
                affinity_index,
                current_user_id,
                emoji_index,
                emoji_source_index,
                emoji_only,
                find_query,
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
                let key = format!("timeline-{}-inline-group-{}", message.id.0, group_ix);
                rendered.push(Self::render_inline_fragments(
                    &message.id.0,
                    key,
                    &inline_group,
                    None,
                    affinity_index,
                    current_user_id,
                    emoji_index,
                    emoji_source_index,
                    emoji_only,
                    find_query,
                    selectable_texts,
                    cx,
                ));
                inline_group.clear();
                group_ix = group_ix.saturating_add(1);
            }

            let precomputed_segments = message_memo
                .and_then(|memo| memo.text_segments.get(index))
                .and_then(|segments| segments.as_deref());
            rendered.push(Self::render_fragment(
                &message.id.0,
                index,
                fragment,
                false,
                affinity_index,
                current_user_id,
                emoji_index,
                emoji_source_index,
                emoji_only,
                find_query,
                precomputed_segments,
                selectable_texts,
                cx,
            ));
        }
        if !inline_group.is_empty() {
            let key = format!("timeline-{}-inline-group-{}", message.id.0, group_ix);
            rendered.push(Self::render_inline_fragments(
                &message.id.0,
                key,
                &inline_group,
                None,
                affinity_index,
                current_user_id,
                emoji_index,
                emoji_source_index,
                emoji_only,
                find_query,
                selectable_texts,
                cx,
            ));
        }
        div()
            .flex()
            .flex_col()
            .gap_0p5()
            .children(rendered)
            .into_any_element()
    }

    fn render_link_previews(
        message_key: &str,
        previews: &[LinkPreview],
        tight_bottom_spacing: bool,
        video_render_cache: &HashMap<String, Arc<RenderImage>>,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        const MAX_VISIBLE: usize = 3;
        let hidden = previews.len().saturating_sub(MAX_VISIBLE);
        let visible = previews
            .iter()
            .take(MAX_VISIBLE)
            .cloned()
            .collect::<Vec<_>>();
        div()
            .flex()
            .flex_col()
            .gap_1()
            .pt_1()
            .when(!tight_bottom_spacing, |container| container.pb_0p5())
            .children(visible.into_iter().enumerate().map(|(index, preview)| {
                let url = preview.url.clone();
                let element_id =
                    SharedString::from(format!("timeline-link-preview-{message_key}-{index}"));
                let image_element_id = SharedString::from(format!(
                    "timeline-link-preview-image-{message_key}-{index}"
                ));

                if preview.is_media {
                    let media_source = preview
                        .thumbnail_asset
                        .clone()
                        .unwrap_or_else(|| preview.url.clone());
                    let (media_width, media_height) = Self::media_frame_size(
                        preview.media_width,
                        preview.media_height,
                        360.0,
                        300.0,
                    );
                    return div()
                        .id(element_id)
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
                                    .rounded_md()
                                    .object_fit(ObjectFit::Contain)
                                    .flex_shrink_0()
                                    .min_w(px(16.))
                                    .min_h(px(16.))
                            } else {
                                img(SharedString::from(media_source))
                                    .id(image_element_id.clone())
                                    .w(px(media_width))
                                    .h(px(media_height))
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
                                    })
                            }
                        })
                        .when(preview.is_video, |container| {
                            let key_url =
                                preview.video_url.as_deref().unwrap_or(preview.url.as_str());
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
                                        .px_1p5()
                                        .py_0p5()
                                        .rounded_sm()
                                        .bg(glass_surface_dark())
                                        .border_1()
                                        .border_color(shell_border())
                                        .text_xs()
                                        .text_color(rgb(text_primary()))
                                        .child("video"),
                                )
                            }
                        })
                        .into_any_element();
                }

                let site = preview.site.clone().unwrap_or_else(|| "link".to_string());
                let title = preview.title.unwrap_or_else(|| preview.url.clone());
                div()
                    .id(element_id)
                    .on_click(cx.listener(move |this, _, window, cx| {
                        this.open_url_or_deep_link(&url, window, cx);
                    }))
                    .cursor(gpui::CursorStyle::PointingHand)
                    .border_l_2()
                    .border_color(rgb(accent()))
                    .pl_2()
                    .py_0p5()
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
        let mut width = width.unwrap_or(320) as f32;
        let mut height = height.unwrap_or(200) as f32;
        if width <= 1.0 || height <= 1.0 {
            return (max_width.min(320.0), max_height.min(200.0));
        }
        let scale = (max_width / width).min(max_height / height).min(1.0);
        width *= scale;
        height *= scale;
        (width.max(120.0), height.max(90.0))
    }

    fn toolbar_icon_button(
        prefix: &str,
        message_key: &str,
        icon: AnyElement,
        listener: impl Fn(&gpui::ClickEvent, &mut gpui::Window, &mut gpui::App) + 'static,
    ) -> AnyElement {
        div()
            .id(SharedString::from(format!("{prefix}-{message_key}")))
            .on_click(listener)
            .w(px(26.))
            .h(px(26.))
            .rounded_md()
            .flex()
            .items_center()
            .justify_center()
            .hover(|s| s.bg(subtle_surface()))
            .child(icon)
            .into_any_element()
    }

    fn render_inline_fragments(
        _message_key: &str,
        render_key: String,
        fragments: &[MessageFragment],
        message_memo: Option<&MessageRenderMemo>,
        affinity_index: &HashMap<UserId, Affinity>,
        current_user_id: Option<&UserId>,
        emoji_index: &HashMap<String, InlineEmojiRender>,
        emoji_source_index: &HashMap<String, InlineEmojiRender>,
        emoji_only: bool,
        find_query: Option<&str>,
        selectable_texts: &mut HashMap<String, Entity<SelectableText>>,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        let mut combined = String::new();
        let mut link_ranges = Vec::new();
        let mut styled_ranges = Vec::new();
        let mut inline_attachments = Vec::new();

        for (index, fragment) in fragments.iter().enumerate() {
            match fragment {
                MessageFragment::Text(text) => {
                    let segments: Cow<'_, [InlineTextSegment]> = if let Some(segments) =
                        message_memo
                            .and_then(|memo| memo.text_segments.get(index))
                            .and_then(|segments| segments.as_deref())
                    {
                        Cow::Borrowed(segments)
                    } else {
                        Cow::Owned(inline_text_segments(text))
                    };

                    for segment in segments.iter() {
                        match segment {
                            InlineTextSegment::Text(value) => combined.push_str(value),
                            InlineTextSegment::InlineCode(code) => {
                                Self::push_inline_code_span(
                                    &mut combined,
                                    &mut styled_ranges,
                                    code,
                                );
                            }
                            InlineTextSegment::Link(url) => {
                                let start = combined.len();
                                combined.push_str(url);
                                link_ranges.push(LinkRange {
                                    byte_range: start..combined.len(),
                                    url: url.clone(),
                                });
                            }
                        }
                    }
                }
                MessageFragment::InlineCode(code) => {
                    Self::push_inline_code_span(&mut combined, &mut styled_ranges, code);
                }
                MessageFragment::Emoji { alias, source_ref } => {
                    if let Some(render) = Self::resolved_emoji_render(
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
                            Self::push_inline_asset_emoji_attachment(
                                &mut combined,
                                &mut inline_attachments,
                                alias,
                                asset_path,
                                emoji_only,
                            );
                            continue;
                        }
                    }
                    combined.push_str(&Self::inline_emoji_text(
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
                        Self::mention_colors_for_user(affinity_index, current_user_id, user_id);
                    Self::push_styled_span(
                        &mut combined,
                        &mut styled_ranges,
                        &label,
                        foreground,
                        background,
                    );
                    link_ranges.push(LinkRange {
                        byte_range: start..combined.len(),
                        url: format!("kbui-mention:{}", user_id.0),
                    });
                }
                MessageFragment::ChannelMention { name } => {
                    let label = format!("#{name}");
                    let start = combined.len();
                    Self::push_styled_span(
                        &mut combined,
                        &mut styled_ranges,
                        &label,
                        accent(),
                        accent_soft(),
                    );
                    link_ranges.push(LinkRange {
                        byte_range: start..combined.len(),
                        url: format!("kbui-channel:{name}"),
                    });
                }
                MessageFragment::BroadcastMention(BroadcastKind::Here) => {
                    Self::push_styled_span(
                        &mut combined,
                        &mut styled_ranges,
                        "@here",
                        warning(),
                        warning_soft(),
                    );
                }
                MessageFragment::BroadcastMention(BroadcastKind::All) => {
                    Self::push_styled_span(
                        &mut combined,
                        &mut styled_ranges,
                        "@channel",
                        warning(),
                        warning_soft(),
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
                MessageFragment::Code(text) | MessageFragment::Quote(text) => {
                    combined.push_str(text);
                }
            }
        }
        Self::append_find_query_highlight_ranges(&combined, find_query, &mut styled_ranges);

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
        message_memo: Option<&MessageRenderMemo>,
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

        for (index, fragment) in fragments.iter().enumerate() {
            if Self::fragment_requires_asset_emoji_box(fragment, emoji_index, emoji_source_index) {
                let MessageFragment::Emoji { alias, source_ref } = fragment else {
                    continue;
                };
                if let Some(render) = Self::resolved_emoji_render(
                    alias,
                    source_ref.as_ref(),
                    emoji_index,
                    emoji_source_index,
                )
                    && let Some(asset_path) = render.asset_path.as_ref() {
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
                    let segments: Cow<'_, [InlineTextSegment]> = if let Some(segments) =
                        message_memo
                            .and_then(|memo| memo.text_segments.get(index))
                            .and_then(|segments| segments.as_deref())
                    {
                        Cow::Borrowed(segments)
                    } else {
                        Cow::Owned(inline_text_segments(text))
                    };
                    for segment in segments.iter() {
                        match segment {
                            InlineTextSegment::Text(value) => combined.push_str(value),
                            InlineTextSegment::InlineCode(code) => {
                                Self::push_inline_code_span(
                                    &mut combined,
                                    &mut styled_ranges,
                                    code,
                                );
                            }
                            InlineTextSegment::Link(url) => {
                                let start = combined.len();
                                combined.push_str(url);
                                link_ranges.push(LinkRange {
                                    byte_range: start..combined.len(),
                                    url: url.clone(),
                                });
                            }
                        }
                    }
                }
                MessageFragment::InlineCode(code) => {
                    Self::push_inline_code_span(&mut combined, &mut styled_ranges, code);
                }
                MessageFragment::Emoji { alias, source_ref } => {
                    combined.push_str(&Self::inline_emoji_text(
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
                        Self::mention_colors_for_user(affinity_index, current_user_id, user_id);
                    Self::push_styled_span(
                        &mut combined,
                        &mut styled_ranges,
                        &label,
                        foreground,
                        background,
                    );
                    link_ranges.push(LinkRange {
                        byte_range: start..combined.len(),
                        url: format!("kbui-mention:{}", user_id.0),
                    });
                }
                MessageFragment::ChannelMention { name } => {
                    let label = format!("#{name}");
                    Self::push_styled_span(
                        &mut combined,
                        &mut styled_ranges,
                        &label,
                        accent(),
                        accent_soft(),
                    );
                }
                MessageFragment::BroadcastMention(BroadcastKind::Here) => {
                    Self::push_styled_span(
                        &mut combined,
                        &mut styled_ranges,
                        "@here",
                        warning(),
                        warning_soft(),
                    );
                }
                MessageFragment::BroadcastMention(BroadcastKind::All) => {
                    Self::push_styled_span(
                        &mut combined,
                        &mut styled_ranges,
                        "@channel",
                        warning(),
                        warning_soft(),
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
                MessageFragment::Code(text) | MessageFragment::Quote(text) => {
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
                    format!("timeline-{message_key}-inline-hybrid"),
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
                    format!("timeline-{message_key}-inline-hybrid"),
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
        find_query: Option<&str>,
        precomputed_segments: Option<&[InlineTextSegment]>,
        selectable_texts: &mut HashMap<String, Entity<SelectableText>>,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        match fragment {
            MessageFragment::Text(text) => {
                let segments: Cow<'_, [InlineTextSegment]> =
                    if let Some(segments) = precomputed_segments {
                        Cow::Borrowed(segments)
                    } else {
                        Cow::Owned(inline_text_segments(text))
                    };

                if emoji_only {
                    return div()
                        .text_size(px(48.))
                        .line_height(px(52.))
                        .child(text.clone())
                        .into_any_element();
                }

                {
                    let mut combined = String::new();
                    let mut link_ranges: Vec<LinkRange> = Vec::new();
                    let mut styled_ranges: Vec<StyledRange> = Vec::new();

                    for segment in segments.iter() {
                        match segment {
                            InlineTextSegment::Text(value) => combined.push_str(value),
                            InlineTextSegment::InlineCode(code) => {
                                Self::push_inline_code_span(
                                    &mut combined,
                                    &mut styled_ranges,
                                    code,
                                );
                            }
                            InlineTextSegment::Link(url) => {
                                let start = combined.len();
                                combined.push_str(url);
                                link_ranges.push(LinkRange {
                                    byte_range: start..combined.len(),
                                    url: url.clone(),
                                });
                            }
                        }
                    }

                    Self::append_find_query_highlight_ranges(
                        &combined,
                        find_query,
                        &mut styled_ranges,
                    );
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
                            format!("timeline-{message_key}-text-{index}"),
                            parsed.text,
                            parsed.link_ranges,
                            parsed.styled_ranges,
                            cx,
                        )
                    } else {
                        resolve_selectable_text(
                            selectable_texts,
                            format!("timeline-{message_key}-text-{index}"),
                            parsed.text,
                            parsed.link_ranges,
                            parsed.styled_ranges,
                            cx,
                        )
                    };
                    div()
                        .when(!compact_inline, |container| {
                            container.w_full().min_w(px(0.))
                        })
                        .when(compact_inline, |container| container.flex_1().min_w(px(0.)))
                        .text_sm()
                        .line_height(px(22.))
                        .text_color(rgb(text_primary()))
                        .child(selectable)
                        .into_any_element()
                }
            }
            MessageFragment::InlineCode(code) => {
                let mut styled_ranges = Vec::new();
                Self::append_find_query_highlight_ranges(code, find_query, &mut styled_ranges);
                let selectable = if compact_inline {
                    resolve_selectable_text_inline(
                        selectable_texts,
                        format!("timeline-{message_key}-inline-code-{index}"),
                        code.clone(),
                        Vec::new(),
                        styled_ranges.clone(),
                        cx,
                    )
                } else {
                    resolve_selectable_text(
                        selectable_texts,
                        format!("timeline-{message_key}-inline-code-{index}"),
                        code.clone(),
                        Vec::new(),
                        styled_ranges,
                        cx,
                    )
                };
                div()
                    .px_1()
                    .py_0p5()
                    .rounded_sm()
                    .bg(subtle_surface().opacity(0.6))
                    .text_sm()
                    .line_height(px(22.))
                    .text_color(rgb(text_primary()))
                    .child(selectable)
                    .into_any_element()
            }
            MessageFragment::Emoji { alias, source_ref } => {
                if let Some(render) = Self::resolved_emoji_render(
                    alias,
                    source_ref.as_ref(),
                    emoji_index,
                    emoji_source_index,
                ) {
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
                    .when(!compact_inline, |container| {
                        container.w_full().min_w(px(0.))
                    })
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
                    Self::mention_colors_for_user(affinity_index, current_user_id, user_id);
                Self::render_linked_styled_fragment_text(
                    message_key,
                    index,
                    &label,
                    format!("kbui-mention:{}", user_id.0),
                    foreground,
                    background,
                    compact_inline,
                    selectable_texts,
                    cx,
                )
            }
            MessageFragment::ChannelMention { name } => {
                let label = format!("#{name}");
                Self::render_linked_styled_fragment_text(
                    message_key,
                    index,
                    &label,
                    format!("kbui-channel:{name}"),
                    accent(),
                    accent_soft(),
                    compact_inline,
                    selectable_texts,
                    cx,
                )
            }
            MessageFragment::BroadcastMention(BroadcastKind::Here) => {
                Self::render_styled_fragment_text(
                    message_key,
                    index,
                    "@here".to_string(),
                    warning(),
                    warning_soft(),
                    compact_inline,
                    selectable_texts,
                    cx,
                )
            }
            MessageFragment::BroadcastMention(BroadcastKind::All) => {
                Self::render_styled_fragment_text(
                    message_key,
                    index,
                    "@channel".to_string(),
                    warning(),
                    warning_soft(),
                    compact_inline,
                    selectable_texts,
                    cx,
                )
            }
            MessageFragment::Link { url, display } => {
                let selectable = if compact_inline {
                    resolve_selectable_text_inline(
                        selectable_texts,
                        format!("timeline-{message_key}-link-{index}"),
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
                        format!("timeline-{message_key}-link-{index}"),
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
                    .when(!compact_inline, |container| {
                        container.w_full().min_w(px(0.))
                    })
                    .when(compact_inline, |container| container.flex_1().min_w(px(0.)))
                    .text_sm()
                    .line_height(px(22.))
                    .text_color(rgb(text_primary()))
                    .child(selectable)
                    .into_any_element()
            }
            MessageFragment::Code(code) => div()
                .w_full()
                .min_w(px(0.))
                .rounded_md()
                .bg(rgb(mention_soft()))
                .px_2()
                .py_1()
                .text_sm()
                .child(resolve_selectable_text(
                    selectable_texts,
                    format!("timeline-{message_key}-code-{index}"),
                    code.clone(),
                    Vec::new(),
                    Vec::new(),
                    cx,
                ))
                .into_any_element(),
            MessageFragment::Quote(quote) => div()
                .w_full()
                .min_w(px(0.))
                .relative()
                .pl_3()
                .pb_1()
                .text_sm()
                .text_color(rgb(text_secondary()))
                .child(
                    div()
                        .absolute()
                        .left_0()
                        .top_0()
                        .bottom_0()
                        .w(px(2.))
                        .rounded_full()
                        .bg(rgb(text_primary())),
                )
                .child(resolve_selectable_text(
                    selectable_texts,
                    format!("timeline-{message_key}-quote-{index}"),
                    quote.clone(),
                    Vec::new(),
                    Vec::new(),
                    cx,
                ))
                .into_any_element(),
        }
    }

    fn render_send_state(
        message_id: &crate::domain::ids::MessageId,
        send_state: &MessageSendState,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        match send_state {
            MessageSendState::Sent => div().into_any_element(),
            MessageSendState::Pending => div()
                .text_xs()
                .italic()
                .text_color(rgb(text_secondary()))
                .child("Sending…")
                .into_any_element(),
            MessageSendState::Failed => {
                let retry_message_id = message_id.clone();
                div()
                    .flex()
                    .items_center()
                    .gap_1()
                    .child(
                        div()
                            .text_xs()
                            .text_color(rgb(danger()))
                            .child("Failed to send"),
                    )
                    .child(
                        div()
                            .id(SharedString::from(format!(
                                "timeline-message-retry-{}",
                                message_id.0
                            )))
                            .text_xs()
                            .text_color(rgb(accent()))
                            .cursor(gpui::CursorStyle::PointingHand)
                            .on_click(cx.listener(move |this, _, _, cx| {
                                this.retry_failed_message_send(retry_message_id.clone(), cx);
                            }))
                            .child("· Retry"),
                    )
                    .into_any_element()
            }
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
                format!("timeline-{message_key}-styled-{index}"),
                value,
                Vec::new(),
                styled_ranges,
                cx,
            )
        } else {
            resolve_selectable_text(
                selectable_texts,
                format!("timeline-{message_key}-styled-{index}"),
                value,
                Vec::new(),
                styled_ranges,
                cx,
            )
        };
        div()
            .when(!compact_inline, |container| {
                container.w_full().min_w(px(0.))
            })
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
                format!("timeline-{message_key}-channel-{index}"),
                value.to_string(),
                link_ranges,
                styled_ranges,
                cx,
            )
        } else {
            resolve_selectable_text(
                selectable_texts,
                format!("timeline-{message_key}-channel-{index}"),
                value.to_string(),
                link_ranges,
                styled_ranges,
                cx,
            )
        };
        div()
            .when(!compact_inline, |container| {
                container.w_full().min_w(px(0.))
            })
            .when(compact_inline, |container| container.flex_1().min_w(px(0.)))
            .text_sm()
            .line_height(px(22.))
            .text_color(rgb(text_primary()))
            .child(selectable)
            .into_any_element()
    }

    fn append_find_query_highlight_ranges(
        combined: &str,
        find_query: Option<&str>,
        styled_ranges: &mut Vec<StyledRange>,
    ) {
        let Some(query) = find_query.map(str::trim).filter(|value| !value.is_empty()) else {
            return;
        };
        let haystack = combined.to_ascii_lowercase();
        let needle = query.to_ascii_lowercase();
        if needle.is_empty() {
            return;
        }
        let mut search_from = 0usize;
        while let Some(relative_start) = haystack[search_from..].find(&needle) {
            let start = search_from + relative_start;
            let end = start + needle.len();
            if start < end && end <= combined.len() {
                styled_ranges.push(StyledRange {
                    byte_range: start..end,
                    color: None,
                    background_color: Some(warning_soft()),
                    bold: false,
                    italic: false,
                    strikethrough: false,
                });
            }
            search_from = end;
            if search_from >= haystack.len() {
                break;
            }
        }
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

    fn push_inline_code_span(
        combined: &mut String,
        styled_ranges: &mut Vec<StyledRange>,
        code: &str,
    ) {
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
        combined.push_str(Self::inline_attachment_placeholder(emoji_only));
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
        if let Some(unicode) =
            Self::resolved_emoji_render(alias, source_ref, emoji_index, emoji_source_index)
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
        Self::resolved_emoji_render(alias, source_ref.as_ref(), emoji_index, emoji_source_index)
            .map(|render| render.unicode.is_none() && render.asset_path.is_some())
            .unwrap_or(false)
    }
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
        } else if previous_underscore {
            None
        } else {
            previous_underscore = true;
            Some('_')
        };
        if let Some(value) = normalized {
            out.push(value);
        }
    }
    out.trim_matches('_').to_string()
}

fn reaction_hover_text(reaction: &MessageReactionRender) -> Option<String> {
    if reaction.actors.is_empty() {
        return None;
    }
    let mut labels = reaction
        .actors
        .iter()
        .map(|actor| {
            if actor.display_name.eq_ignore_ascii_case(&actor.user_id) {
                actor.user_id.clone()
            } else {
                format!("{} (@{})", actor.display_name, actor.user_id)
            }
        })
        .collect::<Vec<_>>();
    labels.sort();
    labels.dedup();
    if labels.is_empty() {
        return None;
    }
    const MAX_NAMES: usize = 8;
    if labels.len() > MAX_NAMES {
        let remaining = labels.len() - MAX_NAMES;
        labels.truncate(MAX_NAMES);
        return Some(format!("{} (+{} more)", labels.join(", "), remaining));
    }
    Some(labels.join(", "))
}

fn author_role_crown_badge(_role: TeamAuthorRole) -> AnyElement {
    div()
        .relative()
        .top(px(-1.))
        .flex()
        .items_center()
        .text_color(rgb(warning()))
        .child(crown_icon(warning()))
        .into_any_element()
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

fn message_is_emoji_only(fragments: &[MessageFragment], has_attachments: bool) -> bool {
    !fragments.is_empty()
        && !has_attachments
        && fragments.iter().all(|fragment| match fragment {
            MessageFragment::Emoji { .. } => true,
            MessageFragment::Text(text) => is_emoji_only(text),
            _ => false,
        })
}

fn custom_emoji_size_px(emoji_only: bool) -> f32 {
    if emoji_only {
        CUSTOM_EMOJI_EMOJI_ONLY_SIZE_PX
    } else {
        CUSTOM_EMOJI_INLINE_SIZE_PX
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_emoji_alias_resolves_cldr_name_variant() {
        assert_eq!(
            standard_emoji_for_alias("rolling_on_the_floor_laughing"),
            Some("🤣")
        );
    }

    #[test]
    fn inline_text_segments_preserves_shortcodes_in_text() {
        let segments = inline_text_segments("it is funny :rolling_on_the_floor_laughing:");
        let rendered = segments
            .into_iter()
            .map(|segment| match segment {
                InlineTextSegment::Text(text) => text,
                InlineTextSegment::InlineCode(text) => text,
                InlineTextSegment::Link(text) => text,
            })
            .collect::<String>();
        assert_eq!(
            rendered,
            "it is funny :rolling_on_the_floor_laughing:".to_string()
        );
    }

    #[test]
    fn reaction_hover_text_includes_display_names_and_usernames() {
        let text = reaction_hover_text(&MessageReactionRender {
            emoji: ":thumbsup:".to_string(),
            source_ref: None,
            count: 2,
            actors: vec![
                crate::models::timeline_model::ReactionActorRender {
                    user_id: "alice".to_string(),
                    display_name: "Alice A".to_string(),
                },
                crate::models::timeline_model::ReactionActorRender {
                    user_id: "bob".to_string(),
                    display_name: "bob".to_string(),
                },
            ],
            reacted_by_me: false,
        });
        assert_eq!(text, Some("Alice A (@alice), bob".to_string()));
    }

    #[test]
    fn inline_emoji_prefers_source_ref_render_over_alias_index() {
        let source_ref = crate::domain::message::EmojiSourceRef {
            backend_id: crate::domain::backend::BackendId::new("keybase"),
            ref_key: "emoji:conv=abcd:msg=42".to_string(),
        };
        let mut emoji_index = HashMap::new();
        emoji_index.insert(
            "nice".to_string(),
            InlineEmojiRender {
                alias: "nice".to_string(),
                unicode: Some("🙂".to_string()),
                asset_path: None,
            },
        );
        let mut emoji_source_index = HashMap::new();
        emoji_source_index.insert(
            source_ref.cache_key(),
            InlineEmojiRender {
                alias: "nice".to_string(),
                unicode: Some("😎".to_string()),
                asset_path: None,
            },
        );

        let rendered = TimelineList::inline_emoji_text(
            "nice",
            Some(&source_ref),
            &emoji_index,
            &emoji_source_index,
        );

        assert_eq!(rendered, "😎");
    }

    #[test]
    fn message_is_emoji_only_requires_no_attachments() {
        let fragments = vec![MessageFragment::Emoji {
            alias: "sbx".to_string(),
            source_ref: None,
        }];
        assert!(message_is_emoji_only(&fragments, false));
        assert!(!message_is_emoji_only(&fragments, true));
    }

    #[test]
    fn custom_emoji_size_uses_emoji_only_constant() {
        assert_eq!(custom_emoji_size_px(false), CUSTOM_EMOJI_INLINE_SIZE_PX);
        assert_eq!(custom_emoji_size_px(true), CUSTOM_EMOJI_EMOJI_ONLY_SIZE_PX);
    }
}
