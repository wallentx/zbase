use crate::{
    domain::{
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
        CUSTOM_EMOJI_REACTION_SIZE_PX, accent, accent_soft, activity_icon,
        app_window::{AppWindow, video_preview_cache_key},
        arrow_left_icon, arrow_right_icon, attachment_display_label, attachment_image_source,
        attachment_lightbox_source,
        avatar::{Avatar, default_avatar_background},
        badge, crown_icon, danger, danger_soft, emoji_icon, glass_surface_dark, hash_icon,
        inline_markdown::{InlineMarkdownConfig, apply_inline_markdown, remap_source_byte_range},
        mention, mention_soft, more_icon, panel_alt_bg, pin_icon, plus_icon,
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
    AnyElement, Context, Entity, FontWeight, ImageSource, InteractiveElement, IntoElement,
    ListState, ObjectFit, ParentElement, RenderImage, SharedString, StatefulInteractiveElement,
    Styled, StyledImage, div, img, list, px, rgb,
};
use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{Arc, OnceLock},
};

#[derive(Default)]
pub struct TimelineList;

const ROW_RENDER_CACHE_MAX_ENTRIES: usize = 512;

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
            .map_or(true, |entry| entry.row_ptr != row_ptr);

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
            .px_4()
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
        find_query: Option<&str>,
        video_render_cache: &HashMap<String, Arc<RenderImage>>,
        selectable_texts: &mut HashMap<String, Entity<SelectableText>>,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        let row_element = match row {
            TimelineRow::DateDivider(label) => div()
                .py_2()
                .flex()
                .items_center()
                .justify_center()
                .child(badge(label.clone(), panel_alt_bg(), text_secondary()))
                .into_any_element(),
            TimelineRow::UnreadDivider(label) => div()
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
                    .pl(px(16.))
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
            TimelineRow::TypingIndicator(label) => div()
                .pl(px(48.))
                .py_0p5()
                .text_xs()
                .text_color(rgb(text_secondary()))
                .child(label.clone())
                .into_any_element(),
            TimelineRow::LoadingIndicator(label) => div()
                .w_full()
                .min_w(px(0.))
                .flex()
                .items_center()
                .gap_2()
                .py_1()
                .pl(px(16.))
                .text_xs()
                .text_color(rgb(text_secondary()))
                .child(activity_icon(accent()))
                .child(label.clone())
                .into_any_element(),
            TimelineRow::Message(message_row) => Self::render_message_row(
                timeline,
                message_row,
                message_memo,
                show_thread_reply_badge,
                find_query,
                video_render_cache,
                selectable_texts,
                cx,
            ),
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
        find_query: Option<&str>,
        video_render_cache: &HashMap<String, Arc<RenderImage>>,
        selectable_texts: &mut HashMap<String, Entity<SelectableText>>,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        let timestamp = message_timestamp_label(row.message.timestamp_ms);
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

        if row.show_header {
            let mut header_children = vec![
                div()
                    .text_sm()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(rgb(text_primary()))
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

        content = content.child(Self::render_message(
            &row.message,
            timeline.highlighted_message_id.as_ref() == Some(&row.message.id),
            find_query,
            &timeline.emoji_index,
            &timeline.reaction_index,
            show_thread_reply_badge,
            row.message.edited.is_some(),
            video_render_cache,
            message_memo,
            selectable_texts,
            cx,
        ));

        if row.show_header {
            div()
                .w_full()
                .min_w(px(0.))
                .flex()
                .gap_3()
                .items_start()
                .pt_0p5()
                .child(div().flex_shrink_0().child(Avatar::render(
                    &row.author.display_name,
                    row.author.avatar_asset.as_deref(),
                    32.,
                    default_avatar_background(&row.author.display_name),
                    text_primary(),
                )))
                .child(content)
                .into_any_element()
        } else {
            div()
                .w_full()
                .min_w(px(0.))
                .flex()
                .gap_3()
                .items_start()
                .child(div().w(px(32.)).flex_shrink_0())
                .child(content)
                .into_any_element()
        }
    }

    fn render_message(
        message: &crate::domain::message::MessageRecord,
        highlighted: bool,
        find_query: Option<&str>,
        emoji_index: &HashMap<String, InlineEmojiRender>,
        reaction_index: &HashMap<crate::domain::ids::MessageId, Vec<MessageReactionRender>>,
        show_thread_reply_badge: bool,
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
        let message_id = message.id.clone();
        let message_id_for_thread = message.id.clone();
        let message_id_for_more = message.id.clone();

        let group_name = SharedString::from(format!("msg-{}", message.id.0));

        div()
            .id(SharedString::from(format!(
                "timeline-message-{}",
                message.id.0
            )))
            .group(group_name.clone())
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
                if image_attachment_message {
                    div().into_any_element()
                } else {
                    Self::render_message_fragments(
                        message,
                        find_query,
                        emoji_index,
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
                                        360.0,
                                        300.0,
                                    );
                                    let lightbox_source = attachment_lightbox_source(attachment);
                                    let caption_text = Self::message_caption_text(message);
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
                        emoji_index,
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
            .when(
                reaction_index
                    .get(&message.id)
                    .is_some_and(|reactions| !reactions.is_empty()),
                |container| {
                    let reactions: Cow<'_, [MessageReactionRender]> =
                        if let Some(memo) = message_memo {
                            Cow::Borrowed(&memo.sorted_reactions)
                        } else {
                            let mut reactions =
                                reaction_index.get(&message.id).cloned().unwrap_or_default();
                            reactions.sort_by(|left, right| left.emoji.cmp(&right.emoji));
                            Cow::Owned(reactions)
                        };
                    container.child(
                        div().flex().flex_wrap().gap_1().pt_1().children(
                            reactions.iter().cloned().enumerate().map(
                                |(reaction_index, reaction)| {
                                    let alias =
                                        reaction.emoji.trim_matches(':').to_ascii_lowercase();
                                    let resolved = emoji_index.get(&alias);
                                    let reaction_group = SharedString::from(format!(
                                        "reaction-hover-{}-{}-{}",
                                        message.id.0,
                                        reaction_index,
                                        normalize_emoji_alias(&reaction.emoji)
                                    ));
                                    let hover_text = reaction_hover_text(&reaction);

                                    let emoji_element: AnyElement = if let Some(render) = resolved {
                                        if let Some(unicode) = &render.unicode {
                                            div()
                                                .text_sm()
                                                .child(unicode.clone())
                                                .into_any_element()
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
                                    } else if let Some(standard) = standard_emoji_for_alias(&alias)
                                    {
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
                                        .border_color(shell_border())
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
                                        .relative()
                                        .group(reaction_group.clone())
                                        .child(chip)
                                        .when(hover_text.is_some(), |container| {
                                            let hover_text = hover_text.clone().unwrap_or_default();
                                            let hover_width = reaction_hover_width_px(&hover_text);
                                            container.child(
                                                div()
                                                    .absolute()
                                                    .left_0()
                                                    .top(px(-30.))
                                                    .opacity(0.)
                                                    .group_hover(reaction_group, |s| s.opacity(1.))
                                                    .w(px(hover_width))
                                                    .rounded_md()
                                                    .bg(glass_surface_dark())
                                                    .border_1()
                                                    .border_color(shell_border())
                                                    .px_2()
                                                    .py_1()
                                                    .text_xs()
                                                    .text_color(rgb(text_primary()))
                                                    .child(hover_text),
                                            )
                                        });
                                    chip_with_hover.into_any_element()
                                },
                            ),
                        ),
                    )
                },
            )
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
                            .text_xs()
                            .text_color(rgb(accent()))
                            .child(format!(
                                "{} {}",
                                message.thread_reply_count,
                                if message.thread_reply_count == 1 {
                                    "reply"
                                } else {
                                    "replies"
                                }
                            )),
                    )
                },
            )
            .child(Self::render_send_state(&message.send_state))
            .child(
                div()
                    .absolute()
                    .right_0()
                    .top(px(-4.))
                    .opacity(0.)
                    .group_hover(group_name, |s| s.opacity(1.))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .rounded_md()
                            .bg(glass_surface_dark())
                            .border_1()
                            .border_color(shell_border())
                            .px_0p5()
                            .py_0p5()
                            .gap_0p5()
                            .child(Self::toolbar_icon_button(
                                "message-react",
                                &message.id.0,
                                emoji_icon(text_secondary()),
                                cx.listener(move |this, _, _, cx| {
                                    this.react_to_message(message_id.clone(), cx);
                                }),
                            ))
                            .child(Self::toolbar_icon_button(
                                "message-thread",
                                &message.id.0,
                                thread_icon(text_secondary()),
                                cx.listener(move |this, _, window, cx| {
                                    this.open_thread(message_id_for_thread.clone(), window, cx);
                                }),
                            ))
                            .child(Self::toolbar_icon_button(
                                "message-more",
                                &message.id.0,
                                more_icon(text_secondary()),
                                cx.listener(move |this, _, _, cx| {
                                    this.open_message_context_menu(message_id_for_more.clone(), cx);
                                }),
                            )),
                    ),
            )
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
                MessageFragment::Emoji { alias } => format!(":{alias}:"),
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
        emoji_index: &HashMap<String, InlineEmojiRender>,
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
                emoji_index,
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
                    emoji_index,
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
                emoji_index,
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
                emoji_index,
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
        emoji_index: &HashMap<String, InlineEmojiRender>,
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
                MessageFragment::Emoji { alias } => {
                    let key = alias.to_ascii_lowercase();
                    if let Some(render) = emoji_index.get(&key) {
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
                    combined.push_str(&Self::inline_emoji_text(alias, emoji_index));
                }
                MessageFragment::Mention(user_id) => {
                    let label = format!("@{}", user_id.0);
                    Self::push_styled_span(
                        &mut combined,
                        &mut styled_ranges,
                        &label,
                        mention(),
                        mention_soft(),
                    );
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
        emoji_index: &HashMap<String, InlineEmojiRender>,
        emoji_only: bool,
        selectable_texts: &mut HashMap<String, Entity<SelectableText>>,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        let mut combined = String::new();
        let mut link_ranges = Vec::new();
        let mut styled_ranges = Vec::new();
        let mut asset_emojis: Vec<AnyElement> = Vec::new();

        for (index, fragment) in fragments.iter().enumerate() {
            if Self::fragment_requires_asset_emoji_box(fragment, emoji_index) {
                let MessageFragment::Emoji { alias } = fragment else {
                    continue;
                };
                let key = alias.to_ascii_lowercase();
                if let Some(render) = emoji_index.get(&key) {
                    if let Some(asset_path) = render.asset_path.as_ref() {
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
                MessageFragment::Emoji { alias } => {
                    combined.push_str(&Self::inline_emoji_text(alias, emoji_index));
                }
                MessageFragment::Mention(user_id) => {
                    let label = format!("@{}", user_id.0);
                    Self::push_styled_span(
                        &mut combined,
                        &mut styled_ranges,
                        &label,
                        mention(),
                        mention_soft(),
                    );
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
        emoji_index: &HashMap<String, InlineEmojiRender>,
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
            MessageFragment::Emoji { alias } => {
                let key = alias.to_ascii_lowercase();
                if let Some(render) = emoji_index.get(&key) {
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
            MessageFragment::Mention(user_id) => Self::render_styled_fragment_text(
                message_key,
                index,
                format!("@{}", user_id.0),
                mention(),
                mention_soft(),
                compact_inline,
                selectable_texts,
                cx,
            ),
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
                .bg(subtle_surface().opacity(0.5))
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

    fn inline_emoji_text(alias: &str, emoji_index: &HashMap<String, InlineEmojiRender>) -> String {
        let key = alias.to_ascii_lowercase();
        if let Some(unicode) = emoji_index
            .get(&key)
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
    ) -> bool {
        let MessageFragment::Emoji { alias } = fragment else {
            return false;
        };
        let key = alias.to_ascii_lowercase();
        emoji_index
            .get(&key)
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

fn reaction_hover_width_px(text: &str) -> f32 {
    // Approximate content-based width so the hover grows with text
    // without becoming unreasonably wide on very long lists.
    let chars = text.chars().count().clamp(20, 96);
    (chars as f32 * 7.2 + 18.0).clamp(190.0, 720.0)
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
        });
        assert_eq!(text, Some("Alice A (@alice), bob".to_string()));
    }

    #[test]
    fn message_is_emoji_only_requires_no_attachments() {
        let fragments = vec![MessageFragment::Emoji {
            alias: "sbx".to_string(),
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
