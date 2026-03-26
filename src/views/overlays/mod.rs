use crate::{
    domain::attachment::{AttachmentKind, AttachmentSource, AttachmentSummary},
    models::{
        emoji_picker_model::EmojiPickerModel,
        file_upload_model::FileUploadLightboxModel,
        new_chat_model::NewChatModel,
        notifications_model::NotificationsModel,
        overlay_model::{FullscreenImageOverlay, OverlayModel},
        profile_panel_model::ProfilePanelModel,
        quick_switcher_model::{QuickSwitcherModel, QuickSwitcherResult, QuickSwitcherResultKind},
    },
    state::state::EmojiRenderState,
    views::{
        accent, accent_soft, app_window::AppWindow, attachment_display_label, badge, card_shadow,
        dm_icon, hash_icon, image_source_from_attachment_source, input::TextField, modal_surface,
        panel_alt_bg, panel_surface, search_icon, shell_border, shell_border_strong,
        subtle_surface, text_primary, text_secondary, tint,
    },
};
use gpui::prelude::FluentBuilder;
use gpui::{
    AnyElement, App, AvailableSpace, Bounds, Context, Element, ElementId, Entity, FontWeight,
    GlobalElementId, InteractiveElement, IntoElement, LayoutId, ObjectFit, ParentElement, Pixels,
    SharedString, StatefulInteractiveElement, Style, Styled, StyledImage, TextRun, Window, div,
    img, px, relative, rgb, size,
};
use std::collections::HashMap;

mod emoji_picker;
mod new_chat;

const QUICK_SWITCHER_RENDER_LIMIT_UNREAD: usize = 14;
const QUICK_SWITCHER_RENDER_LIMIT_RECENT: usize = 14;
const QUICK_SWITCHER_RECENT_SHORTCUT_LIMIT: usize = 5;
const QUICK_SWITCHER_RENDER_LIMIT_CONVERSATIONS: usize = 3;
const QUICK_SWITCHER_RENDER_LIMIT_MESSAGES: usize = 10;

#[derive(Default)]
pub struct OverlayHost;

impl OverlayHost {
    pub fn render(
        &self,
        overlay: &OverlayModel,
        profile_panel: &ProfilePanelModel,
        _notifications: &NotificationsModel,
        quick_switcher: &QuickSwitcherModel,
        new_chat: &NewChatModel,
        quick_switcher_input: &Entity<TextField>,
        new_chat_input: &Entity<TextField>,
        file_upload_caption_input: &Entity<TextField>,
        emoji_picker_state: &EmojiPickerModel,
        emoji_picker_input: &Entity<TextField>,
        custom_emoji_index: Option<&HashMap<String, EmojiRenderState>>,
        supports_custom_emoji: bool,
        quick_switcher_indexing_status: Option<&str>,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        if overlay.active_modal.is_none()
            && !overlay.new_chat_open
            && !overlay.quick_switcher_open
            && !overlay.command_palette_open
            && !overlay.emoji_picker_open
            && overlay.fullscreen_image.is_none()
            && overlay.file_upload_lightbox.is_none()
            && overlay.profile_card_user_id.is_none()
        {
            return div().into_any_element();
        }

        let has_backdrop = overlay.new_chat_open
            || overlay.quick_switcher_open
            || overlay.command_palette_open
            || overlay.fullscreen_image.is_some()
            || overlay.file_upload_lightbox.is_some();

        div()
            .absolute()
            .inset_0()
            .occlude()
            .flex()
            .flex_col()
            .items_center()
            .id("overlay-root")
            .on_scroll_wheel(cx.listener(AppWindow::consume_scroll_wheel))
            .on_mouse_move(cx.listener(AppWindow::consume_mouse_move))
            .when(has_backdrop, |d| {
                d.bg(tint(0x000000, 0.35))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.dismiss_overlays(cx);
                    }))
            })
            .when(overlay.new_chat_open, |container| {
                container.child(new_chat::render_new_chat_modal(
                    new_chat,
                    new_chat_input,
                    cx,
                ))
            })
            .when(overlay.quick_switcher_open, |container| {
                container.child(Self::render_search_overlay(
                    quick_switcher,
                    quick_switcher_input,
                    quick_switcher_indexing_status,
                    cx,
                ))
            })
            .when(overlay.command_palette_open, |container| {
                container.child(Self::render_command_palette(cx))
            })
            .when_some(overlay.fullscreen_image.as_ref(), |container, image| {
                container.child(Self::render_fullscreen_image_overlay(image, cx))
            })
            .when_some(
                overlay.file_upload_lightbox.as_ref(),
                |container, lightbox| {
                    container.child(Self::render_file_upload_lightbox(
                        lightbox,
                        file_upload_caption_input,
                        cx,
                    ))
                },
            )
            .when(!has_backdrop, |d| {
                d.justify_end().items_end().px_6().pb_4().child(
                    div()
                        .flex()
                        .gap_2()
                        .when_some(overlay.active_modal.as_ref(), |container, modal| {
                            container.child(
                                div()
                                    .rounded_lg()
                                    .bg(panel_surface())
                                    .px_3()
                                    .py_2()
                                    .flex()
                                    .items_center()
                                    .gap_2()
                                    .child(modal.clone())
                                    .child(
                                        div()
                                            .id("overlay-modal-close")
                                            .on_click(cx.listener(|this, _, _, cx| {
                                                this.dismiss_overlays(cx);
                                            }))
                                            .child(badge("Close", panel_alt_bg(), text_primary())),
                                    ),
                            )
                        })
                        .when(overlay.emoji_picker_open, |container| {
                            container.child(emoji_picker::render_emoji_picker(
                                overlay,
                                emoji_picker_state,
                                emoji_picker_input,
                                custom_emoji_index,
                                supports_custom_emoji,
                                cx,
                            ))
                        })
                        .when_some(
                            overlay.profile_card_user_id.as_ref(),
                            |container, user_id| {
                                container.child(crate::views::profile::render_profile_card(
                                    profile_panel,
                                    user_id,
                                    cx,
                                ))
                            },
                        ),
                )
            })
            .into_any_element()
    }

    fn render_fullscreen_image_overlay(
        image: &FullscreenImageOverlay,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        let caption = image
            .caption
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let Some(image_source) = image_source_from_attachment_source(&image.source) else {
            return div()
                .id("overlay-fullscreen-image-unavailable")
                .size_full()
                .flex()
                .items_center()
                .justify_center()
                .on_click(cx.listener(|_, _, _, cx| {
                    cx.stop_propagation();
                }))
                .child(
                    div()
                        .px_4()
                        .py_3()
                        .rounded_lg()
                        .bg(panel_surface())
                        .text_sm()
                        .text_color(rgb(text_primary()))
                        .child("Unable to load image preview."),
                )
                .into_any_element();
        };
        let (frame_width, frame_height) =
            fullscreen_image_frame_size(image.width, image.height, 1120.0, 780.0);

        div()
            .id("overlay-fullscreen-image")
            .size_full()
            .px_6()
            .py_6()
            .flex()
            .items_center()
            .justify_center()
            .on_click(cx.listener(|_, _, _, cx| {
                cx.stop_propagation();
            }))
            .child(
                div()
                    .size_full()
                    .flex()
                    .flex_col()
                    .items_center()
                    .justify_center()
                    .gap_3()
                    .child(
                        div()
                            .flex_1()
                            .w_full()
                            .min_h(px(0.))
                            .flex()
                            .items_center()
                            .justify_center()
                            .child(
                                img(image_source)
                                    .id("lightbox-image")
                                    .w(px(frame_width))
                                    .h(px(frame_height))
                                    .object_fit(ObjectFit::Contain)
                                    .with_fallback(|| {
                                        div()
                                            .size_full()
                                            .flex()
                                            .items_center()
                                            .justify_center()
                                            .text_sm()
                                            .text_color(rgb(text_secondary()))
                                            .child("Image unavailable")
                                            .into_any_element()
                                    }),
                            ),
                    )
                    .when_some(caption, |container, caption| {
                        container.child(
                            div()
                                .max_w(px(900.))
                                .text_sm()
                                .text_color(rgb(text_primary()))
                                .child(caption),
                        )
                    }),
            )
            .into_any_element()
    }

    fn render_file_upload_lightbox(
        lightbox: &FileUploadLightboxModel,
        file_upload_caption_input: &Entity<TextField>,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        let total = lightbox.candidates.len();
        let index = lightbox.current_index.min(total.saturating_sub(1));
        let Some(candidate) = lightbox.candidates.get(index) else {
            return div()
                .mt(px(120.))
                .w(px(420.))
                .rounded_xl()
                .border_1()
                .border_color(shell_border_strong())
                .bg(modal_surface())
                .shadow(card_shadow())
                .px_4()
                .py_3()
                .text_sm()
                .text_color(rgb(text_primary()))
                .child("No files selected.")
                .into_any_element();
        };

        let source_text = candidate.path.to_string_lossy().to_string();
        let image_source = AttachmentSource::LocalPath(source_text.clone());
        let attachment_label = attachment_display_label(&AttachmentSummary {
            name: candidate.filename.clone(),
            kind: candidate.kind.clone(),
            size_bytes: candidate.size_bytes,
            ..AttachmentSummary::default()
        });
        let is_last = index + 1 >= total;
        let counter = format!("{} of {}", index + 1, total);

        let preview = match candidate.kind {
            AttachmentKind::Image => {
                if let Some(preview_source) = image_source_from_attachment_source(&image_source) {
                    let (frame_width, frame_height) = file_upload_preview_frame_size(
                        candidate.width,
                        candidate.height,
                        480.0,
                        280.0,
                    );
                    div()
                        .size_full()
                        .flex()
                        .items_center()
                        .justify_center()
                        .child(
                            img(preview_source)
                                .w(px(frame_width))
                                .h(px(frame_height))
                                .object_fit(ObjectFit::Contain)
                                .with_fallback(|| {
                                    div()
                                        .size_full()
                                        .flex()
                                        .items_center()
                                        .justify_center()
                                        .text_sm()
                                        .text_color(rgb(text_secondary()))
                                        .child("Image unavailable")
                                        .into_any_element()
                                }),
                        )
                        .into_any_element()
                } else {
                    div()
                        .size_full()
                        .flex()
                        .items_center()
                        .justify_center()
                        .text_sm()
                        .text_color(rgb(text_secondary()))
                        .child("Image unavailable")
                        .into_any_element()
                }
            }
            AttachmentKind::Video => div()
                .size_full()
                .flex()
                .items_center()
                .justify_center()
                .text_sm()
                .text_color(rgb(text_secondary()))
                .child("Video preview coming soon")
                .into_any_element(),
            AttachmentKind::Audio => div()
                .size_full()
                .flex()
                .items_center()
                .justify_center()
                .text_sm()
                .text_color(rgb(text_secondary()))
                .child("Audio file selected")
                .into_any_element(),
            AttachmentKind::File => div()
                .size_full()
                .flex()
                .items_center()
                .justify_center()
                .text_sm()
                .text_color(rgb(text_secondary()))
                .child("File selected")
                .into_any_element(),
        };

        div()
            .mt(px(96.))
            .w(px(520.))
            .max_h(px(620.))
            .rounded_xl()
            .border_1()
            .border_color(shell_border_strong())
            .bg(modal_surface())
            .shadow(card_shadow())
            .flex()
            .flex_col()
            .overflow_hidden()
            .id("overlay-file-upload-lightbox")
            .on_click(cx.listener(|_, _, _, cx| {
                cx.stop_propagation();
            }))
            .child(
                div()
                    .px_4()
                    .py_3()
                    .border_b_1()
                    .border_color(shell_border())
                    .text_sm()
                    .text_color(rgb(text_primary()))
                    .child("Upload files"),
            )
            .child(
                div()
                    .px_4()
                    .py_3()
                    .flex_1()
                    .min_h(px(0.))
                    .flex()
                    .flex_col()
                    .gap_3()
                    .child(
                        div()
                            .h(px(280.))
                            .rounded_lg()
                            .bg(panel_surface())
                            .border_1()
                            .border_color(shell_border())
                            .overflow_hidden()
                            .child(preview),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(rgb(text_primary()))
                            .child(candidate.filename.clone()),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(rgb(text_secondary()))
                            .child(format!("{counter} · {attachment_label}")),
                    )
                    .child(
                        div()
                            .id("file-upload-caption-input")
                            .rounded_md()
                            .bg(panel_surface())
                            .border_1()
                            .border_color(shell_border())
                            .px_3()
                            .py_2()
                            .text_sm()
                            .text_color(rgb(text_primary()))
                            .on_click(cx.listener(AppWindow::focus_file_upload_caption_input))
                            .child(file_upload_caption_input.clone()),
                    ),
            )
            .child(
                div()
                    .px_4()
                    .py_3()
                    .border_t_1()
                    .border_color(shell_border())
                    .flex()
                    .items_center()
                    .justify_end()
                    .gap_2()
                    .child(
                        div()
                            .id("file-upload-cancel")
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.file_upload_cancel(cx);
                            }))
                            .child(badge("Cancel", panel_alt_bg(), text_primary())),
                    )
                    .when(!is_last, |container| {
                        container.child(
                            div()
                                .id("file-upload-next")
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.file_upload_next(cx);
                                }))
                                .child(badge("Next", panel_alt_bg(), text_primary())),
                        )
                    })
                    .when(is_last || total == 1, |container| {
                        container.child(
                            div()
                                .id("file-upload-send")
                                .on_click(cx.listener(move |this, _, window, cx| {
                                    if total > 1 {
                                        this.file_upload_send_all(window, cx);
                                    } else {
                                        this.file_upload_send_current(window, cx);
                                    }
                                }))
                                .child(badge("Send", accent(), panel_alt_bg())),
                        )
                    })
                    .when(total > 1, |container| {
                        container.child(
                            div()
                                .id("file-upload-send-all")
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.file_upload_send_all(window, cx);
                                }))
                                .child(badge("Send All", panel_alt_bg(), text_primary())),
                        )
                    }),
            )
            .into_any_element()
    }

    fn render_search_overlay(
        quick_switcher: &QuickSwitcherModel,
        quick_switcher_input: &Entity<TextField>,
        quick_switcher_indexing_status: Option<&str>,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        let query_empty = quick_switcher.query.trim().is_empty();
        let mut unread_indices = Vec::new();
        let mut recent_indices = Vec::new();
        let mut conversation_indices = Vec::new();
        let mut message_indices = Vec::new();
        for (index, result) in quick_switcher.results.iter().enumerate() {
            if query_empty {
                if result.kind == QuickSwitcherResultKind::UnreadChannel
                    && unread_indices.len() < QUICK_SWITCHER_RENDER_LIMIT_UNREAD
                {
                    unread_indices.push(index);
                } else if result.kind != QuickSwitcherResultKind::UnreadChannel
                    && recent_indices.len() < QUICK_SWITCHER_RENDER_LIMIT_RECENT
                {
                    recent_indices.push(index);
                }

                if unread_indices.len() >= QUICK_SWITCHER_RENDER_LIMIT_UNREAD
                    && recent_indices.len() >= QUICK_SWITCHER_RENDER_LIMIT_RECENT
                {
                    break;
                }
            } else {
                if result.kind == QuickSwitcherResultKind::Message {
                    if message_indices.len() < QUICK_SWITCHER_RENDER_LIMIT_MESSAGES {
                        message_indices.push(index);
                    }
                } else if conversation_indices.len() < QUICK_SWITCHER_RENDER_LIMIT_CONVERSATIONS {
                    conversation_indices.push(index);
                }

                if conversation_indices.len() >= QUICK_SWITCHER_RENDER_LIMIT_CONVERSATIONS
                    && message_indices.len() >= QUICK_SWITCHER_RENDER_LIMIT_MESSAGES
                {
                    break;
                }
            }
        }

        let mut results = div().px_4().py_3().flex().flex_col().gap_2();
        if query_empty {
            if !unread_indices.is_empty() {
                results = results.child(Self::quick_switcher_section(
                    "Unread",
                    &unread_indices,
                    quick_switcher,
                    false,
                    cx,
                ));
            }
            if !recent_indices.is_empty() {
                results = results.child(Self::quick_switcher_section(
                    "Recent",
                    &recent_indices,
                    quick_switcher,
                    true,
                    cx,
                ));
            }
        } else {
            if !conversation_indices.is_empty() {
                results = results.child(Self::quick_switcher_section(
                    "Channels and DMs",
                    &conversation_indices,
                    quick_switcher,
                    false,
                    cx,
                ));
            }
            if !message_indices.is_empty() {
                results = results.child(Self::quick_switcher_section(
                    "Messages",
                    &message_indices,
                    quick_switcher,
                    false,
                    cx,
                ));
            }
        }

        if quick_switcher.results.is_empty() {
            results = results.child(
                div()
                    .rounded_md()
                    .bg(subtle_surface())
                    .px_3()
                    .py_2()
                    .text_sm()
                    .text_color(rgb(text_secondary()))
                    .child(if query_empty {
                        "No conversations yet."
                    } else {
                        "No matches found."
                    }),
            );
        }
        if quick_switcher.loading_messages && !query_empty {
            results = results.child(
                div()
                    .text_xs()
                    .text_color(rgb(text_secondary()))
                    .child("Searching messages…"),
            );
        }
        if let Some(indexing_status) = quick_switcher_indexing_status
            && !query_empty
        {
            results = results.child(
                div()
                    .text_xs()
                    .text_color(rgb(text_secondary()))
                    .child(indexing_status.to_string()),
            );
        }

        div()
            .mt(px(120.))
            .w(px(480.))
            .max_h(px(560.))
            .rounded_xl()
            .border_1()
            .border_color(shell_border_strong())
            .bg(modal_surface())
            .shadow(card_shadow())
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(
                div()
                    .px_4()
                    .py_3()
                    .flex()
                    .items_center()
                    .gap_3()
                    .border_b_1()
                    .border_color(shell_border())
                    .child(search_icon(text_secondary()))
                    .child(
                        div()
                            .flex_1()
                            .text_sm()
                            .text_color(rgb(text_primary()))
                            .id("quick-switcher-overlay-input")
                            .on_click(cx.listener(AppWindow::focus_quick_switcher_input))
                            .child(quick_switcher_input.clone()),
                    ),
            )
            .child(
                div()
                    .flex_1()
                    .min_h(px(0.))
                    .id("quick-switcher-results-scroll")
                    .overflow_y_scroll()
                    .on_scroll_wheel(cx.listener(AppWindow::consume_scroll_wheel))
                    .scrollbar_width(px(8.))
                    .pr_1()
                    .child(results),
            )
            .child(
                div()
                    .px_4()
                    .py_2()
                    .border_t_1()
                    .border_color(shell_border())
                    .flex()
                    .items_center()
                    .justify_between()
                    .child(
                        div()
                            .text_xs()
                            .text_color(rgb(text_secondary()))
                            .child("up/down to navigate · enter to go · esc to close"),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(rgb(text_secondary()))
                            .child("⌘K toggles"),
                    ),
            )
            .into_any_element()
    }

    fn quick_switcher_section(
        title: &str,
        indices: &[usize],
        quick_switcher: &QuickSwitcherModel,
        show_recent_shortcuts: bool,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        div()
            .flex()
            .flex_col()
            .gap_1()
            .child(
                div()
                    .text_xs()
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(rgb(text_secondary()))
                    .child(title.to_string()),
            )
            .children(
                indices
                    .iter()
                    .enumerate()
                    .filter_map(|(display_index, index)| {
                        quick_switcher.results.get(*index).map(|result| {
                            let shortcut_hint = (show_recent_shortcuts
                                && display_index < QUICK_SWITCHER_RECENT_SHORTCUT_LIMIT)
                                .then(|| format!("⌘+{}", display_index + 1));
                            Self::quick_switcher_row(
                                *index,
                                result,
                                quick_switcher.selected_index == *index,
                                shortcut_hint,
                                cx,
                            )
                        })
                    }),
            )
            .into_any_element()
    }

    fn quick_switcher_row(
        index: usize,
        result: &QuickSwitcherResult,
        selected: bool,
        shortcut_hint: Option<String>,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        let label_ranges = if matches!(result.kind, QuickSwitcherResultKind::Message) {
            &[]
        } else {
            result.match_ranges.as_slice()
        };
        let sublabel_ranges = if matches!(result.kind, QuickSwitcherResultKind::Message) {
            result.match_ranges.as_slice()
        } else {
            &[]
        };

        div()
            .id(SharedString::from(format!("quick-switcher-result-{index}")))
            .w_full()
            .px_2()
            .py_1p5()
            .rounded_md()
            .flex()
            .items_start()
            .gap_2()
            .text_sm()
            .text_color(rgb(text_primary()))
            .bg(if selected {
                tint(accent_soft(), 0.85)
            } else {
                panel_surface()
            })
            .hover(|s| s.bg(tint(accent_soft(), 0.60)))
            .on_click(cx.listener(move |this, _, window, cx| {
                this.open_quick_switcher_result_at(index, window, cx);
            }))
            .child(Self::quick_switcher_icon(&result.kind))
            .child(
                div()
                    .flex_1()
                    .min_w(px(0.))
                    .overflow_hidden()
                    .flex()
                    .flex_col()
                    .gap_0p5()
                    .child(
                        div()
                            .text_sm()
                            .text_color(rgb(text_primary()))
                            .flex()
                            .flex_wrap()
                            .items_center()
                            .children(Self::highlighted_label(&result.label, label_ranges)),
                    )
                    .when_some(result.sublabel.as_ref(), |container, sublabel| {
                        container.child(
                            div()
                                .w_full()
                                .min_w(px(0.))
                                .text_xs()
                                .text_color(rgb(text_secondary()))
                                .child(Self::highlighted_text_element(sublabel, sublabel_ranges)),
                        )
                    }),
            )
            .when_some(shortcut_hint, |container, hint| {
                container.child(
                    div()
                        .text_xs()
                        .text_color(rgb(text_secondary()))
                        .child(badge(hint, panel_alt_bg(), text_secondary())),
                )
            })
            .into_any_element()
    }

    fn quick_switcher_icon(kind: &QuickSwitcherResultKind) -> AnyElement {
        match kind {
            QuickSwitcherResultKind::UnreadChannel | QuickSwitcherResultKind::Channel => {
                hash_icon(text_secondary())
            }
            QuickSwitcherResultKind::DirectMessage => dm_icon(text_secondary()),
            QuickSwitcherResultKind::Message => search_icon(text_secondary()),
        }
    }

    fn highlighted_label(text: &str, ranges: &[(usize, usize)]) -> Vec<AnyElement> {
        if text.is_empty() {
            return vec![div().child(String::new()).into_any_element()];
        }
        if ranges.is_empty() {
            return vec![div().child(text.to_string()).into_any_element()];
        }
        if ranges.len() == 1 {
            let (start, end) = ranges[0];
            if start < end
                && end <= text.len()
                && text.is_char_boundary(start)
                && text.is_char_boundary(end)
            {
                let mut elements = Vec::new();
                if start > 0 {
                    elements.push(div().child(text[..start].to_string()).into_any_element());
                }
                elements.push(
                    div()
                        .font_weight(FontWeight::SEMIBOLD)
                        .text_color(rgb(accent()))
                        .child(text[start..end].to_string())
                        .into_any_element(),
                );
                if end < text.len() {
                    elements.push(div().child(text[end..].to_string()).into_any_element());
                }
                return elements;
            }
        }

        let mut normalized: Vec<(usize, usize)> = ranges
            .iter()
            .copied()
            .filter(|(start, end)| {
                start < end
                    && *end <= text.len()
                    && text.is_char_boundary(*start)
                    && text.is_char_boundary(*end)
            })
            .collect::<Vec<_>>();
        normalized.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));

        let mut merged: Vec<(usize, usize)> = Vec::new();
        for (start, end) in normalized {
            if let Some(last) = merged.last_mut()
                && start <= last.1
            {
                last.1 = last.1.max(end);
                continue;
            }
            merged.push((start, end));
        }

        if merged.is_empty() {
            return vec![div().child(text.to_string()).into_any_element()];
        }

        let mut elements = Vec::new();
        let mut cursor = 0usize;
        for (start, end) in merged {
            if start > cursor {
                elements.push(
                    div()
                        .child(text[cursor..start].to_string())
                        .into_any_element(),
                );
            }
            elements.push(
                div()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(rgb(accent()))
                    .child(text[start..end].to_string())
                    .into_any_element(),
            );
            cursor = end;
        }
        if cursor < text.len() {
            elements.push(div().child(text[cursor..].to_string()).into_any_element());
        }
        elements
    }

    fn highlighted_text_element(text: &str, ranges: &[(usize, usize)]) -> HighlightedTextElement {
        let mut normalized: Vec<(usize, usize)> = ranges
            .iter()
            .copied()
            .filter(|(s, e)| {
                s < e && *e <= text.len() && text.is_char_boundary(*s) && text.is_char_boundary(*e)
            })
            .collect();
        normalized.sort_by_key(|&(s, e)| (s, e));
        let mut merged: Vec<(usize, usize)> = Vec::new();
        for (s, e) in normalized {
            if let Some(last) = merged.last_mut()
                && s <= last.1
            {
                last.1 = last.1.max(e);
                continue;
            }
            merged.push((s, e));
        }
        HighlightedTextElement {
            content: text.to_string(),
            highlight_ranges: merged,
        }
    }

    fn render_command_palette(cx: &mut Context<AppWindow>) -> AnyElement {
        div()
            .mt(px(120.))
            .w(px(400.))
            .rounded_xl()
            .border_1()
            .border_color(shell_border_strong())
            .bg(modal_surface())
            .shadow(card_shadow())
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(
                div()
                    .px_4()
                    .py_3()
                    .border_b_1()
                    .border_color(shell_border())
                    .text_xs()
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(rgb(text_secondary()))
                    .child("Commands"),
            )
            .child(
                div()
                    .px_4()
                    .py_3()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .child(Self::command_palette_row(
                        "overlay-command-new-chat",
                        "Start new chat",
                        "⌘N",
                        "new-chat",
                        cx,
                    ))
                    .child(Self::command_palette_row(
                        "overlay-command-search",
                        "Search messages",
                        "⌘J",
                        "search",
                        cx,
                    ))
                    .child(Self::command_palette_row(
                        "overlay-command-thread",
                        "Toggle thread pane",
                        "⌘⇧\\",
                        "thread",
                        cx,
                    ))
                    .child(Self::command_palette_row(
                        "overlay-command-prefs",
                        "Open preferences",
                        "⌘,",
                        "preferences",
                        cx,
                    ))
                    .child(Self::command_palette_row(
                        "overlay-command-activity",
                        "Show activity",
                        "⌘⇧A",
                        "activity",
                        cx,
                    ))
                    .child(Self::command_palette_row(
                        "overlay-command-mark-all-read",
                        "Mark all as read",
                        "",
                        "mark-all-read",
                        cx,
                    )),
            )
            .into_any_element()
    }

    fn command_palette_row(
        id: &'static str,
        label: &'static str,
        shortcut: &'static str,
        action: &'static str,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        div()
            .id(SharedString::from(id))
            .w_full()
            .px_2()
            .py_1p5()
            .rounded_md()
            .flex()
            .items_center()
            .justify_between()
            .text_sm()
            .text_color(rgb(text_primary()))
            .hover(|s| s.bg(subtle_surface()))
            .on_click(cx.listener(move |this, _, window, cx| {
                this.run_palette_action(action, window, cx);
            }))
            .child(label)
            .child(
                div()
                    .text_xs()
                    .text_color(rgb(text_secondary()))
                    .child(shortcut),
            )
            .into_any_element()
    }
}

fn fullscreen_image_frame_size(
    width: Option<u32>,
    height: Option<u32>,
    max_width: f32,
    max_height: f32,
) -> (f32, f32) {
    let mut width = width.unwrap_or(1600) as f32;
    let mut height = height.unwrap_or(1000) as f32;
    if width <= 1.0 || height <= 1.0 {
        return (max_width.min(900.0), max_height.min(600.0));
    }
    let scale = (max_width / width).min(max_height / height).min(1.0);
    width *= scale;
    height *= scale;
    (width.max(120.0), height.max(120.0))
}

fn file_upload_preview_frame_size(
    width: Option<u32>,
    height: Option<u32>,
    max_width: f32,
    max_height: f32,
) -> (f32, f32) {
    let mut width = width.unwrap_or(900) as f32;
    let mut height = height.unwrap_or(600) as f32;
    if width <= 1.0 || height <= 1.0 {
        return (max_width.min(420.0), max_height.min(240.0));
    }
    let scale = (max_width / width).min(max_height / height).min(1.0);
    width *= scale;
    height *= scale;
    (width.max(60.0), height.max(60.0))
}

struct HighlightedTextElement {
    content: String,
    highlight_ranges: Vec<(usize, usize)>,
}

impl HighlightedTextElement {
    fn build_runs(&self, base_color: gpui::Hsla, font: gpui::Font) -> Vec<TextRun> {
        let text_len = self.content.len();
        if text_len == 0 || self.highlight_ranges.is_empty() {
            return vec![TextRun {
                len: text_len.max(1),
                font,
                color: base_color,
                background_color: None,
                underline: None,
                strikethrough: None,
            }];
        }

        let highlight_color = gpui::Hsla::from(rgb(accent()));
        let mut runs = Vec::new();
        let mut cursor = 0usize;
        for &(start, end) in &self.highlight_ranges {
            if start > cursor {
                runs.push(TextRun {
                    len: start - cursor,
                    font: font.clone(),
                    color: base_color,
                    background_color: None,
                    underline: None,
                    strikethrough: None,
                });
            }
            runs.push(TextRun {
                len: end - start,
                font: gpui::Font {
                    weight: FontWeight::SEMIBOLD,
                    ..font.clone()
                },
                color: highlight_color,
                background_color: None,
                underline: None,
                strikethrough: None,
            });
            cursor = end;
        }
        if cursor < text_len {
            runs.push(TextRun {
                len: text_len - cursor,
                font,
                color: base_color,
                background_color: None,
                underline: None,
                strikethrough: None,
            });
        }
        runs
    }
}

struct HighlightedTextPrepaint {
    lines: Vec<gpui::WrappedLine>,
    line_height: Pixels,
}

impl IntoElement for HighlightedTextElement {
    type Element = Self;
    fn into_element(self) -> Self::Element {
        self
    }
}

impl Element for HighlightedTextElement {
    type RequestLayoutState = ();
    type PrepaintState = HighlightedTextPrepaint;

    fn id(&self) -> Option<ElementId> {
        None
    }

    fn source_location(&self) -> Option<&'static core::panic::Location<'static>> {
        None
    }

    fn request_layout(
        &mut self,
        _id: Option<&GlobalElementId>,
        _inspector_id: Option<&gpui::InspectorElementId>,
        window: &mut Window,
        _cx: &mut App,
    ) -> (LayoutId, Self::RequestLayoutState) {
        let text_style = window.text_style();
        let font_size = text_style.font_size.to_pixels(window.rem_size());
        let line_height = window.line_height();
        let content = self.content.clone();
        let runs = self.build_runs(text_style.color, text_style.font());

        let mut style = Style::default();
        style.size.width = relative(1.).into();

        (
            window.request_measured_layout(style, move |known, available, window, _cx| {
                let wrap_width = known.width.or(match available.width {
                    AvailableSpace::Definite(width) => Some(width),
                    _ => None,
                });
                let Some(lines) = window
                    .text_system()
                    .shape_text(content.clone().into(), font_size, &runs, wrap_width, None)
                    .ok()
                else {
                    return size(px(0.), px(0.));
                };
                let mut measured = size(px(0.), px(0.));
                for line in &lines {
                    let line_size = line.size(line_height);
                    measured.height += line_size.height;
                    measured.width = measured.width.max(line_size.width).ceil();
                }
                if let Some(w) = wrap_width {
                    measured.width = measured.width.min(w);
                }
                measured
            }),
            (),
        )
    }

    fn prepaint(
        &mut self,
        _id: Option<&GlobalElementId>,
        _inspector_id: Option<&gpui::InspectorElementId>,
        bounds: Bounds<Pixels>,
        _request_layout: &mut Self::RequestLayoutState,
        window: &mut Window,
        _cx: &mut App,
    ) -> Self::PrepaintState {
        let style = window.text_style();
        let font_size = style.font_size.to_pixels(window.rem_size());
        let runs = self.build_runs(style.color, style.font());
        let lines = window
            .text_system()
            .shape_text(
                self.content.clone().into(),
                font_size,
                &runs,
                Some(bounds.size.width.max(px(1.0))),
                None,
            )
            .map(|lines| lines.into_vec())
            .unwrap_or_default();
        HighlightedTextPrepaint {
            lines,
            line_height: window.line_height(),
        }
    }

    fn paint(
        &mut self,
        _id: Option<&GlobalElementId>,
        _inspector_id: Option<&gpui::InspectorElementId>,
        bounds: Bounds<Pixels>,
        _request_layout: &mut Self::RequestLayoutState,
        prepaint: &mut Self::PrepaintState,
        window: &mut Window,
        _cx: &mut App,
    ) {
        let align = window.text_style().text_align;
        let mut line_origin = bounds.origin;
        for line in &prepaint.lines {
            let _ = line.paint(
                line_origin,
                prepaint.line_height,
                align,
                Some(bounds),
                window,
                _cx,
            );
            line_origin.y += line.size(prepaint.line_height).height;
        }
    }
}
