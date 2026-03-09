use crate::{
    models::{
        notifications_model::NotificationsModel,
        overlay_model::{FullscreenImageOverlay, OverlayModel},
        quick_switcher_model::{QuickSwitcherModel, QuickSwitcherResult, QuickSwitcherResultKind},
    },
    views::{
        accent, accent_soft, app_window::AppWindow, badge, card_shadow, dm_icon, hash_icon,
        image_source_from_attachment_source, input::TextField, modal_surface, panel_alt_bg,
        panel_surface, search_icon, shell_border, shell_border_strong, subtle_surface,
        text_primary, text_secondary, tint,
    },
};
use gpui::prelude::FluentBuilder;
use gpui::{
    AnyElement, Context, Entity, FontWeight, InteractiveElement, IntoElement, ObjectFit,
    ParentElement, SharedString, StatefulInteractiveElement, Styled, StyledImage, div, img, px,
    rgb,
};

const QUICK_SWITCHER_RENDER_LIMIT_UNREAD: usize = 14;
const QUICK_SWITCHER_RENDER_LIMIT_RECENT: usize = 14;
const QUICK_SWITCHER_RENDER_LIMIT_CONVERSATIONS: usize = 3;
const QUICK_SWITCHER_RENDER_LIMIT_MESSAGES: usize = 10;

#[derive(Default)]
pub struct OverlayHost;

impl OverlayHost {
    pub fn render(
        &self,
        overlay: &OverlayModel,
        _notifications: &NotificationsModel,
        quick_switcher: &QuickSwitcherModel,
        quick_switcher_input: &Entity<TextField>,
        quick_switcher_indexing_status: Option<&str>,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        if overlay.active_modal.is_none()
            && overlay.active_context_menu.is_none()
            && !overlay.quick_switcher_open
            && !overlay.command_palette_open
            && !overlay.emoji_picker_open
            && overlay.fullscreen_image.is_none()
        {
            return div().into_any_element();
        }

        let has_backdrop = overlay.quick_switcher_open
            || overlay.command_palette_open
            || overlay.fullscreen_image.is_some();

        div()
            .absolute()
            .inset_0()
            .occlude()
            .flex()
            .flex_col()
            .items_center()
            .id("overlay-root")
            .on_scroll_wheel(cx.listener(AppWindow::consume_scroll_wheel))
            .when(has_backdrop, |d| {
                d.bg(tint(0x000000, 0.35))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.dismiss_overlays(cx);
                    }))
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
                        .when_some(overlay.active_context_menu.as_ref(), |container, menu| {
                            container.child(
                                div()
                                    .rounded_lg()
                                    .bg(panel_surface())
                                    .px_3()
                                    .py_2()
                                    .flex()
                                    .items_center()
                                    .gap_2()
                                    .child(menu.clone())
                                    .child(
                                        div()
                                            .id("overlay-context-close")
                                            .on_click(cx.listener(|this, _, _, cx| {
                                                this.dismiss_overlays(cx);
                                            }))
                                            .child(badge("Done", panel_alt_bg(), text_primary())),
                                    ),
                            )
                        })
                        .when(overlay.emoji_picker_open, |container| {
                            container.child(
                                div()
                                    .rounded_lg()
                                    .bg(panel_surface())
                                    .px_3()
                                    .py_2()
                                    .flex()
                                    .gap_2()
                                    .child(
                                        div()
                                            .id("overlay-emoji-smile")
                                            .on_click(cx.listener(|this, _, _, cx| {
                                                this.composer_insert_emoji("🙂 ", cx);
                                            }))
                                            .child(badge("🙂", panel_alt_bg(), text_primary())),
                                    )
                                    .child(
                                        div()
                                            .id("overlay-emoji-rocket")
                                            .on_click(cx.listener(|this, _, _, cx| {
                                                this.composer_insert_emoji("🚀 ", cx);
                                            }))
                                            .child(badge("🚀", panel_alt_bg(), text_primary())),
                                    )
                                    .child(
                                        div()
                                            .id("overlay-emoji-check")
                                            .on_click(cx.listener(|this, _, _, cx| {
                                                this.thread_insert_emoji("✅ ", cx);
                                            }))
                                            .child(badge("✅", panel_alt_bg(), text_primary())),
                                    )
                                    .child(
                                        div()
                                            .id("overlay-emoji-close")
                                            .on_click(cx.listener(|this, _, _, cx| {
                                                this.dismiss_overlays(cx);
                                            }))
                                            .child(badge(
                                                "Close",
                                                panel_alt_bg(),
                                                text_secondary(),
                                            )),
                                    ),
                            )
                        }),
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
                        div().flex_1().w_full().min_h(px(0.)).child(
                            img(image_source)
                                .w_full()
                                .h_full()
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
                    cx,
                ));
            }
            if !recent_indices.is_empty() {
                results = results.child(Self::quick_switcher_section(
                    "Recent",
                    &recent_indices,
                    quick_switcher,
                    cx,
                ));
            }
        } else {
            if !conversation_indices.is_empty() {
                results = results.child(Self::quick_switcher_section(
                    "Channels and DMs",
                    &conversation_indices,
                    quick_switcher,
                    cx,
                ));
            }
            if !message_indices.is_empty() {
                results = results.child(Self::quick_switcher_section(
                    "Messages",
                    &message_indices,
                    quick_switcher,
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
                        "No unread channels yet."
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
            .children(indices.iter().filter_map(|index| {
                quick_switcher.results.get(*index).map(|result| {
                    Self::quick_switcher_row(
                        *index,
                        result,
                        quick_switcher.selected_index == *index,
                        cx,
                    )
                })
            }))
            .into_any_element()
    }

    fn quick_switcher_row(
        index: usize,
        result: &QuickSwitcherResult,
        selected: bool,
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
                                .text_xs()
                                .text_color(rgb(text_secondary()))
                                .flex()
                                .flex_wrap()
                                .items_center()
                                .children(Self::highlighted_label(sublabel, sublabel_ranges)),
                        )
                    }),
            )
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
