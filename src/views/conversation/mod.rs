use crate::{
    domain::pins::{PinnedItem, PinnedTarget},
    models::{
        composer_model::ComposerModel, conversation_model::ConversationModel,
        find_in_chat_model::FindInChatModel, timeline_model::TimelineModel,
    },
    views::{
        accent, accent_soft,
        app_window::AppWindow,
        avatar::{Avatar, default_avatar_background},
        chevron_down_icon, close_icon,
        composer::ComposerPanel,
        glass_surface_dark, hash_icon,
        input::TextField,
        is_dark_theme, pin_icon, search_icon, shell_border, subtle_surface, text_primary,
        text_secondary,
        timeline::TimelineList,
        tint,
    },
};
use gpui::prelude::FluentBuilder;
use gpui::{
    AnyElement, Context, Entity, ExternalPaths, FontWeight, InteractiveElement, IntoElement,
    ListState, ParentElement, StatefulInteractiveElement, Styled, div, px, rgb,
};

#[derive(Default)]
pub struct ConversationView;

#[derive(Default)]
pub struct ConversationHeader;

impl ConversationView {
    pub fn render(
        &self,
        conversation: &ConversationModel,
        timeline: &TimelineModel,
        find_in_chat: &FindInChatModel,
        find_in_chat_input: &Entity<TextField>,
        composer: &ComposerModel,
        composer_input: &Entity<TextField>,
        timeline_list_state: &ListState,
        unseen_message_count: usize,
        show_jump_to_bottom: bool,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        let show_jump = show_jump_to_bottom || unseen_message_count > 0;
        let jump_label = if unseen_message_count > 0 {
            format!(
                "{} new message{}",
                unseen_message_count,
                if unseen_message_count == 1 { "" } else { "s" }
            )
        } else {
            "Jump to latest".to_string()
        };
        let jump_pill = div()
            .id("conversation-jump-latest")
            .on_click(cx.listener(AppWindow::scroll_timeline_to_bottom))
            .rounded_full()
            .bg(glass_surface_dark().opacity(0.92))
            .border_1()
            .border_color(shell_border())
            .px_3()
            .py_1p5()
            .flex()
            .items_center()
            .gap_2()
            .text_xs()
            .text_color(rgb(text_primary()))
            .hover(|s| s.bg(glass_surface_dark().opacity(0.98)))
            .child(chevron_down_icon(text_primary()))
            .child(jump_label);
        div()
            .flex_1()
            .h_full()
            .min_h(px(0.))
            .flex()
            .flex_col()
            .overflow_hidden()
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
                    this.open_composer_file_upload_with_paths(file_paths, cx);
                }
            }))
            .child(ConversationHeader.render(conversation, cx))
            .when_some(conversation.pinned_message.as_ref(), |container, pinned| {
                container.child(Self::render_pinned_banner(pinned, cx))
            })
            .when(find_in_chat.open, |container| {
                container.child(Self::render_find_bar(find_in_chat, find_in_chat_input, cx))
            })
            .child(
                div()
                    .flex_1()
                    .min_h(px(0.))
                    .relative()
                    .flex()
                    .flex_col()
                    .overflow_hidden()
                    .child(TimelineList.render(timeline, timeline_list_state, cx))
                    .when_some(timeline.typing_text.clone(), |container, typing_label| {
                        container.child(
                            div()
                                .absolute()
                                .left_0()
                                .right_0()
                                .bottom(px(0.))
                                .pl(px(48.))
                                .pr_4()
                                .py_0p5()
                                .text_xs()
                                .text_color(rgb(text_secondary()))
                                .child(typing_label),
                        )
                    })
                    .when(show_jump, |container| {
                        container.child(
                            div()
                                .absolute()
                                .left_0()
                                .right_0()
                                .bottom(px(14.))
                                .flex()
                                .justify_center()
                                .child(jump_pill),
                        )
                    }),
            )
            .child(ComposerPanel.render(composer, composer_input, cx))
            .into_any_element()
    }

    fn render_find_bar(
        find_in_chat: &FindInChatModel,
        find_in_chat_input: &Entity<TextField>,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        let result_count = find_in_chat.results.len();
        let status = if find_in_chat.query.trim().is_empty() {
            "Type to search this conversation".to_string()
        } else if find_in_chat.loading {
            "Searching…".to_string()
        } else if result_count == 0 {
            "0 results".to_string()
        } else if let Some(selected) = find_in_chat.selected_index {
            format!("{} of {}", selected.saturating_add(1), result_count)
        } else {
            format!(
                "{} result{}",
                result_count,
                if result_count == 1 { "" } else { "s" }
            )
        };

        div()
            .id("conversation-find-bar")
            .px_3()
            .py_1p5()
            .flex()
            .items_center()
            .gap_2()
            .bg(rgb(accent_soft()))
            .child(search_icon(accent()))
            .child(
                div()
                    .id("conversation-find-input")
                    .flex_1()
                    .min_w(px(0.))
                    .on_click(cx.listener(AppWindow::focus_find_in_chat_input))
                    .child(find_in_chat_input.clone()),
            )
            .child(
                div()
                    .text_xs()
                    .text_color(rgb(text_secondary()))
                    .child(status),
            )
            .child(
                div()
                    .id("conversation-find-close")
                    .w(px(22.))
                    .h(px(22.))
                    .rounded_full()
                    .flex()
                    .items_center()
                    .justify_center()
                    .hover(|s| s.bg(subtle_surface()))
                    .on_click(cx.listener(AppWindow::close_find_in_chat_click))
                    .child(close_icon(text_secondary())),
            )
            .into_any_element()
    }

    fn render_pinned_banner(pinned: &PinnedItem, cx: &mut Context<AppWindow>) -> AnyElement {
        let preview_text = pinned
            .preview
            .as_ref()
            .and_then(|preview| preview.text.as_deref())
            .map(compact_pinned_preview_text)
            .filter(|text| !text.trim().is_empty())
            .unwrap_or_else(|| "Pinned message".to_string());
        let is_message_pin = matches!(&pinned.target, PinnedTarget::Message { .. });
        div()
            .id("conversation-pinned-banner")
            .px_3()
            .py_1p5()
            .flex()
            .items_center()
            .justify_between()
            .gap_2()
            .bg(rgb(accent_soft()))
            .child(
                div()
                    .id("conversation-pinned-banner-open")
                    .flex()
                    .flex_1()
                    .items_center()
                    .gap_2()
                    .min_w(px(0.))
                    .overflow_hidden()
                    .on_click(cx.listener(AppWindow::jump_to_pinned_message))
                    .child(pin_icon(accent()))
                    .child(
                        div()
                            .min_w(px(0.))
                            .overflow_hidden()
                            .text_xs()
                            .text_color(rgb(text_secondary()))
                            .child(preview_text),
                    )
                    .when(!is_message_pin, |d| {
                        d.text_color(rgb(text_secondary()))
                            .child(div().text_xs().child("Unavailable"))
                    }),
            )
            .child(
                div()
                    .id("conversation-pinned-banner-dismiss")
                    .w(px(22.))
                    .h(px(22.))
                    .rounded_full()
                    .flex()
                    .items_center()
                    .justify_center()
                    .hover(|s| s.bg(subtle_surface()))
                    .on_click(cx.listener(AppWindow::dismiss_pinned_banner))
                    .child(close_icon(text_secondary())),
            )
            .into_any_element()
    }
}

fn compact_pinned_preview_text(raw: &str) -> String {
    let mut lines = raw.lines().map(str::trim).filter(|line| !line.is_empty());
    let mut preview_lines = Vec::new();
    for line in lines.by_ref().take(2) {
        preview_lines.push(line.to_string());
    }
    if preview_lines.is_empty() {
        return String::new();
    }
    let mut preview = preview_lines.join("\n");
    const MAX_CHARS: usize = 180;
    if preview.chars().count() > MAX_CHARS {
        preview = preview.chars().take(MAX_CHARS).collect::<String>();
        preview.push('…');
        return preview;
    }
    if lines.next().is_some() {
        preview.push('…');
    }
    preview
}

impl ConversationHeader {
    pub fn render(
        &self,
        conversation: &ConversationModel,
        _cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        div()
            .px_4()
            .py_3()
            .flex()
            .items_center()
            .justify_between()
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .child(Self::conversation_badge(conversation))
                    .when_some(conversation.summary.group.as_ref(), |d, group| {
                        d.child(
                            div()
                                .text_sm()
                                .text_color(rgb(text_secondary()))
                                .child(group.display_name.clone()),
                        )
                        .child(div().text_sm().text_color(rgb(text_secondary())).child("›"))
                    })
                    .child(
                        div()
                            .text_xl()
                            .font_weight(FontWeight::SEMIBOLD)
                            .child(conversation.summary.title.clone()),
                    ),
            )
            .into_any_element()
    }

    fn conversation_badge(conversation: &ConversationModel) -> AnyElement {
        match conversation.summary.kind {
            crate::domain::conversation::ConversationKind::Channel => {
                if let Some(asset) = conversation.avatar_asset.as_deref() {
                    Avatar::render_square(
                        &conversation.summary.title,
                        Some(asset),
                        28.,
                        accent_soft(),
                        accent(),
                    )
                } else {
                    div()
                        .w(px(28.))
                        .h(px(28.))
                        .rounded_md()
                        .bg(rgb(accent_soft()))
                        .text_color(rgb(accent()))
                        .flex()
                        .items_center()
                        .justify_center()
                        .child(hash_icon(accent()))
                        .into_any_element()
                }
            }
            crate::domain::conversation::ConversationKind::DirectMessage => div()
                .child(Avatar::render(
                    &conversation.summary.title,
                    conversation.avatar_asset.as_deref(),
                    28.,
                    default_avatar_background(&conversation.summary.title),
                    text_primary(),
                ))
                .into_any_element(),
            crate::domain::conversation::ConversationKind::GroupDirectMessage => div()
                .w(px(28.))
                .h(px(28.))
                .rounded_md()
                .bg(rgb(accent_soft()))
                .text_color(rgb(accent()))
                .flex()
                .items_center()
                .justify_center()
                .child(hash_icon(accent()))
                .into_any_element(),
        }
    }
}
