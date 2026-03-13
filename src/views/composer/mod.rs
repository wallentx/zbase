use crate::{
    models::composer_model::{
        AutocompleteCandidate, AutocompleteState, ComposerMode, ComposerModel,
    },
    views::{
        accent, accent_soft, app_window::AppWindow, card_shadow, emoji_icon, input::TextField,
        is_dark_theme, modal_surface, paperclip_icon, shell_border, shell_border_strong,
        text_primary, text_secondary, tint,
    },
};
use gpui::prelude::FluentBuilder;
use gpui::{
    AnyElement, Context, CursorStyle, Entity, FontWeight, InteractiveElement, IntoElement,
    ParentElement, StatefulInteractiveElement, Styled, div, px, rgb,
};

#[derive(Default)]
pub struct ComposerPanel;

impl ComposerPanel {
    pub fn render(
        &self,
        composer: &ComposerModel,
        editor: &Entity<TextField>,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        let mode_banner = match &composer.mode {
            ComposerMode::Compose => None,
            ComposerMode::Edit { .. } => Some(
                div()
                    .pl_2()
                    .border_l_2()
                    .border_color(rgb(accent()))
                    .text_xs()
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(rgb(text_secondary()))
                    .child("Editing · press Escape to cancel")
                    .into_any_element(),
            ),
            ComposerMode::ReplyInThread { .. } => Some(
                div()
                    .pl_2()
                    .border_l_2()
                    .border_color(rgb(accent()))
                    .text_xs()
                    .font_weight(FontWeight::MEDIUM)
                    .text_color(rgb(text_secondary()))
                    .child("Replying in thread")
                    .into_any_element(),
            ),
        };

        div()
            .px_4()
            .py_2()
            .border_t_1()
            .border_color(shell_border())
            .flex_shrink_0()
            .flex()
            .flex_col()
            .gap_2()
            .when_some(mode_banner, |div, banner| div.child(banner))
            .child(
                div()
                    .rounded_md()
                    .bg(if is_dark_theme() {
                        tint(0x141d25, 0.34)
                    } else {
                        tint(0xffffff, 0.86)
                    })
                    .w_full()
                    .text_sm()
                    .line_height(px(22.))
                    .text_color(rgb(text_primary()))
                    .relative()
                    .id("composer-input-surface")
                    .on_click(cx.listener(AppWindow::focus_composer_input))
                    .child(div().px_3().py_2().pr(px(64.)).child(editor.clone()))
                    .child(
                        div()
                            .absolute()
                            .top_0()
                            .bottom_0()
                            .right_1()
                            .flex()
                            .items_center()
                            .gap_1()
                            .child(
                                div()
                                    .id("composer-attach-inline")
                                    .w(px(24.))
                                    .h(px(24.))
                                    .rounded_md()
                                    .flex()
                                    .items_center()
                                    .justify_center()
                                    .cursor(CursorStyle::PointingHand)
                                    .hover(|s| {
                                        s.bg(tint(
                                            accent_soft(),
                                            if is_dark_theme() { 0.34 } else { 0.70 },
                                        ))
                                    })
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.open_composer_file_upload_picker(cx);
                                    }))
                                    .child(paperclip_icon(text_secondary())),
                            )
                            .child(
                                div()
                                    .id("composer-emoji-inline")
                                    .w(px(24.))
                                    .h(px(24.))
                                    .rounded_md()
                                    .flex()
                                    .items_center()
                                    .justify_center()
                                    .cursor(CursorStyle::PointingHand)
                                    .hover(|s| {
                                        s.bg(tint(
                                            accent_soft(),
                                            if is_dark_theme() { 0.34 } else { 0.70 },
                                        ))
                                    })
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

pub(crate) fn render_autocomplete_popup(
    autocomplete: &AutocompleteState,
    composer_target: bool,
    cx: &mut Context<AppWindow>,
) -> AnyElement {
    let rows = autocomplete
        .candidates
        .iter()
        .enumerate()
        .map(|(index, candidate)| {
            let selected = index == autocomplete.selected_index;
            let base = div()
                .id((
                    if composer_target {
                        "inline-autocomplete-composer"
                    } else {
                        "inline-autocomplete-thread"
                    },
                    index,
                ))
                .w_full()
                .px_2()
                .py_1()
                .rounded_sm()
                .cursor(CursorStyle::PointingHand)
                .bg(if selected {
                    tint(accent_soft(), if is_dark_theme() { 0.45 } else { 0.85 })
                } else {
                    tint(0x000000, 0.0)
                })
                .hover(|style| {
                    style.bg(tint(
                        accent_soft(),
                        if is_dark_theme() { 0.32 } else { 0.65 },
                    ))
                });
            let clickable = if composer_target {
                base.on_click(cx.listener(move |this, _, _, cx| {
                    this.select_composer_autocomplete_index(index, cx);
                }))
            } else {
                base.on_click(cx.listener(move |this, _, _, cx| {
                    this.select_thread_autocomplete_index(index, cx);
                }))
            };
            clickable.child(render_candidate_row(candidate))
        })
        .collect::<Vec<_>>();

    div()
        .w_full()
        .rounded_xl()
        .border_1()
        .border_color(shell_border_strong())
        .bg(modal_surface())
        .shadow(card_shadow())
        .px_1()
        .py_1()
        .flex()
        .flex_col()
        .gap_1()
        .max_h(px(220.))
        .overflow_hidden()
        .children(rows)
        .into_any_element()
}

fn render_candidate_row(candidate: &AutocompleteCandidate) -> AnyElement {
    match candidate {
        AutocompleteCandidate::MentionUser {
            username,
            display_name,
            ..
        } => div()
            .flex()
            .items_center()
            .justify_between()
            .gap_2()
            .child(
                div()
                    .text_sm()
                    .text_color(rgb(text_primary()))
                    .child(display_name.clone()),
            )
            .child(
                div()
                    .text_xs()
                    .text_color(rgb(text_secondary()))
                    .child(format!("@{username}")),
            )
            .into_any_element(),
        AutocompleteCandidate::MentionBroadcast {
            keyword,
            description,
        } => div()
            .flex()
            .items_center()
            .justify_between()
            .gap_2()
            .child(
                div()
                    .text_sm()
                    .text_color(rgb(text_primary()))
                    .child(format!("@{keyword}")),
            )
            .child(
                div()
                    .text_xs()
                    .text_color(rgb(text_secondary()))
                    .child(description.clone()),
            )
            .into_any_element(),
        AutocompleteCandidate::Emoji {
            label,
            glyph,
            insert_text: _,
        } => div()
            .flex()
            .items_center()
            .gap_2()
            .child(
                div()
                    .text_base()
                    .text_color(rgb(text_primary()))
                    .child(glyph.clone().unwrap_or_else(|| "◻".to_string())),
            )
            .child(
                div()
                    .text_sm()
                    .text_color(rgb(text_primary()))
                    .child(label.clone()),
            )
            .into_any_element(),
    }
}
