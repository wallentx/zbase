use crate::{
    models::new_chat_model::NewChatModel,
    views::{
        accent, badge, card_shadow, input::TextField, modal_surface, panel_alt_bg, panel_surface,
        shell_border, shell_border_strong, subtle_surface, text_primary, text_secondary,
    },
};
use gpui::prelude::FluentBuilder;
use gpui::{
    AnyElement, Context, Entity, InteractiveElement, IntoElement, ParentElement,
    StatefulInteractiveElement, Styled, div, px, rgb,
};

use crate::views::app_window::AppWindow;

pub fn render_new_chat_modal(
    new_chat: &NewChatModel,
    new_chat_input: &Entity<TextField>,
    cx: &mut Context<AppWindow>,
) -> AnyElement {
    let mut selected_users = div().flex().flex_wrap().gap_1();
    for (index, user) in new_chat.selected_participants.iter().enumerate() {
        let user_id = user.id.clone();
        let label = if user.display_name.trim().is_empty() {
            user.id.0.clone()
        } else {
            user.display_name.clone()
        };
        selected_users = selected_users.child(
            div()
                .rounded_md()
                .bg(subtle_surface())
                .px_2()
                .py_1()
                .flex()
                .items_center()
                .gap_1()
                .child(div().text_xs().text_color(rgb(text_primary())).child(label))
                .child(
                    div()
                        .id(("new-chat-remove", index))
                        .cursor_pointer()
                        .on_click(cx.listener(move |this, _, _, cx| {
                            this.new_chat_remove_participant(user_id.clone(), cx);
                        }))
                        .child(div().text_xs().text_color(rgb(text_secondary())).child("x")),
                ),
        );
    }

    let mut results_list = div().flex().flex_col().gap_1();
    if new_chat.search_query.trim().is_empty() {
        results_list = results_list.child(
            div()
                .rounded_md()
                .bg(subtle_surface())
                .px_3()
                .py_2()
                .text_sm()
                .text_color(rgb(text_secondary()))
                .child("Search for people to start a chat."),
        );
    } else if new_chat.search_results.is_empty() {
        results_list = results_list.child(
            div()
                .rounded_md()
                .bg(subtle_surface())
                .px_3()
                .py_2()
                .text_sm()
                .text_color(rgb(text_secondary()))
                .child("No people found."),
        );
    } else {
        for (index, user) in new_chat.search_results.iter().enumerate() {
            let user_for_add = user.clone();
            let selected = new_chat
                .selected_participants
                .iter()
                .any(|selected_user| selected_user.id == user.id);
            let display_name = if user.display_name.trim().is_empty() {
                user.id.0.clone()
            } else {
                user.display_name.clone()
            };
            let subtitle = if user.title.trim().is_empty() {
                user.id.0.clone()
            } else {
                user.title.clone()
            };
            results_list = results_list.child(
                div()
                    .id(("new-chat-result", index))
                    .rounded_md()
                    .px_2()
                    .py_1p5()
                    .flex()
                    .items_center()
                    .justify_between()
                    .bg(if selected {
                        subtle_surface()
                    } else {
                        panel_surface()
                    })
                    .hover(|s| s.bg(subtle_surface()))
                    .when(!selected, |row| {
                        row.on_click(cx.listener(move |this, _, _, cx| {
                            this.new_chat_add_participant(user_for_add.clone(), cx);
                        }))
                    })
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .gap_0p5()
                            .child(
                                div()
                                    .text_sm()
                                    .text_color(rgb(text_primary()))
                                    .child(display_name),
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(rgb(text_secondary()))
                                    .child(subtitle),
                            ),
                    )
                    .child(if selected {
                        badge("Selected", panel_alt_bg(), text_secondary())
                    } else {
                        badge("Add", panel_alt_bg(), text_primary())
                    }),
            );
        }
    }

    let can_start = !new_chat.selected_participants.is_empty() && !new_chat.creating;

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
        .id("overlay-new-chat")
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
                .child("New chat"),
        )
        .child(
            div()
                .px_4()
                .py_3()
                .flex()
                .flex_col()
                .gap_2()
                .child(
                    div()
                        .id("new-chat-input")
                        .rounded_md()
                        .bg(panel_surface())
                        .border_1()
                        .border_color(shell_border())
                        .px_3()
                        .py_2()
                        .text_sm()
                        .text_color(rgb(text_primary()))
                        .on_click(cx.listener(AppWindow::focus_new_chat_input))
                        .child(new_chat_input.clone()),
                )
                .when(!new_chat.selected_participants.is_empty(), |container| {
                    container.child(selected_users)
                })
                .when_some(new_chat.error.as_ref(), |container, error| {
                    container.child(
                        div()
                            .text_xs()
                            .text_color(rgb(accent()))
                            .child(error.clone()),
                    )
                }),
        )
        .child(
            div()
                .flex_1()
                .min_h(px(0.))
                .px_4()
                .pb_3()
                .id("new-chat-results-scroll")
                .overflow_y_scroll()
                .on_scroll_wheel(cx.listener(AppWindow::consume_scroll_wheel))
                .child(results_list),
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
                        .id("new-chat-cancel")
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.dismiss_overlays(cx);
                        }))
                        .child(badge("Cancel", panel_alt_bg(), text_primary())),
                )
                .child(
                    div()
                        .id("new-chat-start")
                        .when(can_start, |button| {
                            button
                                .cursor_pointer()
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.new_chat_create(window, cx);
                                }))
                        })
                        .child(if new_chat.creating {
                            badge("Starting…", panel_alt_bg(), text_secondary())
                        } else if can_start {
                            badge("Start", accent(), panel_alt_bg())
                        } else {
                            badge("Start", panel_alt_bg(), text_secondary())
                        }),
                ),
        )
        .into_any_element()
}
