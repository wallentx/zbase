use crate::{
    models::composer_model::{ComposerMode, ComposerModel},
    views::{
        accent, app_window::AppWindow, attachment_display_label, badge, composer_tool,
        content_surface, emoji_icon, format_icon, input::TextField, link_icon, mention_icon,
        panel_alt_bg, panel_bg, plus_icon, shell_border, text_primary, text_secondary, video_icon,
    },
};
use gpui::prelude::FluentBuilder;
use gpui::{
    AnyElement, Context, Entity, InteractiveElement, IntoElement, ParentElement,
    StatefulInteractiveElement, Styled, div, px, rgb,
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
            ComposerMode::Edit { message_id } => Some(badge(
                format!("Editing {}", message_id.0),
                panel_alt_bg(),
                text_primary(),
            )),
            ComposerMode::ReplyInThread { root_id } => Some(badge(
                format!("Replying in thread {}", root_id.0),
                panel_alt_bg(),
                text_primary(),
            )),
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
                    .bg(content_surface())
                    .overflow_hidden()
                    .px_3()
                    .py_2()
                    .w_full()
                    .text_sm()
                    .line_height(px(22.))
                    .text_color(rgb(text_primary()))
                    .id("composer-input-surface")
                    .on_click(cx.listener(AppWindow::focus_composer_input))
                    .child(editor.clone()),
            )
            .when(!composer.attachments.is_empty(), |container| {
                container.child(
                    div()
                        .flex()
                        .gap_2()
                        .children(composer.attachments.iter().enumerate().map(
                            |(index, attachment)| {
                                let attachment_name = attachment.name.clone();
                                let attachment_label = attachment_display_label(attachment);
                                div()
                                    .rounded_md()
                                    .bg(content_surface())
                                    .px_2()
                                    .py_1()
                                    .flex()
                                    .items_center()
                                    .gap_2()
                                    .child(
                                        div()
                                            .text_xs()
                                            .text_color(rgb(text_primary()))
                                            .child(attachment_label),
                                    )
                                    .child(
                                        div()
                                            .id(("composer-attachment-open", index))
                                            .on_click(cx.listener(|this, _, _, cx| {
                                                this.open_files_pane(cx);
                                            }))
                                            .child(badge("Open", panel_bg(), text_primary())),
                                    )
                                    .child(
                                        div()
                                            .id(("composer-attachment-remove", index))
                                            .on_click(cx.listener(move |this, _, _, cx| {
                                                this.composer_remove_attachment(index, cx);
                                            }))
                                            .child(badge("Remove", panel_bg(), text_primary())),
                                    )
                                    .child(
                                        div()
                                            .id(("composer-attachment-menu", index))
                                            .on_click(cx.listener(move |this, _, _, cx| {
                                                this.open_attachment_context_menu(
                                                    attachment_name.clone(),
                                                    cx,
                                                );
                                            }))
                                            .child(badge("More", panel_bg(), text_primary())),
                                    )
                                    .into_any_element()
                            },
                        )),
                )
            })
            .when_some(composer.autocomplete.as_ref(), |container, autocomplete| {
                container.child(
                    div()
                        .px_2()
                        .text_xs()
                        .text_color(rgb(text_secondary()))
                        .child(format!(
                            "Autocomplete · {}{}",
                            autocomplete.trigger, autocomplete.query
                        )),
                )
            })
            .child(
                div()
                    .pt_1p5()
                    .border_t_1()
                    .border_color(shell_border())
                    .flex()
                    .items_center()
                    .justify_between()
                    .gap_2()
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .child(Self::tool_button(
                                "composer-tool-attach",
                                composer_tool(plus_icon(text_secondary()), text_secondary()),
                                cx.listener(|this, _, _, cx| {
                                    this.composer_add_attachment(cx);
                                    this.open_attachment_modal("composer", cx);
                                }),
                            ))
                            .child(Self::tool_button(
                                "composer-tool-emoji",
                                composer_tool(emoji_icon(text_secondary()), text_secondary()),
                                cx.listener(|this, _, _, cx| {
                                    this.toggle_emoji_picker(cx);
                                }),
                            ))
                            .child(Self::tool_button(
                                "composer-tool-mention",
                                composer_tool(mention_icon(text_secondary()), text_secondary()),
                                cx.listener(|this, _, _, cx| {
                                    this.composer_insert_mention(cx);
                                }),
                            ))
                            .child(Self::tool_button(
                                "composer-tool-format",
                                composer_tool(format_icon(text_secondary()), text_secondary()),
                                cx.listener(|this, _, _, cx| {
                                    this.composer_insert_formatting(cx);
                                }),
                            ))
                            .child(Self::tool_button(
                                "composer-tool-link",
                                composer_tool(link_icon(text_secondary()), text_secondary()),
                                cx.listener(|this, _, _, cx| {
                                    this.composer_insert_link(cx);
                                }),
                            ))
                            .child(Self::tool_button(
                                "composer-tool-call",
                                composer_tool(video_icon(text_secondary()), text_secondary()),
                                cx.listener(|this, _, window, cx| {
                                    this.start_or_open_call(window, cx);
                                }),
                            )),
                    )
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .justify_end()
                            .gap_2()
                            .flex_shrink_0()
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(rgb(text_secondary()))
                                    .child("Enter sends"),
                            )
                            .child(
                                div()
                                    .id("composer-send")
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.send_composer_message(window, cx);
                                    }))
                                    .child(badge("Send", accent(), panel_bg())),
                            ),
                    ),
            )
            .into_any_element()
    }

    fn tool_button(
        id: &'static str,
        content: AnyElement,
        listener: impl Fn(&gpui::ClickEvent, &mut gpui::Window, &mut gpui::App) + 'static,
    ) -> AnyElement {
        div()
            .id(id)
            .on_click(listener)
            .child(content)
            .into_any_element()
    }
}
