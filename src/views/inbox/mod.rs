use crate::{
    models::notifications_model::{ActivityItem, ActivityKind, NotificationsModel},
    views::{
        accent, accent_soft, app_window::AppWindow, badge, border, panel_alt_bg, panel_alt_surface,
        panel_surface, success, success_soft, text_primary, text_secondary, tint, warning,
        warning_soft,
    },
};
use gpui::{
    AnyElement, Context, FontWeight, InteractiveElement, IntoElement, ParentElement, SharedString,
    StatefulInteractiveElement, Styled, div, px, rgb,
};

#[derive(Default)]
pub struct InboxView;

impl InboxView {
    pub fn render(
        &self,
        notifications: &NotificationsModel,
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
            .child(div().font_weight(FontWeight::SEMIBOLD).child("Inbox"))
            .child(badge(
                format!("{} unread items", notifications.notification_center_count),
                panel_alt_bg(),
                warning(),
            ))
            .child(
                div()
                    .rounded_lg()
                    .bg(panel_alt_surface())
                    .p_4()
                    .text_sm()
                    .text_color(rgb(text_secondary()))
                    .child("Mentions, thread replies, reactions, and reminders will land here."),
            )
            .child(
                div()
                    .flex_1()
                    .id("inbox-items-scroll")
                    .overflow_y_scroll()
                    .scrollbar_width(px(8.))
                    .pr_2()
                    .children(notifications.activity_items.iter().enumerate().map(
                        |(index, item)| {
                            Self::render_item(
                                index,
                                item,
                                notifications.highlighted_index == Some(index),
                                cx,
                            )
                        },
                    )),
            )
            .into_any_element()
    }

    fn render_item(
        index: usize,
        item: &ActivityItem,
        highlighted: bool,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        let kind_label = match item.kind {
            ActivityKind::Mention => badge("Mention", warning_soft(), warning()),
            ActivityKind::ThreadReply => badge("Thread", success_soft(), success()),
            ActivityKind::Reaction => badge("Reaction", accent_soft(), accent()),
            ActivityKind::Reminder => badge("Reminder", panel_alt_bg(), text_primary()),
        };

        div()
            .id(SharedString::from(format!("inbox-item-{index}")))
            .rounded_lg()
            .border_1()
            .border_color(rgb(if highlighted { accent() } else { border() }))
            .bg(if highlighted {
                tint(accent_soft(), 0.80)
            } else {
                panel_surface()
            })
            .p_4()
            .flex()
            .justify_between()
            .items_center()
            .gap_4()
            .on_click(cx.listener(move |this, _, window, cx| {
                this.open_activity_item_at(index, window, cx);
            }))
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .child(
                        div().flex().items_center().gap_2().child(kind_label).child(
                            div()
                                .font_weight(FontWeight::MEDIUM)
                                .text_color(rgb(text_primary()))
                                .child(item.title.clone()),
                        ),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(rgb(text_secondary()))
                            .child(item.detail.clone()),
                    ),
            )
            .child(if item.unread {
                badge("Unread", success_soft(), success())
            } else {
                badge("Viewed", panel_alt_bg(), text_secondary())
            })
            .into_any_element()
    }
}
