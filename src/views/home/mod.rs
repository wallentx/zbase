use crate::{
    domain::{
        conversation::ConversationSummary,
        ids::{ChannelId, DmId, WorkspaceId},
        route::Route,
    },
    models::{
        app_model::{AppModel, Connectivity},
        notifications_model::NotificationsModel,
        workspace_model::WorkspaceModel,
    },
    views::{
        accent, accent_soft, app_window::AppWindow, badge, border, panel_alt_bg, panel_alt_surface,
        panel_bg, panel_surface, text_primary, text_secondary,
    },
};
use gpui::{
    AnyElement, Context, FontWeight, InteractiveElement, IntoElement, ParentElement, SharedString,
    StatefulInteractiveElement, Styled, div, px, rgb,
};

#[derive(Default)]
pub struct WorkspaceHomeView;

impl WorkspaceHomeView {
    pub fn render(
        &self,
        app: &AppModel,
        workspace: &WorkspaceModel,
        notifications: &NotificationsModel,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        let connectivity = match app.connectivity {
            Connectivity::Online => "Online",
            Connectivity::Reconnecting => "Reconnecting",
            Connectivity::Offline => "Offline",
        };

        div()
            .flex_1()
            .m_4()
            .rounded_lg()
            .border_1()
            .border_color(rgb(border()))
            .bg(panel_surface())
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(
                div()
                    .flex_1()
                    .id("home-scroll")
                    .overflow_y_scroll()
                    .scrollbar_width(px(8.))
                    .p_6()
                    .pr_4()
                    .flex()
                    .flex_col()
                    .gap_6()
                    .child(
                        div()
                            .flex()
                            .justify_between()
                            .items_center()
                            .child(
                                div()
                                    .flex()
                                    .flex_col()
                                    .gap_2()
                                    .child(
                                        div()
                                            .font_weight(FontWeight::SEMIBOLD)
                                            .text_color(rgb(text_primary()))
                                            .child(workspace.workspace_name.clone()),
                                    )
                                    .child(
                                        div()
                                            .text_sm()
                                            .text_color(rgb(text_secondary()))
                                            .child("Slack-like shell status and quick routes"),
                                    ),
                            )
                            .child(
                                div()
                                    .flex()
                                    .gap_2()
                                    .child(badge(connectivity, accent_soft(), text_primary()))
                                    .child(badge(
                                        format!("{} unread", app.global_unread_count),
                                        panel_alt_bg(),
                                        text_primary(),
                                    ))
                                    .child(badge(
                                        format!(
                                            "{} inbox",
                                            notifications.notification_center_count
                                        ),
                                        panel_alt_bg(),
                                        text_primary(),
                                    )),
                            ),
                    )
                    .child(
                        div()
                            .flex()
                            .gap_3()
                            .child(
                                div()
                                    .id("home-search")
                                    .on_click(cx.listener(AppWindow::search_click))
                                    .child(badge("Search", accent(), panel_bg())),
                            )
                            .child(
                                div()
                                    .id("home-inbox")
                                    .on_click(cx.listener(AppWindow::activity_click))
                                    .child(badge("Inbox", panel_alt_bg(), text_primary())),
                            )
                            .child(
                                div()
                                    .id("home-preferences")
                                    .on_click(cx.listener(AppWindow::preferences_click))
                                    .child(badge("Preferences", panel_alt_bg(), text_primary())),
                            ),
                    )
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .gap_3()
                            .child(
                                div()
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .text_color(rgb(text_primary()))
                                    .child("Channels"),
                            )
                            .children(workspace.channels.iter().enumerate().map(
                                |(index, channel)| {
                                    Self::conversation_card(
                                        &workspace.workspace_id,
                                        channel,
                                        SharedString::from(format!("home-channel-{index}")),
                                        cx,
                                    )
                                },
                            )),
                    )
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .gap_3()
                            .child(
                                div()
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .text_color(rgb(text_primary()))
                                    .child("Direct messages"),
                            )
                            .children(workspace.direct_messages.iter().enumerate().map(
                                |(index, conversation)| {
                                    Self::conversation_card(
                                        &workspace.workspace_id,
                                        conversation,
                                        SharedString::from(format!("home-dm-{index}")),
                                        cx,
                                    )
                                },
                            )),
                    ),
            )
            .into_any_element()
    }

    fn conversation_card(
        workspace_id: &WorkspaceId,
        conversation: &ConversationSummary,
        element_id: SharedString,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        let route = route_for_conversation(workspace_id, conversation);

        div()
            .id(element_id)
            .rounded_lg()
            .border_1()
            .border_color(rgb(border()))
            .bg(panel_alt_surface())
            .p_4()
            .flex()
            .justify_between()
            .items_center()
            .on_click(cx.listener(move |this, _, window, cx| {
                this.navigate_to(route.clone(), window, cx);
            }))
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .child(
                        div()
                            .font_weight(FontWeight::MEDIUM)
                            .text_color(rgb(text_primary()))
                            .child(conversation.title.clone()),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(rgb(text_secondary()))
                            .child(conversation.topic.clone()),
                    ),
            )
            .child(
                div()
                    .flex()
                    .gap_2()
                    .child(badge(
                        format!("{} unread", conversation.unread_count),
                        panel_bg(),
                        text_primary(),
                    ))
                    .child(badge(
                        format!("{} mentions", conversation.mention_count),
                        panel_bg(),
                        text_primary(),
                    )),
            )
            .into_any_element()
    }
}

fn route_for_conversation(workspace_id: &WorkspaceId, conversation: &ConversationSummary) -> Route {
    match conversation.id.0.as_str() {
        "conv_alice" => Route::DirectMessage {
            workspace_id: workspace_id.clone(),
            dm_id: DmId::new("alice"),
        },
        other => {
            let channel_name = other.strip_prefix("conv_").unwrap_or("general");
            Route::Channel {
                workspace_id: workspace_id.clone(),
                channel_id: ChannelId::new(channel_name),
            }
        }
    }
}
