use crate::{
    models::call_model::CallModel,
    views::{
        accent, app_window::AppWindow, badge, border, panel_alt_bg, panel_alt_surface, panel_bg,
        panel_surface, text_primary, text_secondary,
    },
};
use gpui::{
    AnyElement, Context, FontWeight, InteractiveElement, IntoElement, ParentElement,
    StatefulInteractiveElement, Styled, div, rgb,
};

#[derive(Default)]
pub struct MiniCallDock;

#[derive(Default)]
pub struct CallWindow;

impl MiniCallDock {
    pub fn render(&self, call: &CallModel, cx: &mut Context<AppWindow>) -> AnyElement {
        div()
            .m_4()
            .mt_0()
            .rounded_lg()
            .bg(panel_alt_surface())
            .px_4()
            .py_3()
            .flex()
            .items_center()
            .justify_between()
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .child("Mini call dock")
                    .child(badge(
                        format!("muted={}", call.is_muted),
                        panel_bg(),
                        text_primary(),
                    ))
                    .child(badge(
                        format!("sharing={}", call.is_sharing_screen),
                        panel_bg(),
                        text_primary(),
                    )),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .child(
                        div()
                            .id("call-dock-open")
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.start_or_open_call(window, cx);
                            }))
                            .child(badge("Open", panel_bg(), text_primary())),
                    )
                    .child(
                        div()
                            .id("call-dock-mute")
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.toggle_call_mute(cx);
                            }))
                            .child(badge("Mute", panel_bg(), text_primary())),
                    )
                    .child(
                        div()
                            .id("call-dock-share")
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.toggle_call_screen_share(cx);
                            }))
                            .child(badge("Share", panel_bg(), text_primary())),
                    )
                    .child(
                        div()
                            .id("call-dock-leave")
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.leave_call(window, cx);
                            }))
                            .child(badge("Leave", accent(), panel_bg())),
                    ),
            )
            .into_any_element()
    }
}

impl CallWindow {
    pub fn render(&self, call: &CallModel) -> AnyElement {
        let (status, participants) = call
            .active_call
            .as_ref()
            .map(|session| {
                (
                    format!("{:?}", session.status),
                    session.participants.clone(),
                )
            })
            .unwrap_or_else(|| ("Idle".to_string(), Vec::new()));

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
            .child(div().font_weight(FontWeight::SEMIBOLD).child("Call window"))
            .child(badge(status.clone(), panel_alt_bg(), text_primary()))
            .child(
                div()
                    .rounded_lg()
                    .bg(panel_alt_surface())
                    .p_4()
                    .text_sm()
                    .text_color(rgb(text_secondary()))
                    .child(match status.as_str() {
                        "SharingScreen" => "Screen share is active.",
                        "Reconnecting" => "Reconnecting media session…",
                        "ActiveVideo" => "Video call is active.",
                        _ => "Audio call is active.",
                    }),
            )
            .child(
                div()
                    .flex()
                    .gap_3()
                    .children(participants.into_iter().map(|participant| {
                        div()
                            .rounded_lg()
                            .bg(panel_alt_surface())
                            .px_3()
                            .py_2()
                            .text_sm()
                            .text_color(rgb(text_primary()))
                            .child(participant.display_name)
                            .into_any_element()
                    })),
            )
            .into_any_element()
    }
}
