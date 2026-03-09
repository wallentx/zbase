use crate::{
    models::app_model::Connectivity,
    views::app_window::AppWindow,
    views::{
        arrow_left_icon, arrow_right_icon,
        avatar::{Avatar, ME_AVATAR_ASSET},
        glass_surface_strong, search_icon, subtle_surface, success, success_soft,
        text_primary, text_secondary,
    },
};
use gpui::prelude::FluentBuilder;
use gpui::{
    AnyElement, Context, FontWeight, InteractiveElement, IntoElement, ParentElement,
    SharedString, StatefulInteractiveElement, Styled, div, px, rgb,
};

#[derive(Default)]
pub struct CustomTitlebar;

impl CustomTitlebar {
    pub fn render(
        &self,
        workspace_name: &str,
        connectivity: &Connectivity,
        back_disabled: bool,
        forward_disabled: bool,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        let connectivity_tint = match connectivity {
            Connectivity::Online => success(),
            Connectivity::Reconnecting => 0xf59e0b,
            Connectivity::Offline => 0xef4444,
        };

        let back_button = if back_disabled {
            div()
                .w(px(28.))
                .h(px(28.))
                .flex()
                .items_center()
                .justify_center()
                .rounded_full()
                .bg(subtle_surface().opacity(0.45))
                .child(arrow_left_icon(text_secondary()))
                .into_any_element()
        } else {
            div()
                .id("titlebar-back")
                .w(px(28.))
                .h(px(28.))
                .rounded_full()
                .bg(subtle_surface())
                .flex()
                .items_center()
                .justify_center()
                .child(arrow_left_icon(text_primary()))
                .on_click(cx.listener(AppWindow::back_click))
                .into_any_element()
        };

        let forward_button = if forward_disabled {
            div()
                .w(px(28.))
                .h(px(28.))
                .flex()
                .items_center()
                .justify_center()
                .rounded_full()
                .bg(subtle_surface().opacity(0.45))
                .child(arrow_right_icon(text_secondary()))
                .into_any_element()
        } else {
            div()
                .id("titlebar-forward")
                .w(px(28.))
                .h(px(28.))
                .rounded_full()
                .bg(subtle_surface())
                .flex()
                .items_center()
                .justify_center()
                .child(arrow_right_icon(text_primary()))
                .on_click(cx.listener(AppWindow::forward_click))
                .into_any_element()
        };

        div()
            .h(px(48.))
            .px_4()
            .flex()
            .items_center()
            .justify_between()
            .bg(glass_surface_strong())
            .text_color(rgb(text_primary()))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .when(cfg!(target_os = "macos"), |div| div.pl(px(72.)))
                    .child(back_button)
                    .child(forward_button)
                    .child(
                        div()
                            .id("titlebar-home")
                            .on_click(cx.listener(AppWindow::home_click))
                            .px_2()
                            .text_sm()
                            .font_weight(FontWeight::SEMIBOLD)
                            .child(workspace_name.to_string()),
                    ),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .child(
                        div()
                            .id(SharedString::from("titlebar-search-trigger"))
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.toggle_quick_switcher(cx);
                            }))
                            .w(px(28.))
                            .h(px(28.))
                            .rounded_full()
                            .bg(subtle_surface())
                            .flex()
                            .items_center()
                            .justify_center()
                            .child(search_icon(text_secondary())),
                    )
                    .child(
                        div()
                            .w(px(6.))
                            .h(px(6.))
                            .rounded_full()
                            .bg(rgb(connectivity_tint)),
                    )
                    .child(
                        div()
                            .id(SharedString::from("titlebar-profile"))
                            .on_click(cx.listener(AppWindow::preferences_click))
                            .child(Avatar::render(
                                "You",
                                Some(ME_AVATAR_ASSET),
                                28.,
                                success_soft(),
                                0xffffff,
                            )),
                    ),
            )
            .into_any_element()
    }
}
