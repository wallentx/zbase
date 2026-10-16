use crate::views::{app_bg, app_window::AppWindow, is_dark_theme, text_primary, tint};
use gpui::{
    AnyElement, BoxShadow, Context, FontWeight, InteractiveElement, IntoElement, MouseButton,
    ParentElement, SharedString, Styled, div, img, point, px, rgb,
};

const APP_ICON_ASSET: &str = "assets/icons/app-icon.png";

#[derive(Default)]
pub struct SplashView;

impl SplashView {
    pub fn render(&self, status: &str, cx: &mut Context<AppWindow>) -> AnyElement {
        let kbd_bg = if is_dark_theme() {
            tint(0x283543, 0.85)
        } else {
            tint(0xffffff, 0.92)
        };
        let kbd_border = if is_dark_theme() {
            tint(0x4a5f74, 0.55)
        } else {
            tint(0xbcc8d4, 0.70)
        };
        let kbd_shadow = if is_dark_theme() {
            tint(0x000000, 0.30)
        } else {
            tint(0x7a8a9a, 0.18)
        };
        let subtitle_color = if is_dark_theme() { 0x7a8fa3 } else { 0x8a97a5 };
        let status_color = if is_dark_theme() { 0x5a6f83 } else { 0x9aa5b0 };

        div()
            .id("splash-screen")
            .absolute()
            .inset_0()
            .flex()
            .items_center()
            .justify_center()
            .bg(rgb(app_bg()))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, _, cx| {
                    this.clear_hovered_message_immediate(cx);
                    cx.stop_propagation();
                }),
            )
            .on_mouse_down(
                MouseButton::Right,
                cx.listener(|this, _, _, cx| {
                    this.clear_hovered_message_immediate(cx);
                    cx.stop_propagation();
                }),
            )
            .on_mouse_move(cx.listener(|this, _, _, cx| {
                this.clear_hovered_message_immediate(cx);
                cx.stop_propagation();
            }))
            .on_scroll_wheel(cx.listener(|this, _, _, cx| {
                this.clear_hovered_message_immediate(cx);
                cx.stop_propagation();
            }))
            .child(
                div()
                    .flex()
                    .flex_col()
                    .items_center()
                    .gap_8()
                    .child(
                        img(SharedString::from(APP_ICON_ASSET))
                            .w(px(192.))
                            .h(px(192.))
                            .flex_shrink_0(),
                    )
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .items_center()
                            .gap_3()
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .gap_1p5()
                                    .child(keycap("⌘", kbd_bg, kbd_border, kbd_shadow))
                                    .child(keycap("K", kbd_bg, kbd_border, kbd_shadow)),
                            )
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::NORMAL)
                                    .text_color(rgb(subtitle_color))
                                    .child("conversations · people · messages"),
                            )
                            .child(
                                div()
                                    .h(px(18.))
                                    .text_xs()
                                    .font_weight(FontWeight::NORMAL)
                                    .text_color(rgb(status_color))
                                    .child(status.to_string()),
                            ),
                    ),
            )
            .into_any_element()
    }
}

fn keycap(
    label: &str,
    bg: gpui::Hsla,
    border_color: gpui::Hsla,
    shadow_color: gpui::Hsla,
) -> AnyElement {
    div()
        .min_w(px(38.))
        .h(px(38.))
        .px_2p5()
        .rounded_lg()
        .bg(bg)
        .border_1()
        .border_color(border_color)
        .shadow(vec![BoxShadow {
            color: shadow_color,
            offset: point(px(0.), px(2.)),
            blur_radius: px(4.),
            spread_radius: px(0.),
        }])
        .flex()
        .items_center()
        .justify_center()
        .text_color(rgb(text_primary()))
        .text_base()
        .font_weight(FontWeight::MEDIUM)
        .child(label.to_string())
        .into_any_element()
}
