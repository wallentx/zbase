use crate::{
    domain::route::Route,
    models::AppModels,
    views::{
        MAIN_PANEL_MIN_WIDTH_PX, app_window::AppWindow, calls::CallWindow, content_surface,
        conversation::ConversationView, home::WorkspaceHomeView, inbox::InboxView,
        input::TextField, preferences::PreferencesView, search::SearchView, text_primary,
    },
};
use gpui::{
    AnyElement, Context, Entity, IntoElement, ListState, ParentElement, RenderImage, Styled, div,
    px, rgb,
};
use std::{collections::HashMap, sync::Arc};

#[derive(Default)]
pub struct MainPanelHost;

impl MainPanelHost {
    pub fn render(
        &self,
        models: &AppModels,
        video_render_cache: &HashMap<String, Arc<RenderImage>>,
        search_input: &Entity<TextField>,
        find_in_chat_input: &Entity<TextField>,
        composer_input: &Entity<TextField>,
        timeline_list_state: &ListState,
        timeline_unseen_count: usize,
        show_timeline_jump_to_bottom: bool,
        cx: &mut Context<AppWindow>,
    ) -> AnyElement {
        let content = match &models.navigation.current {
            Route::WorkspaceHome { .. } => WorkspaceHomeView.render(
                &models.app,
                &models.workspace,
                &models.notifications,
                cx,
            ),
            Route::Channel { .. } | Route::DirectMessage { .. } => ConversationView
                .render(
                    &models.conversation,
                    &models.timeline,
                    &models.find_in_chat,
                    find_in_chat_input,
                    &models.composer,
                    composer_input,
                    timeline_list_state,
                    timeline_unseen_count,
                    show_timeline_jump_to_bottom,
                    cx,
                ),
            Route::Search { .. } => {
                SearchView.render(&models.search, video_render_cache, search_input, cx)
            }
            Route::Activity { .. } => InboxView.render(&models.notifications, cx),
            Route::Preferences => PreferencesView.render(&models.settings, cx),
            Route::ActiveCall { .. } => CallWindow.render(&models.call),
        };

        div()
            .flex_1()
            .min_w(px(MAIN_PANEL_MIN_WIDTH_PX))
            .h_full()
            .flex()
            .flex_col()
            .bg(content_surface())
            .overflow_hidden()
            .text_color(rgb(text_primary()))
            .child(content)
            .into_any_element()
    }
}
