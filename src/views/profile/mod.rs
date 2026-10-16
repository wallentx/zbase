use crate::{
    domain::{
        affinity::Affinity,
        ids::UserId,
        presence::{Availability, Presence},
        profile::{ProfileSection, ProofState, SocialGraph, SocialGraphListType, UserProfile},
    },
    models::profile_panel_model::{ProfilePanelModel, SocialTab},
    views::{
        accent,
        app_window::AppWindow,
        avatar::{Avatar, default_avatar_background},
        badge, border, close_icon, danger, panel_alt_bg, panel_alt_surface, panel_surface,
        subtle_surface, success, text_primary, text_secondary, warning,
    },
};
use gpui::prelude::FluentBuilder;
use gpui::{
    AnyElement, Context, CursorStyle, FontWeight, InteractiveElement, IntoElement, ParentElement,
    ScrollHandle, StatefulInteractiveElement, Styled, div, px, rgb,
};

const ENV_PROFILE_DISABLE_SOCIAL_VIRTUALIZATION: &str =
    "ZBASE_PROFILE_DISABLE_SOCIAL_VIRTUALIZATION";
const SOCIAL_VIRTUALIZATION_MIN_ENTRIES: usize = 200;

pub fn render_profile_panel(
    profile_panel: &ProfilePanelModel,
    profile_scroll: &ScrollHandle,
    _profile_social_scroll: &ScrollHandle,
    cx: &mut Context<AppWindow>,
) -> AnyElement {
    let Some(user_id) = profile_panel.user_id.as_ref() else {
        return empty_profile_panel(cx);
    };
    let profile = profile_panel.profile.as_ref();
    let display_name = profile
        .map(|value| value.display_name.trim())
        .filter(|value| !value.is_empty())
        .unwrap_or(&user_id.0)
        .to_string();
    let username = profile
        .map(|value| value.username.trim())
        .filter(|value| !value.is_empty())
        .unwrap_or(&user_id.0)
        .to_string();
    let avatar_asset = profile.and_then(|value| value.avatar_asset.as_deref());
    let presence = profile
        .map(|value| value.presence.clone())
        .unwrap_or_else(default_presence);
    let affinity = profile
        .map(|value| value.affinity)
        .unwrap_or(Affinity::None);
    let social_graph = profile.and_then(profile_social_graph);
    let follow = social_graph.is_some_and(|graph| graph.you_are_following);
    let follow_label = if follow { "Unfollow" } else { "Follow" };
    let title = profile
        .and_then(|p| p.title.as_deref())
        .map(str::trim)
        .filter(|t| !t.is_empty());
    let bio = profile
        .and_then(|value| value.bio.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let location = profile
        .and_then(|value| value.location.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let has_bio_block = bio.is_some() || location.is_some();

    let identity_proofs = profile.map(profile_identity_proofs).unwrap_or(&[]);
    let team_showcase = profile.map(profile_team_showcase).unwrap_or(&[]);
    let custom_fields = profile.map(profile_custom_fields).unwrap_or(&[]);
    let social_entries = social_graph_entries(social_graph, profile_panel.active_social_tab);
    let use_social_virtualization = social_entries.len() >= SOCIAL_VIRTUALIZATION_MIN_ENTRIES
        && !social_virtualization_disabled();
    let (social_start_index, social_end_index, social_top_spacer_px, social_bottom_spacer_px) =
        if use_social_virtualization {
            social_entry_window(social_entries.len(), profile_scroll)
        } else {
            (0, social_entries.len(), 0.0, 0.0)
        };
    let visible_social_entries = &social_entries[social_start_index..social_end_index];

    div()
        .size_full()
        .id("right-pane-profile-scroll")
        .overflow_y_scroll()
        .track_scroll(profile_scroll)
        .when(use_social_virtualization, |panel| {
            panel.on_scroll_wheel(cx.listener(AppWindow::profile_scrolled))
        })
        .scrollbar_width(px(8.))
        .p_4()
        .flex()
        .flex_col()
        .gap_3()
        // Header with close icon (matches pane_header pattern)
        .child(pane_header(cx))
        // Hero card: avatar, name, presence, actions
        .child(
            div()
                .rounded_lg()
                .bg(panel_surface())
                .border_1()
                .border_color(rgb(border()))
                .p_4()
                .flex()
                .flex_col()
                .items_center()
                .gap_3()
                // Avatar with presence dot
                .child(
                    div()
                        .relative()
                        .child(Avatar::render(
                            &display_name,
                            avatar_asset,
                            80.,
                            default_avatar_background(&display_name),
                            text_primary(),
                        ))
                        .child(presence_dot(&presence)),
                )
                // Name + username + title
                .child(
                    div()
                        .flex()
                        .flex_col()
                        .items_center()
                        .gap_0p5()
                        .child(
                            div()
                                .font_weight(FontWeight::SEMIBOLD)
                                .text_color(rgb(profile_name_color(affinity)))
                                .child(display_name.clone()),
                        )
                        .child(
                            div()
                                .text_sm()
                                .text_color(rgb(text_secondary()))
                                .child(format!("@{username}")),
                        )
                        .when_some(title, |container, title| {
                            container.child(
                                div()
                                    .text_xs()
                                    .text_color(rgb(text_secondary()))
                                    .child(title.to_string()),
                            )
                        }),
                )
                // Action buttons
                .child(
                    div()
                        .flex()
                        .gap_2()
                        .child({
                            let user_id = user_id.clone();
                            div()
                                .id("profile-open-message")
                                .cursor(CursorStyle::PointingHand)
                                .hover(|s| s.opacity(0.85))
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    this.profile_open_message(user_id.clone(), cx);
                                }))
                                .child(badge("Message", panel_alt_bg(), text_primary()))
                        })
                        .child({
                            let user_id = user_id.clone();
                            div()
                                .id("profile-follow-toggle")
                                .cursor(CursorStyle::PointingHand)
                                .hover(|s| s.opacity(0.85))
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    if follow {
                                        this.profile_unfollow_user(user_id.clone(), cx);
                                    } else {
                                        this.profile_follow_user(user_id.clone(), cx);
                                    }
                                }))
                                .child(badge(
                                    follow_label,
                                    if follow { panel_alt_bg() } else { accent() },
                                    text_primary(),
                                ))
                        }),
                )
                // Refreshing indicator (stale data visible underneath)
                .when(profile_panel.loading && profile.is_some(), |container| {
                    container.child(
                        div()
                            .w_full()
                            .text_xs()
                            .text_color(rgb(text_secondary()))
                            .flex()
                            .justify_center()
                            .child("Updating\u{2026}"),
                    )
                }),
        )
        // Skeleton placeholders for first load
        .when(profile_panel.loading && profile.is_none(), |container| {
            container
                .child(skeleton_section(vec![px(180.), px(120.)]))
                .child(skeleton_section(vec![px(140.), px(100.), px(160.)]))
        })
        // Identity proofs
        .when(!identity_proofs.is_empty(), |container| {
            container.child(render_proofs_section(identity_proofs))
        })
        // Networks (teams)
        .when(!team_showcase.is_empty(), |container| {
            container.child(render_teams_section(team_showcase))
        })
        // Bio + location block
        .when(has_bio_block, |container| {
            container.child(
                div()
                    .rounded_lg()
                    .bg(panel_surface())
                    .border_1()
                    .border_color(rgb(border()))
                    .p_3()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .when_some(bio, |card, bio| {
                        card.child(
                            div()
                                .text_sm()
                                .text_color(rgb(text_primary()))
                                .child(bio.to_string()),
                        )
                    })
                    .when_some(location, |card, location| {
                        card.child(
                            div()
                                .text_xs()
                                .text_color(rgb(text_secondary()))
                                .child(format!("\u{1F4CD} {location}")),
                        )
                    }),
            )
        })
        // Custom fields
        .when(!custom_fields.is_empty(), |container| {
            container.child(render_custom_fields_section(custom_fields))
        })
        // Followers / following (social graph)
        .when_some(social_graph, |container, graph| {
            let followers_count = graph.followers_count.unwrap_or(0);
            let following_count = graph.following_count.unwrap_or(0);
            container.child(
                div()
                    .rounded_lg()
                    .bg(panel_surface())
                    .border_1()
                    .border_color(rgb(border()))
                    .p_3()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .child(
                        div()
                            .flex()
                            .border_b_1()
                            .border_color(rgb(border()))
                            .gap_4()
                            .child(social_tab(
                                "Followers",
                                followers_count,
                                profile_panel.active_social_tab == SocialTab::Followers,
                                "profile-followers-tab",
                                SocialGraphListType::Followers,
                                cx,
                            ))
                            .child(social_tab(
                                "Following",
                                following_count,
                                profile_panel.active_social_tab == SocialTab::Following,
                                "profile-following-tab",
                                SocialGraphListType::Following,
                                cx,
                            )),
                    )
                    .when(profile_panel.loading_social_list, |section| {
                        section.child(
                            div()
                                .rounded_md()
                                .bg(panel_alt_surface())
                                .p_3()
                                .text_sm()
                                .text_color(rgb(text_secondary()))
                                .child("Loading people\u{2026}"),
                        )
                    })
                    .when(
                        !profile_panel.loading_social_list && social_entries.is_empty(),
                        |section| {
                            section.child(
                                div()
                                    .rounded_md()
                                    .bg(panel_alt_surface())
                                    .p_3()
                                    .text_sm()
                                    .text_color(rgb(text_secondary()))
                                    .child("No people to show yet."),
                            )
                        },
                    )
                    .child({
                        let render_entry =
                            |index: usize, entry: &crate::domain::profile::SocialGraphEntry| {
                                let entry_user_id = entry.user_id.clone();
                                let entry_name = if entry.display_name.trim().is_empty() {
                                    entry.user_id.0.clone()
                                } else {
                                    entry.display_name.clone()
                                };
                                div()
                                    .id(("profile-social-entry", index))
                                    .rounded_md()
                                    .bg(panel_alt_surface())
                                    .px_2()
                                    .py_2()
                                    .flex()
                                    .items_center()
                                    .justify_between()
                                    .cursor(CursorStyle::PointingHand)
                                    .hover(|s| s.bg(subtle_surface()))
                                    .on_click(cx.listener(move |this, _, window, cx| {
                                        this.open_user_profile_panel(
                                            entry_user_id.clone(),
                                            window,
                                            cx,
                                        );
                                    }))
                                    .child(
                                        div()
                                            .flex()
                                            .items_center()
                                            .gap_2()
                                            .child(Avatar::render(
                                                &entry_name,
                                                entry.avatar_asset.as_deref(),
                                                28.,
                                                default_avatar_background(&entry_name),
                                                text_primary(),
                                            ))
                                            .child(
                                                div()
                                                    .text_sm()
                                                    .text_color(rgb(profile_name_color(
                                                        entry.affinity,
                                                    )))
                                                    .child(entry_name),
                                            ),
                                    )
                                    .into_any_element()
                            };

                        if use_social_virtualization {
                            div()
                                .id("profile-social-list-scroll")
                                .flex()
                                .flex_col()
                                .when(social_top_spacer_px > 0.0, |list_container| {
                                    list_container.child(div().h(px(social_top_spacer_px)))
                                })
                                .children(visible_social_entries.iter().enumerate().map(
                                    |(visible_offset, entry)| {
                                        let index = social_start_index + visible_offset;
                                        render_entry(index, entry)
                                    },
                                ))
                                .when(social_bottom_spacer_px > 0.0, |list_container| {
                                    list_container.child(div().h(px(social_bottom_spacer_px)))
                                })
                                .into_any_element()
                        } else {
                            div()
                                .flex()
                                .flex_col()
                                .children(
                                    social_entries
                                        .iter()
                                        .enumerate()
                                        .map(|(index, entry)| render_entry(index, entry)),
                                )
                                .into_any_element()
                        }
                    }),
            )
        })
        .into_any_element()
}

pub fn render_profile_card(
    profile_panel: &ProfilePanelModel,
    user_id: &UserId,
    cx: &mut Context<AppWindow>,
) -> AnyElement {
    let profile = profile_panel
        .profile
        .as_ref()
        .filter(|profile| profile.user_id == *user_id);
    let display_name = profile
        .map(|value| value.display_name.trim())
        .filter(|value| !value.is_empty())
        .unwrap_or(&user_id.0)
        .to_string();
    let username = profile
        .map(|value| value.username.trim())
        .filter(|value| !value.is_empty())
        .unwrap_or(&user_id.0)
        .to_string();
    let avatar_asset = profile.and_then(|value| value.avatar_asset.as_deref());
    let presence = profile
        .map(|value| value.presence.clone())
        .unwrap_or_else(default_presence);
    let affinity = profile
        .map(|value| value.affinity)
        .unwrap_or(Affinity::None);
    let social_graph = profile.and_then(profile_social_graph);
    let follow = social_graph.is_some_and(|graph| graph.you_are_following);

    div()
        .rounded_xl()
        .bg(panel_surface())
        .border_1()
        .border_color(rgb(border()))
        .shadow(crate::views::card_shadow())
        .w(px(320.))
        .p_3()
        .flex()
        .flex_col()
        .gap_2()
        .id("profile-card")
        .on_click(cx.listener(|_, _, _, cx| {
            cx.stop_propagation();
        }))
        .child(
            div()
                .flex()
                .items_center()
                .gap_3()
                .child(
                    div()
                        .relative()
                        .child(Avatar::render(
                            &display_name,
                            avatar_asset,
                            48.,
                            default_avatar_background(&display_name),
                            text_primary(),
                        ))
                        .child(presence_dot_small(&presence)),
                )
                .child(
                    div()
                        .flex()
                        .flex_col()
                        .gap_0p5()
                        .child(
                            div()
                                .font_weight(FontWeight::SEMIBOLD)
                                .text_color(rgb(profile_name_color(affinity)))
                                .child(display_name),
                        )
                        .child(
                            div()
                                .text_xs()
                                .text_color(rgb(text_secondary()))
                                .child(format!("@{username}")),
                        ),
                ),
        )
        .when_some(
            profile
                .and_then(|value| value.bio.as_deref())
                .map(str::trim)
                .filter(|value| !value.is_empty()),
            |container, bio| {
                container.child(
                    div()
                        .text_sm()
                        .text_color(rgb(text_secondary()))
                        .child(bio.to_string()),
                )
            },
        )
        .when_some(social_graph, |container, graph| {
            container.child(
                div()
                    .text_xs()
                    .text_color(rgb(text_secondary()))
                    .child(format!(
                        "{} followers \u{00B7} {} following",
                        graph.followers_count.unwrap_or(0),
                        graph.following_count.unwrap_or(0)
                    )),
            )
        })
        .child(
            div()
                .pt_1()
                .flex()
                .gap_2()
                .child({
                    let user_id = user_id.clone();
                    div()
                        .id("profile-card-view-full")
                        .cursor(CursorStyle::PointingHand)
                        .hover(|s| s.opacity(0.85))
                        .on_click(cx.listener(move |this, _, window, cx| {
                            this.open_user_profile_panel(user_id.clone(), window, cx);
                        }))
                        .child(badge("View profile", accent(), text_primary()))
                })
                .child({
                    let user_id = user_id.clone();
                    div()
                        .id("profile-card-follow-toggle")
                        .cursor(CursorStyle::PointingHand)
                        .hover(|s| s.opacity(0.85))
                        .on_click(cx.listener(move |this, _, _, cx| {
                            if follow {
                                this.profile_unfollow_user(user_id.clone(), cx);
                            } else {
                                this.profile_follow_user(user_id.clone(), cx);
                            }
                        }))
                        .child(badge(
                            if follow { "Unfollow" } else { "Follow" },
                            panel_alt_bg(),
                            text_primary(),
                        ))
                })
                .child(
                    div()
                        .id("profile-card-close")
                        .cursor(CursorStyle::PointingHand)
                        .hover(|s| s.opacity(0.85))
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.dismiss_overlays(cx);
                        }))
                        .child(badge("Close", panel_alt_bg(), text_primary())),
                ),
        )
        .into_any_element()
}

fn pane_header(cx: &mut Context<AppWindow>) -> AnyElement {
    div()
        .flex()
        .items_center()
        .justify_between()
        .child(
            div()
                .font_weight(FontWeight::SEMIBOLD)
                .text_color(rgb(text_primary()))
                .child("Profile"),
        )
        .child(
            div()
                .id("profile-pane-close")
                .on_click(cx.listener(|this, _, window, cx| {
                    this.close_right_pane(window, cx);
                }))
                .w(px(22.))
                .h(px(22.))
                .rounded_full()
                .flex()
                .items_center()
                .justify_center()
                .cursor(CursorStyle::PointingHand)
                .hover(|s| s.bg(subtle_surface()))
                .child(close_icon(text_secondary())),
        )
        .into_any_element()
}

fn empty_profile_panel(cx: &mut Context<AppWindow>) -> AnyElement {
    div()
        .size_full()
        .id("right-pane-profile-scroll")
        .overflow_y_scroll()
        .scrollbar_width(px(8.))
        .p_4()
        .flex()
        .flex_col()
        .gap_3()
        .child(pane_header(cx))
        .child(
            div()
                .rounded_lg()
                .bg(panel_alt_surface())
                .p_4()
                .text_sm()
                .text_color(rgb(text_secondary()))
                .child("Select a person to view their profile."),
        )
        .into_any_element()
}

fn skeleton_section(widths: Vec<gpui::Pixels>) -> AnyElement {
    div()
        .rounded_lg()
        .bg(panel_surface())
        .border_1()
        .border_color(rgb(border()))
        .p_3()
        .flex()
        .flex_col()
        .gap_2()
        .children(widths.into_iter().map(|w| {
            div()
                .h(px(12.))
                .w(w)
                .rounded_md()
                .bg(panel_alt_surface())
                .into_any_element()
        }))
        .into_any_element()
}

fn section_header(title: &str) -> AnyElement {
    div()
        .font_weight(FontWeight::MEDIUM)
        .text_sm()
        .text_color(rgb(text_secondary()))
        .child(title.to_string())
        .into_any_element()
}

fn social_tab(
    label: &str,
    count: u32,
    selected: bool,
    id: &'static str,
    list_type: SocialGraphListType,
    cx: &mut Context<AppWindow>,
) -> AnyElement {
    div()
        .id(id)
        .pb_2()
        .text_sm()
        .font_weight(FontWeight::MEDIUM)
        .cursor(CursorStyle::PointingHand)
        .text_color(rgb(if selected {
            text_primary()
        } else {
            text_secondary()
        }))
        .hover(|s| s.text_color(rgb(text_primary())))
        .when(selected, |tab| tab.border_b_2().border_color(rgb(accent())))
        .on_click(cx.listener(move |this, _, _, cx| {
            this.profile_select_social_tab(list_type, cx);
        }))
        .child(format!("{label} ({count})"))
        .into_any_element()
}

fn presence_dot(presence: &Presence) -> AnyElement {
    let color = presence_dot_color(&presence.availability);
    div()
        .absolute()
        .bottom(px(0.))
        .right(px(0.))
        .w(px(14.))
        .h(px(14.))
        .rounded_full()
        .bg(panel_surface())
        .flex()
        .items_center()
        .justify_center()
        .child(div().w(px(10.)).h(px(10.)).rounded_full().bg(rgb(color)))
        .into_any_element()
}

fn presence_dot_small(presence: &Presence) -> AnyElement {
    let color = presence_dot_color(&presence.availability);
    div()
        .absolute()
        .bottom(px(0.))
        .right(px(0.))
        .w(px(12.))
        .h(px(12.))
        .rounded_full()
        .bg(panel_surface())
        .flex()
        .items_center()
        .justify_center()
        .child(div().w(px(8.)).h(px(8.)).rounded_full().bg(rgb(color)))
        .into_any_element()
}

fn presence_dot_color(availability: &Availability) -> u32 {
    match availability {
        Availability::Active => success(),
        Availability::Away => warning(),
        Availability::DoNotDisturb => danger(),
        Availability::Offline | Availability::Unknown => text_secondary(),
    }
}

fn render_proofs_section(proofs: &[crate::domain::profile::IdentityProof]) -> AnyElement {
    div()
        .rounded_lg()
        .bg(panel_surface())
        .border_1()
        .border_color(rgb(border()))
        .p_3()
        .flex()
        .flex_col()
        .gap_2()
        .child(section_header("Identity proofs"))
        .children(proofs.iter().map(|proof| {
            let color = match proof.state {
                ProofState::Verified => success(),
                ProofState::Broken => danger(),
                ProofState::Pending => warning(),
                ProofState::Unknown => text_secondary(),
            };
            div()
                .rounded_md()
                .bg(panel_alt_surface())
                .px_2()
                .py_2()
                .overflow_hidden()
                .text_sm()
                .text_color(rgb(color))
                .child(format!(
                    "{} \u{00B7} {}",
                    proof.service_name, proof.service_username
                ))
                .into_any_element()
        }))
        .into_any_element()
}

fn render_teams_section(teams: &[crate::domain::profile::TeamShowcaseEntry]) -> AnyElement {
    div()
        .rounded_lg()
        .bg(panel_surface())
        .border_1()
        .border_color(rgb(border()))
        .p_3()
        .flex()
        .flex_col()
        .gap_2()
        .child(section_header("Teams"))
        .children(teams.iter().map(|team| {
            div()
                .rounded_md()
                .bg(panel_alt_surface())
                .px_2()
                .py_2()
                .flex()
                .flex_col()
                .gap_0p5()
                .child(
                    div()
                        .text_sm()
                        .text_color(rgb(text_primary()))
                        .child(team.name.clone()),
                )
                .when(!team.description.trim().is_empty(), |card| {
                    card.child(
                        div()
                            .text_xs()
                            .text_color(rgb(text_secondary()))
                            .child(team.description.clone()),
                    )
                })
                .child(
                    div()
                        .text_xs()
                        .text_color(rgb(text_secondary()))
                        .child(format!("{} members", team.members_count)),
                )
                .into_any_element()
        }))
        .into_any_element()
}

fn render_custom_fields_section(fields: &[crate::domain::profile::CustomField]) -> AnyElement {
    div()
        .rounded_lg()
        .bg(panel_surface())
        .border_1()
        .border_color(rgb(border()))
        .p_3()
        .flex()
        .flex_col()
        .gap_2()
        .child(section_header("Details"))
        .children(fields.iter().map(|field| {
            div()
                .rounded_md()
                .bg(panel_alt_surface())
                .px_2()
                .py_2()
                .flex()
                .justify_between()
                .items_center()
                .child(
                    div()
                        .text_xs()
                        .text_color(rgb(text_secondary()))
                        .child(field.label.clone()),
                )
                .child(
                    div()
                        .text_sm()
                        .text_color(rgb(text_primary()))
                        .child(field.value.clone()),
                )
                .into_any_element()
        }))
        .into_any_element()
}

fn profile_social_graph(profile: &UserProfile) -> Option<&SocialGraph> {
    profile.sections.iter().find_map(|section| match section {
        ProfileSection::SocialGraph(graph) => Some(graph),
        _ => None,
    })
}

fn social_graph_entries(
    social_graph: Option<&SocialGraph>,
    tab: SocialTab,
) -> &[crate::domain::profile::SocialGraphEntry] {
    match (social_graph, tab) {
        (Some(graph), SocialTab::Followers) => graph.followers.as_deref().unwrap_or(&[]),
        (Some(graph), SocialTab::Following) => graph.following.as_deref().unwrap_or(&[]),
        (None, _) => &[],
    }
}

const SOCIAL_ENTRY_ESTIMATED_HEIGHT_PX: f32 = 44.0;
const SOCIAL_ENTRY_INITIAL_RENDER_ROWS: usize = 180;
const SOCIAL_ENTRY_PROGRESSIVE_CHUNK_ROWS: usize = 72;
const SOCIAL_ENTRY_MAX_RENDER_ROWS: usize = 260;

fn social_entry_window(
    total_entries: usize,
    profile_scroll: &ScrollHandle,
) -> (usize, usize, f32, f32) {
    if total_entries == 0 {
        return (0, 0, 0.0, 0.0);
    }
    if total_entries <= SOCIAL_ENTRY_INITIAL_RENDER_ROWS {
        return (0, total_entries, 0.0, 0.0);
    }

    let max_offset = profile_scroll.max_offset();
    let progress = if max_offset.height <= px(0.) {
        0.0
    } else {
        let mut scroll_top = profile_scroll.offset().y.abs();
        if scroll_top > max_offset.height {
            scroll_top = max_offset.height;
        }
        (f32::from(scroll_top) / f32::from(max_offset.height)).clamp(0.0, 1.0)
    };
    let remaining = total_entries.saturating_sub(SOCIAL_ENTRY_INITIAL_RENDER_ROWS);
    let revealed = ((remaining as f32) * progress).ceil() as usize;
    let reveal_chunks = revealed.div_ceil(SOCIAL_ENTRY_PROGRESSIVE_CHUNK_ROWS);
    let revealed_rows = reveal_chunks.saturating_mul(SOCIAL_ENTRY_PROGRESSIVE_CHUNK_ROWS);
    let end = (SOCIAL_ENTRY_INITIAL_RENDER_ROWS + revealed_rows).min(total_entries);
    let start = end.saturating_sub(SOCIAL_ENTRY_MAX_RENDER_ROWS);
    let top_spacer_px = (start as f32) * SOCIAL_ENTRY_ESTIMATED_HEIGHT_PX;
    let bottom_spacer_px =
        ((total_entries.saturating_sub(end)) as f32) * SOCIAL_ENTRY_ESTIMATED_HEIGHT_PX;
    (start, end, top_spacer_px, bottom_spacer_px)
}

fn social_virtualization_disabled() -> bool {
    std::env::var(ENV_PROFILE_DISABLE_SOCIAL_VIRTUALIZATION)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn profile_identity_proofs(profile: &UserProfile) -> &[crate::domain::profile::IdentityProof] {
    profile
        .sections
        .iter()
        .find_map(|section| match section {
            ProfileSection::IdentityProofs(items) => Some(items.as_slice()),
            _ => None,
        })
        .unwrap_or(&[])
}

fn profile_team_showcase(profile: &UserProfile) -> &[crate::domain::profile::TeamShowcaseEntry] {
    profile
        .sections
        .iter()
        .find_map(|section| match section {
            ProfileSection::TeamShowcase(items) => Some(items.as_slice()),
            _ => None,
        })
        .unwrap_or(&[])
}

fn profile_custom_fields(profile: &UserProfile) -> &[crate::domain::profile::CustomField] {
    profile
        .sections
        .iter()
        .find_map(|section| match section {
            ProfileSection::CustomFields(items) => Some(items.as_slice()),
            _ => None,
        })
        .unwrap_or(&[])
}

fn presence_label(presence: &Presence) -> String {
    let base = match presence.availability {
        Availability::Active => "Active",
        Availability::Away => "Away",
        Availability::DoNotDisturb => "Do not disturb",
        Availability::Offline => "Offline",
        Availability::Unknown => "",
    };
    let status = presence.status_text.as_deref().map(str::trim).unwrap_or("");
    match (base.is_empty(), status.is_empty()) {
        (true, true) => String::new(),
        (true, false) => status.to_string(),
        (false, true) => base.to_string(),
        (false, false) => format!("{base} \u{00B7} {status}"),
    }
}

fn profile_name_color(affinity: Affinity) -> u32 {
    match affinity {
        Affinity::None => text_primary(),
        Affinity::Positive => success(),
        Affinity::Broken => danger(),
    }
}

fn default_presence() -> Presence {
    Presence {
        availability: Availability::Unknown,
        status_text: None,
    }
}
