use crate::models::settings_model::{Density, SettingsModel, ThemeMode};
use std::{
    fs,
    io::{self, ErrorKind},
    path::PathBuf,
};

#[derive(Default)]
pub struct SettingsStore;

impl SettingsStore {
    pub fn load_from_disk() -> io::Result<SettingsModel> {
        let path = Self::path();
        let raw = match fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(SettingsModel::default()),
            Err(err) => return Err(err),
        };

        Ok(parse_settings(&raw))
    }

    pub fn save_to_disk(settings: &SettingsModel) -> io::Result<()> {
        let path = Self::path();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, serialize_settings(settings))
    }

    fn path() -> PathBuf {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(".kbui")
            .join("settings.conf")
    }
}

fn parse_settings(raw: &str) -> SettingsModel {
    let mut settings = SettingsModel::default();

    for line in raw.lines() {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };

        let key = key.trim();
        let value = value.trim();

        if let Some(account_id) = key.strip_prefix("sidebar_section_order.") {
            let order = value
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            settings
                .sidebar_section_order
                .insert(account_id.to_string(), order);
            continue;
        }

        if let Some(account_id) = key.strip_prefix("sidebar_collapsed.") {
            let collapsed = value
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            settings
                .sidebar_collapsed_sections
                .insert(account_id.to_string(), collapsed);
            continue;
        }

        if let Some(conversation_id) = key.strip_prefix("dismissed_pinned.") {
            if !value.is_empty() {
                settings
                    .dismissed_pinned_items
                    .insert(conversation_id.to_string(), value.to_string());
            }
            continue;
        }

        if let Some(conversation_id) = key.strip_prefix("quick_switcher_affinity.") {
            if conversation_id.is_empty() {
                continue;
            }
            let Some((affinity_raw, last_updated_raw)) = value.split_once(',') else {
                continue;
            };
            let Some(affinity) = affinity_raw
                .trim()
                .parse::<f32>()
                .ok()
                .filter(|value| value.is_finite() && *value >= 0.0)
            else {
                continue;
            };
            let Some(last_updated_ms) = last_updated_raw.trim().parse::<i64>().ok() else {
                continue;
            };
            settings
                .quick_switcher_affinity
                .insert(conversation_id.to_string(), (affinity, last_updated_ms));
            continue;
        }

        match key {
            "theme" => {
                settings.theme_mode = match value {
                    "light" => ThemeMode::Light,
                    "dark" => ThemeMode::Dark,
                    _ => ThemeMode::System,
                };
            }
            "density" => {
                settings.density = match value {
                    "compact" => Density::Compact,
                    _ => Density::Comfortable,
                };
            }
            "reduced_motion" => {
                settings.reduced_motion = value == "true";
            }
            "show_right_pane" => {
                settings.show_right_pane = value != "false";
            }
            "emoji_skin_tone" => {
                settings.emoji_skin_tone = (!value.is_empty()).then(|| value.to_string());
            }
            "emoji_recents" => {
                settings.emoji_recents = value
                    .split(',')
                    .map(|entry| entry.trim().to_ascii_lowercase())
                    .filter(|entry| !entry.is_empty())
                    .collect();
            }
            _ => {}
        }
    }

    settings
}

fn serialize_settings(settings: &SettingsModel) -> String {
    let mut content = format!(
        "theme={}\ndensity={}\nreduced_motion={}\nshow_right_pane={}\n",
        theme_label(&settings.theme_mode),
        density_label(&settings.density),
        settings.reduced_motion,
        settings.show_right_pane
    );
    if let Some(skin_tone) = settings
        .emoji_skin_tone
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        content.push_str(&format!("emoji_skin_tone={skin_tone}\n"));
    }
    if !settings.emoji_recents.is_empty() {
        content.push_str(&format!(
            "emoji_recents={}\n",
            settings.emoji_recents.join(",")
        ));
    }

    for (account_id, order) in &settings.sidebar_section_order {
        content.push_str(&format!(
            "sidebar_section_order.{}={}\n",
            account_id,
            order.join(",")
        ));
    }

    for (account_id, collapsed) in &settings.sidebar_collapsed_sections {
        if !collapsed.is_empty() {
            let mut ids: Vec<&str> = collapsed.iter().map(String::as_str).collect();
            ids.sort();
            content.push_str(&format!(
                "sidebar_collapsed.{}={}\n",
                account_id,
                ids.join(",")
            ));
        }
    }

    for (conversation_id, pinned_item_id) in &settings.dismissed_pinned_items {
        if !pinned_item_id.trim().is_empty() {
            content.push_str(&format!(
                "dismissed_pinned.{}={}\n",
                conversation_id, pinned_item_id
            ));
        }
    }

    let mut affinity_entries = settings
        .quick_switcher_affinity
        .iter()
        .collect::<Vec<(&String, &(f32, i64))>>();
    affinity_entries.sort_by(|left, right| left.0.cmp(right.0));
    for (conversation_id, (affinity, last_updated_ms)) in affinity_entries {
        if conversation_id.trim().is_empty() || !affinity.is_finite() {
            continue;
        }
        content.push_str(&format!(
            "quick_switcher_affinity.{}={},{}\n",
            conversation_id,
            affinity.max(0.0),
            last_updated_ms
        ));
    }

    content
}

fn theme_label(theme: &ThemeMode) -> &'static str {
    match theme {
        ThemeMode::Light => "light",
        ThemeMode::Dark => "dark",
        ThemeMode::System => "system",
    }
}

fn density_label(density: &Density) -> &'static str {
    match density {
        Density::Comfortable => "comfortable",
        Density::Compact => "compact",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quick_switcher_affinity_round_trips_through_settings_content() {
        let mut settings = SettingsModel::default();
        settings
            .quick_switcher_affinity
            .insert("conv_geoff_a".to_string(), (3.25, 1_710_000_000_000));
        settings
            .quick_switcher_affinity
            .insert("conv_geoff_b".to_string(), (0.5, 1_709_000_000_000));

        let serialized = serialize_settings(&settings);
        assert!(serialized.contains("quick_switcher_affinity.conv_geoff_a=3.25,1710000000000"));
        assert!(serialized.contains("quick_switcher_affinity.conv_geoff_b=0.5,1709000000000"));

        let parsed = parse_settings(&serialized);
        assert_eq!(
            parsed.quick_switcher_affinity.get("conv_geoff_a"),
            Some(&(3.25, 1_710_000_000_000))
        );
        assert_eq!(
            parsed.quick_switcher_affinity.get("conv_geoff_b"),
            Some(&(0.5, 1_709_000_000_000))
        );
    }

    #[test]
    fn quick_switcher_affinity_parse_ignores_malformed_entries() {
        let raw = r#"
quick_switcher_affinity.conv_ok=1.5,1710000000000
quick_switcher_affinity.conv_missing_timestamp=2.5
quick_switcher_affinity.conv_bad_affinity=abc,1710000000001
quick_switcher_affinity.conv_bad_timestamp=2.5,not_ms
quick_switcher_affinity.=2.5,1710000000002
"#;
        let parsed = parse_settings(raw);
        assert_eq!(parsed.quick_switcher_affinity.len(), 1);
        assert_eq!(
            parsed.quick_switcher_affinity.get("conv_ok"),
            Some(&(1.5, 1_710_000_000_000))
        );
    }
}
