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

        let mut content = format!(
            "theme={}\ndensity={}\nreduced_motion={}\nshow_right_pane={}\n",
            theme_label(&settings.theme_mode),
            density_label(&settings.density),
            settings.reduced_motion,
            settings.show_right_pane
        );

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

        fs::write(path, content)
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
            _ => {}
        }
    }

    settings
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
