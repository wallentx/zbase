#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeybaseChatDeepLink {
    pub team: String,
    pub channel: Option<String>,
    pub message_id: String,
}

pub fn parse_keybase_chat_link(raw: &str) -> Option<KeybaseChatDeepLink> {
    let payload = raw.trim().strip_prefix("keybase://chat/")?;
    let (team_channel, message_id) = payload.rsplit_once('/')?;
    if message_id.trim().is_empty() {
        return None;
    }
    if let Some((team, channel)) = team_channel.split_once('#') {
        if team.is_empty() || channel.is_empty() {
            return None;
        }
        Some(KeybaseChatDeepLink {
            team: team.to_string(),
            channel: Some(channel.to_string()),
            message_id: message_id.to_string(),
        })
    } else {
        if team_channel.is_empty() {
            return None;
        }
        Some(KeybaseChatDeepLink {
            team: team_channel.to_string(),
            channel: None,
            message_id: message_id.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_team_channel_link() {
        let parsed = parse_keybase_chat_link("keybase://chat/chia_network#general/26360")
            .expect("expected keybase chat link to parse");
        assert_eq!(parsed.team, "chia_network");
        assert_eq!(parsed.channel.as_deref(), Some("general"));
        assert_eq!(parsed.message_id, "26360");
    }

    #[test]
    fn parses_dm_link() {
        let parsed = parse_keybase_chat_link("keybase://chat/alice,bob/42")
            .expect("expected dm link to parse");
        assert_eq!(parsed.team, "alice,bob");
        assert_eq!(parsed.channel, None);
        assert_eq!(parsed.message_id, "42");
    }

    #[test]
    fn parses_group_dm_link() {
        let parsed = parse_keybase_chat_link(
            "keybase://chat/adschmidtedly,cameroncooper,catherinerae,esaung,jde5011,paulhainsworth,s_shah22,storage_jm/14",
        )
        .expect("expected group dm link to parse");
        assert_eq!(
            parsed.team,
            "adschmidtedly,cameroncooper,catherinerae,esaung,jde5011,paulhainsworth,s_shah22,storage_jm"
        );
        assert_eq!(parsed.channel, None);
        assert_eq!(parsed.message_id, "14");
    }

    #[test]
    fn rejects_non_chat_link() {
        assert!(parse_keybase_chat_link("https://example.com").is_none());
    }
}
