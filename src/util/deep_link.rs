#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeybaseChatDeepLink {
    pub team: String,
    pub channel: String,
    pub message_id: String,
}

pub fn parse_keybase_chat_link(raw: &str) -> Option<KeybaseChatDeepLink> {
    let payload = raw.trim().strip_prefix("keybase://chat/")?;
    let (team_channel, message_id) = payload.rsplit_once('/')?;
    let (team, channel) = team_channel.split_once('#')?;
    if team.is_empty() || channel.is_empty() || message_id.trim().is_empty() {
        return None;
    }
    Some(KeybaseChatDeepLink {
        team: team.to_string(),
        channel: channel.to_string(),
        message_id: message_id.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_keybase_chat_link() {
        let parsed = parse_keybase_chat_link("keybase://chat/chia_network#general/26360")
            .expect("expected keybase chat link to parse");
        assert_eq!(parsed.team, "chia_network");
        assert_eq!(parsed.channel, "general");
        assert_eq!(parsed.message_id, "26360");
    }

    #[test]
    fn rejects_non_chat_link() {
        assert!(parse_keybase_chat_link("https://example.com").is_none());
    }
}
