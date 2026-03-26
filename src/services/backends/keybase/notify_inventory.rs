#![cfg_attr(test, allow(dead_code))]

use rmpv::Value;

const KNOWN_NOTIFY_METHODS: &[&str] = &[
    "chat.1.NotifyChat.NewChatActivity",
    "chat.1.NotifyChat.ChatIdentifyUpdate",
    "chat.1.NotifyChat.ChatTLFFinalize",
    "chat.1.NotifyChat.ChatTLFResolve",
    "chat.1.NotifyChat.ChatInboxStale",
    "chat.1.NotifyChat.ChatThreadsStale",
    "chat.1.NotifyChat.ChatTypingUpdate",
    "chat.1.NotifyChat.ChatJoinedConversation",
    "chat.1.NotifyChat.ChatLeftConversation",
    "chat.1.NotifyChat.ChatResetConversation",
    "chat.1.NotifyChat.ChatInboxSyncStarted",
    "chat.1.NotifyChat.ChatInboxSynced",
    "chat.1.NotifyChat.ChatSetConvRetention",
    "chat.1.NotifyChat.ChatSetTeamRetention",
    "chat.1.NotifyChat.ChatSetConvSettings",
    "chat.1.NotifyChat.ChatSubteamRename",
    "chat.1.NotifyChat.ChatKBFSToImpteamUpgrade",
    "chat.1.NotifyChat.ChatAttachmentUploadStart",
    "chat.1.NotifyChat.ChatAttachmentUploadProgress",
    "chat.1.NotifyChat.ChatAttachmentDownloadProgress",
    "chat.1.NotifyChat.ChatAttachmentDownloadComplete",
    "chat.1.NotifyChat.ChatArchiveProgress",
    "chat.1.NotifyChat.ChatArchiveComplete",
    "chat.1.NotifyChat.ChatPaymentInfo",
    "chat.1.NotifyChat.ChatRequestInfo",
    "chat.1.NotifyChat.ChatPromptUnfurl",
    "chat.1.NotifyChat.ChatConvUpdate",
    "chat.1.NotifyChat.ChatWelcomeMessageLoaded",
    "chat.1.NotifyChat.ChatParticipantsInfo",
    "keybase.1.NotifyApp.exit",
    "keybase.1.NotifyAudit.rootAuditError",
    "keybase.1.NotifyAudit.boxAuditError",
    "keybase.1.NotifyBadges.badgeState",
    "keybase.1.NotifyCanUserPerform.canUserPerformChanged",
    "keybase.1.NotifyDeviceClone.deviceCloneCountChanged",
    "keybase.1.NotifyEmailAddress.emailAddressVerified",
    "keybase.1.NotifyEmailAddress.emailsChanged",
    "keybase.1.NotifyEphemeral.newTeamEk",
    "keybase.1.NotifyEphemeral.newTeambotEk",
    "keybase.1.NotifyEphemeral.teambotEkNeeded",
    "keybase.1.NotifyFavorites.favoritesChanged",
    "keybase.1.NotifyFeaturedBots.featuredBotsUpdate",
    "keybase.1.NotifyFS.FSActivity",
    "keybase.1.NotifyFS.FSPathUpdated",
    "keybase.1.NotifyFS.FSSyncActivity",
    "keybase.1.NotifyFS.FSEditListResponse",
    "keybase.1.NotifyFS.FSSyncStatusResponse",
    "keybase.1.NotifyFS.FSOverallSyncStatusChanged",
    "keybase.1.NotifyFS.FSFavoritesChanged",
    "keybase.1.NotifyFS.FSOnlineStatusChanged",
    "keybase.1.NotifyFS.FSSubscriptionNotifyPath",
    "keybase.1.NotifyFS.FSSubscriptionNotify",
    "keybase.1.NotifyFSRequest.FSEditListRequest",
    "keybase.1.NotifyFSRequest.FSSyncStatusRequest",
    "keybase.1.NotifyInviteFriends.updateInviteCounts",
    "keybase.1.NotifyKeyfamily.keyfamilyChanged",
    "keybase.1.NotifyPaperKey.paperKeyCached",
    "keybase.1.NotifyPGP.pgpKeyInSecretStoreFile",
    "keybase.1.NotifyPhoneNumber.phoneNumbersChanged",
    "keybase.1.NotifyRuntimeStats.runtimeStatsUpdate",
    "keybase.1.NotifySaltpack.saltpackOperationStart",
    "keybase.1.NotifySaltpack.saltpackOperationProgress",
    "keybase.1.NotifySaltpack.saltpackOperationDone",
    "keybase.1.NotifyService.HTTPSrvInfoUpdate",
    "keybase.1.NotifyService.handleKeybaseLink",
    "keybase.1.NotifyService.shutdown",
    "keybase.1.NotifySession.loggedOut",
    "keybase.1.NotifySession.loggedIn",
    "keybase.1.NotifySession.clientOutOfDate",
    "keybase.1.NotifySimpleFS.simpleFSArchiveStatusChanged",
    "keybase.1.NotifyTeam.teamChangedByID",
    "keybase.1.NotifyTeam.teamChangedByName",
    "keybase.1.NotifyTeam.teamDeleted",
    "keybase.1.NotifyTeam.teamAbandoned",
    "keybase.1.NotifyTeam.teamExit",
    "keybase.1.NotifyTeam.newlyAddedToTeam",
    "keybase.1.NotifyTeam.teamRoleMapChanged",
    "keybase.1.NotifyTeam.avatarUpdated",
    "keybase.1.NotifyTeam.teamMetadataUpdate",
    "keybase.1.NotifyTeam.teamTreeMembershipsPartial",
    "keybase.1.NotifyTeam.teamTreeMembershipsDone",
    "keybase.1.NotifyTeambot.newTeambotKey",
    "keybase.1.NotifyTeambot.teambotKeyNeeded",
    "keybase.1.NotifyTracking.trackingChanged",
    "keybase.1.NotifyTracking.trackingInfo",
    "keybase.1.NotifyTracking.notifyUserBlocked",
    "keybase.1.NotifyUsers.userChanged",
    "keybase.1.NotifyUsers.webOfTrustChanged",
    "keybase.1.NotifyUsers.passwordChanged",
    "keybase.1.NotifyUsers.identifyUpdate",
    "stellar.1.notify.paymentNotification",
    "stellar.1.notify.paymentStatusNotification",
    "stellar.1.notify.requestStatusNotification",
    "stellar.1.notify.accountDetailsUpdate",
    "stellar.1.notify.accountsUpdate",
    "stellar.1.notify.pendingPaymentsUpdate",
    "stellar.1.notify.recentPaymentsUpdate",
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeybaseNotifyKind {
    Known(&'static str),
}

impl KeybaseNotifyKind {
    pub fn from_method(method: &str) -> Option<Self> {
        KNOWN_NOTIFY_METHODS
            .iter()
            .copied()
            .find(|known| *known == method)
            .map(KeybaseNotifyKind::Known)
    }

    pub fn method_name(&self) -> &'static str {
        match self {
            KeybaseNotifyKind::Known(method) => method,
        }
    }

    pub fn all_known_methods() -> &'static [&'static str] {
        KNOWN_NOTIFY_METHODS
    }
}

#[derive(Clone, Debug)]
pub enum KeybaseNotifyEvent {
    Known {
        kind: KeybaseNotifyKind,
        raw_params: Value,
    },
    Unknown {
        method: String,
        raw_params: Value,
    },
}

impl KeybaseNotifyEvent {
    pub fn from_method(method: &str, raw_params: Value) -> Self {
        match KeybaseNotifyKind::from_method(method) {
            Some(kind) => Self::Known { kind, raw_params },
            None => Self::Unknown {
                method: method.to_string(),
                raw_params,
            },
        }
    }

    pub fn method_name(&self) -> &str {
        match self {
            KeybaseNotifyEvent::Known { kind, .. } => kind.method_name(),
            KeybaseNotifyEvent::Unknown { method, .. } => method,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::KeybaseNotifyKind;
    use std::{
        collections::BTreeSet,
        fs,
        path::{Path, PathBuf},
    };

    #[test]
    fn avdl_notify_methods_are_covered() {
        let methods = discovered_notify_methods();
        let known_methods: BTreeSet<&str> = KeybaseNotifyKind::all_known_methods()
            .iter()
            .copied()
            .collect();

        let missing: Vec<String> = methods
            .iter()
            .filter(|method| !known_methods.contains(method.as_str()))
            .cloned()
            .collect();

        assert!(
            missing.is_empty(),
            "missing notify method mappings: {missing:?}"
        );
    }

    fn discovered_notify_methods() -> BTreeSet<String> {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let avdl_root = manifest_dir
            .join("keybase-client")
            .join("protocol")
            .join("avdl");
        let mut methods = BTreeSet::new();

        for file in notify_avdl_files(&avdl_root) {
            if file.file_name().and_then(|value| value.to_str()) == Some("notify_ctl.avdl") {
                continue;
            }

            let Ok(contents) = fs::read_to_string(&file) else {
                continue;
            };
            let Some(namespace) = extract_namespace(&contents) else {
                continue;
            };
            let Some(protocol) = extract_protocol_name(&contents) else {
                continue;
            };

            for method in extract_void_methods(&contents) {
                methods.insert(format!("{namespace}.{protocol}.{method}"));
            }
        }

        methods
    }

    fn notify_avdl_files(avdl_root: &Path) -> Vec<PathBuf> {
        let mut files = Vec::new();

        let chat_notify = avdl_root.join("chat1").join("notify.avdl");
        if chat_notify.exists() {
            files.push(chat_notify);
        }

        let stellar_notify = avdl_root.join("stellar1").join("notify.avdl");
        if stellar_notify.exists() {
            files.push(stellar_notify);
        }

        let keybase_notify_root = avdl_root.join("keybase1");
        if let Ok(entries) = fs::read_dir(keybase_notify_root) {
            for entry in entries.flatten() {
                let path = entry.path();
                let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
                    continue;
                };
                if name.starts_with("notify_") && name.ends_with(".avdl") {
                    files.push(path);
                }
            }
        }

        files.sort();
        files
    }

    fn extract_namespace(contents: &str) -> Option<String> {
        let marker = "@namespace(\"";
        let start = contents.find(marker)?;
        let tail = &contents[start + marker.len()..];
        let end = tail.find('"')?;
        Some(tail[..end].to_string())
    }

    fn extract_protocol_name(contents: &str) -> Option<String> {
        for line in contents.lines() {
            let trimmed = line.trim();
            if !trimmed.starts_with("protocol ") {
                continue;
            }
            let name = trimmed
                .trim_start_matches("protocol ")
                .split_whitespace()
                .next()?
                .trim_end_matches('{')
                .to_string();
            return Some(name);
        }
        None
    }

    fn extract_void_methods(contents: &str) -> Vec<String> {
        let mut methods = Vec::new();

        for line in contents.lines() {
            let trimmed = line.trim();
            let Some(rest) = trimmed.strip_prefix("void ") else {
                continue;
            };
            let Some((method, _)) = rest.split_once('(') else {
                continue;
            };
            methods.push(method.trim().to_string());
        }

        methods
    }
}
