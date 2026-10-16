#![cfg(not(target_os = "windows"))]

use std::{env, path::PathBuf};

use rmpv::Value;
use tokio::{sync::mpsc, time};

#[path = "../services/backends/keybase/rpc/client.rs"]
#[allow(dead_code)]
mod client;
#[path = "../services/backends/keybase/paths.rs"]
#[allow(dead_code)]
mod keybase_paths;
#[path = "../services/backends/keybase/rpc/transport.rs"]
mod transport;

use client::KeybaseRpcClient;
use client::{NotificationChannels, RpcNotification};
use transport::FramedMsgpackTransport;

const GET_THREAD_REASON_FOREGROUND: i64 = 2;
const IDENTIFY_BEHAVIOR_CHAT_GUI: i64 = 2;
const MESSAGE_ID_CONTROL_MODE_OLDER: i64 = 0;
const MESSAGE_ID_CONTROL_MODE_NEWER: i64 = 1;

fn usage_and_exit() -> ! {
    eprintln!(
        "Usage:\n  Inspect a thread message:\n    cargo run --bin keybase_rpc_inspect -- <conversation_id_hex|kb_conv:...> [message_id]\n\n  Listen for NotifyChat.NewChatActivity frames:\n    cargo run --bin keybase_rpc_inspect -- --listen [--seconds N] [--max N]\n\n  Probe user search RPC:\n    cargo run --bin keybase_rpc_inspect -- --user-search <query>\n\n  Resolve a channel by team+topic name (tests getInboxAndUnboxLocal with tlfName+topicName):\n    cargo run --bin keybase_rpc_inspect -- --resolve <team_name> <channel_name>\n\nExamples:\n  cargo run --bin keybase_rpc_inspect -- kb_conv:0000722768... 1\n  cargo run --bin keybase_rpc_inspect -- --listen --seconds 10 --max 200\n  cargo run --bin keybase_rpc_inspect -- --user-search cameron\n  cargo run --bin keybase_rpc_inspect -- --resolve chia_network.projectnemo release\n"
    );
    std::process::exit(2);
}

fn strip_conv_prefix(raw: &str) -> &str {
    raw.strip_prefix("kb_conv:").unwrap_or(raw).trim()
}

fn hex_decode_bytes(hex: &str) -> Result<Vec<u8>, String> {
    let hex = hex.trim();
    if !hex.len().is_multiple_of(2) {
        return Err("hex string must have even length".to_string());
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let hi = (bytes[i] as char)
            .to_digit(16)
            .ok_or_else(|| format!("invalid hex at position {i}"))?;
        let lo = (bytes[i + 1] as char)
            .to_digit(16)
            .ok_or_else(|| format!("invalid hex at position {}", i + 1))?;
        out.push(((hi << 4) | lo) as u8);
        i += 2;
    }
    Ok(out)
}

fn value_key_preview(value: &Value) -> Vec<String> {
    let Value::Map(entries) = value else {
        return Vec::new();
    };
    entries
        .iter()
        .take(64)
        .map(|(key, _)| match key {
            Value::String(s) => s.as_str().unwrap_or("").to_string(),
            other => format!("{other:?}"),
        })
        .filter(|k| !k.trim().is_empty())
        .collect()
}

fn find_thread_messages_root(thread_result: &Value) -> Option<&Value> {
    let Value::Map(entries) = thread_result else {
        return None;
    };
    for (k, v) in entries {
        if k.as_str() == Some("messages") {
            return Some(v);
        }
        // Keybase getThreadLocal typically nests under "thread".
        if k.as_str() == Some("thread")
            && let Value::Map(thread_map) = v
        {
            for (tk, tv) in thread_map {
                if tk.as_str() == Some("messages") {
                    return Some(tv);
                }
                // Some shapes might nest messages under "thread" -> "thread"
                if tk.as_str() == Some("thread")
                    && let Value::Map(inner_thread) = tv
                {
                    for (itk, itv) in inner_thread {
                        if itk.as_str() == Some("messages") {
                            return Some(itv);
                        }
                    }
                }
            }
        }
        // Some shapes might nest under "result"
        if k.as_str() == Some("result")
            && let Value::Map(inner) = v
        {
            for (ik, iv) in inner {
                if ik.as_str() == Some("messages") {
                    return Some(iv);
                }
            }
        }
    }
    None
}

fn find_first_string(value: &Value, keys: &[&str]) -> Option<String> {
    fn walk(value: &Value, keys: &[&str], depth: usize) -> Option<String> {
        if depth > 16 {
            return None;
        }
        match value {
            Value::Map(entries) => {
                for (k, v) in entries {
                    if let Some(ks) = k.as_str()
                        && keys.iter().any(|wanted| wanted.eq_ignore_ascii_case(ks))
                        && let Some(s) = v.as_str()
                    {
                        return Some(s.to_string());
                    }
                    if let Some(found) = walk(v, keys, depth + 1) {
                        return Some(found);
                    }
                }
                None
            }
            Value::Array(items) => items.iter().find_map(|v| walk(v, keys, depth + 1)),
            _ => None,
        }
    }
    walk(value, keys, 0)
}

fn find_first_binary_hex(value: &Value, keys: &[&str]) -> Option<String> {
    fn hex_encode(bytes: &[u8]) -> String {
        let mut s = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            s.push_str(&format!("{:02x}", b));
        }
        s
    }
    fn walk(value: &Value, keys: &[&str], depth: usize) -> Option<String> {
        if depth > 16 {
            return None;
        }
        match value {
            Value::Map(entries) => {
                for (k, v) in entries {
                    if let Some(ks) = k.as_str()
                        && keys.iter().any(|wanted| wanted.eq_ignore_ascii_case(ks))
                        && let Value::Binary(bytes) = v
                    {
                        return Some(hex_encode(bytes));
                    }
                    if let Some(found) = walk(v, keys, depth + 1) {
                        return Some(found);
                    }
                }
                None
            }
            Value::Array(items) => items.iter().find_map(|v| walk(v, keys, depth + 1)),
            _ => None,
        }
    }
    walk(value, keys, 0)
}

fn find_first_map_by_key<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    fn walk<'a>(value: &'a Value, key: &str, depth: usize) -> Option<&'a Value> {
        if depth > 24 {
            return None;
        }
        match value {
            Value::Map(entries) => {
                for (k, v) in entries {
                    if k.as_str() == Some(key) {
                        return Some(v);
                    }
                    if let Some(found) = walk(v, key, depth + 1) {
                        return Some(found);
                    }
                }
                None
            }
            Value::Array(items) => items.iter().find_map(|v| walk(v, key, depth + 1)),
            _ => None,
        }
    }
    walk(value, key, 0)
}

fn message_type_from_message_body(message_body: &Value) -> Option<i64> {
    let Value::Map(entries) = message_body else {
        return None;
    };
    for key in ["messageType", "mt", "t"] {
        for (k, v) in entries {
            if k.as_str() == Some(key) {
                if let Some(i) = v.as_i64() {
                    return Some(i);
                }
                if let Some(s) = v.as_str()
                    && let Ok(i) = s.parse::<i64>()
                {
                    return Some(i);
                }
            }
        }
    }
    None
}

async fn listen_for_new_chat_activity(max: usize, seconds: u64) {
    let socket: PathBuf = match keybase_paths::socket_path() {
        Some(path) => path,
        None => {
            eprintln!(
                "Could not resolve Keybase socket path (set KEYBASE_SOCKET_PATH to override)."
            );
            std::process::exit(2);
        }
    };

    let transport = match FramedMsgpackTransport::connect(&socket).await {
        Ok(t) => t,
        Err(err) => {
            eprintln!("Failed to connect to Keybase socket at {socket:?}: {err}");
            std::process::exit(1);
        }
    };
    let mut client = KeybaseRpcClient::new(transport);
    if let Err(err) = client
        .set_notifications(NotificationChannels::all_enabled())
        .await
    {
        eprintln!("Failed to subscribe to notifications: {err}");
        std::process::exit(1);
    }

    let (tx, mut rx) = mpsc::channel::<RpcNotification>(4096);
    tokio::spawn(async move {
        let _ = client.run_notification_loop(tx).await;
    });

    let deadline = time::Instant::now() + time::Duration::from_secs(seconds.max(1));
    let mut seen = 0usize;
    let mut matched = 0usize;

    while seen < max && time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(time::Instant::now());
        let notification =
            match time::timeout(remaining.min(time::Duration::from_secs(2)), rx.recv()).await {
                Ok(Some(n)) => n,
                Ok(None) => break,
                Err(_) => continue,
            };
        seen += 1;

        if notification.method != "chat.1.NotifyChat.NewChatActivity" {
            continue;
        }

        // Heuristic extraction: find first "messageBody" map anywhere in params.
        let message_body = find_first_map_by_key(&notification.params, "messageBody");
        let message_type = message_body.and_then(message_type_from_message_body);
        let body_keys = message_body.map(value_key_preview).unwrap_or_default();

        if message_type == Some(0) || (body_keys.len() == 1 && body_keys[0] == "messageType") {
            matched += 1;
            let conv_hex = find_first_binary_hex(
                &notification.params,
                &[
                    "convID",
                    "convId",
                    "conversationID",
                    "conversationId",
                    "conversation_id",
                ],
            )
            .or_else(|| {
                find_first_string(&notification.params, &["conversation_id", "conversationID"])
            });

            let msg_id = find_first_string(
                &notification.params,
                &["messageID", "messageId", "msgID", "msgId", "id", "m"],
            );
            println!("\n--- Match #{matched} ---");
            if let Some(conv_hex) = conv_hex {
                println!("conversation_id={conv_hex}");
            }
            if let Some(msg_id) = msg_id {
                println!("candidate_message_id={msg_id}");
            }
            println!("message_type={message_type:?} message_body_keys={body_keys:?}");
            if let Some(message_body) = message_body {
                println!("messageBody={message_body:?}");
            } else {
                println!(
                    "No messageBody found in params; top-level keys={:?}",
                    value_key_preview(&notification.params)
                );
            }
        }
    }

    println!("\nDone. seen={seen} matched={matched} seconds={seconds}");
}

async fn inspect_user_search(query: &str) {
    let socket: PathBuf = match keybase_paths::socket_path() {
        Some(path) => path,
        None => {
            eprintln!(
                "Could not resolve Keybase socket path (set KEYBASE_SOCKET_PATH to override)."
            );
            std::process::exit(2);
        }
    };

    let transport = match FramedMsgpackTransport::connect(&socket).await {
        Ok(t) => t,
        Err(err) => {
            eprintln!("Failed to connect to Keybase socket at {socket:?}: {err}");
            std::process::exit(1);
        }
    };
    let mut client = KeybaseRpcClient::new(transport);
    let params = Value::Map(vec![
        (Value::from("query"), Value::from(query.to_string())),
        (Value::from("service"), Value::from("keybase")),
        (Value::from("maxResults"), Value::from(25)),
        (Value::from("includeServicesSummary"), Value::from(false)),
        (Value::from("includeContacts"), Value::from(false)),
    ]);
    let result = match client
        .call("keybase.1.userSearch.userSearch", vec![params])
        .await
    {
        Ok(value) => value,
        Err(err) => {
            eprintln!("RPC error calling keybase.1.userSearch.userSearch: {err}");
            std::process::exit(1);
        }
    };

    println!("--- raw result ---");
    println!("{result:#?}");
    let values = match &result {
        Value::Array(items) => items.clone(),
        Value::Map(_) => find_first_map_by_key(&result, "users")
            .and_then(|value| match value {
                Value::Array(items) => Some(items.clone()),
                _ => None,
            })
            .unwrap_or_default(),
        _ => Vec::new(),
    };
    println!("--- parsed count: {} ---", values.len());
    for (index, entry) in values.iter().take(10).enumerate() {
        let username =
            find_first_string(entry, &["keybaseUsername", "username", "name", "assertion"])
                .unwrap_or_else(|| "<none>".to_string());
        let full_name = find_first_string(
            entry,
            &["prettyName", "fullName", "fullname", "displayName"],
        )
        .unwrap_or_else(|| "<none>".to_string());
        println!("{index:>2}: username={username} full_name={full_name}");
    }
}

fn extract_message_body_candidate(message_entry: &Value) -> Option<&Value> {
    // We try a few shapes:
    // - { "msg": { "messageBody": ... } }
    // - { "msg": { "content": { "messageBody": ... } } }
    // - { "messageBody": ... } directly
    // - { "msg": { "valid": { "messageBody": ... } } } (notify-like)
    let Value::Map(entries) = message_entry else {
        return None;
    };

    for (k, v) in entries {
        if k.as_str() == Some("messageBody") {
            return Some(v);
        }
        if k.as_str() == Some("msg")
            && let Value::Map(msg_map) = v
        {
            for (mk, mv) in msg_map {
                if mk.as_str() == Some("messageBody") {
                    return Some(mv);
                }
                if mk.as_str() == Some("valid")
                    && let Value::Map(valid_map) = mv
                {
                    for (vk, vv) in valid_map {
                        if vk.as_str() == Some("messageBody") {
                            return Some(vv);
                        }
                    }
                }
                if mk.as_str() == Some("content")
                    && let Value::Map(content_map) = mv
                {
                    for (ck, cv) in content_map {
                        if ck.as_str() == Some("messageBody") {
                            return Some(cv);
                        }
                    }
                }
            }
        }
    }
    None
}

fn extract_message_id_candidate(message_entry: &Value) -> Option<i64> {
    // Try common keys:
    // - entry.msg.id
    // - entry.msg.messageID / messageId / msgID
    // - entry.messageID etc
    let as_i64 = |v: &Value| {
        v.as_i64()
            .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
    };
    let Value::Map(entries) = message_entry else {
        return None;
    };
    for (k, v) in entries {
        if matches!(
            k.as_str(),
            Some("messageID") | Some("messageId") | Some("msgID") | Some("msgId") | Some("id")
        ) && let Some(id) = as_i64(v)
        {
            return Some(id);
        }
        if k.as_str() == Some("msg")
            && let Value::Map(msg_map) = v
        {
            for (mk, mv) in msg_map {
                if matches!(
                    mk.as_str(),
                    Some("messageID")
                        | Some("messageId")
                        | Some("msgID")
                        | Some("msgId")
                        | Some("id")
                ) && let Some(id) = as_i64(mv)
                {
                    return Some(id);
                }
            }
        }
    }
    None
}

fn direct_map_get<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a Value> {
    let Value::Map(entries) = value else {
        return None;
    };
    for (k, v) in entries {
        if let Some(ks) = k.as_str()
            && keys.iter().any(|wanted| wanted.eq_ignore_ascii_case(ks))
        {
            return Some(v);
        }
    }
    None
}

const CHAT_GET_INBOX_AND_UNBOX_LOCAL: &str = "chat.1.local.getInboxAndUnboxLocal";
const TOPIC_TYPE_CHAT: i64 = 1;
const RESOLVE_IDENTIFY_BEHAVIOR: i64 = 2;
const TEAM_MEMBERS_TYPE: i64 = 1;

async fn inspect_resolve_channel(team_name: &str, channel_name: &str) {
    let socket: PathBuf = match keybase_paths::socket_path() {
        Some(path) => path,
        None => {
            eprintln!("Could not resolve Keybase socket path.");
            std::process::exit(2);
        }
    };
    println!("=== Resolve channel: {team_name}#{channel_name} ===");
    println!("socket: {socket:?}");

    let transport = match FramedMsgpackTransport::connect(&socket).await {
        Ok(t) => t,
        Err(err) => {
            eprintln!("Failed to connect to Keybase socket: {err}");
            std::process::exit(1);
        }
    };
    let mut client = KeybaseRpcClient::new(transport);

    let all_member_statuses = Value::Array(vec![
        Value::from(0i64),
        Value::from(1i64),
        Value::from(2i64),
        Value::from(3i64),
        Value::from(4i64),
        Value::from(5i64),
    ]);
    let query = Value::Map(vec![
        (Value::from("topicType"), Value::from(TOPIC_TYPE_CHAT)),
        (Value::from("status"), Value::Array(Vec::new())),
        (Value::from("memberStatus"), all_member_statuses),
        (Value::from("tlfName"), Value::from(team_name.to_string())),
        (
            Value::from("topicName"),
            Value::from(channel_name.to_string()),
        ),
        (Value::from("membersType"), Value::from(TEAM_MEMBERS_TYPE)),
        (Value::from("unreadOnly"), Value::from(false)),
        (Value::from("readOnly"), Value::from(false)),
        (Value::from("computeActiveList"), Value::from(false)),
    ]);

    let params = Value::Map(vec![
        (Value::from("query"), query),
        (
            Value::from("identifyBehavior"),
            Value::from(RESOLVE_IDENTIFY_BEHAVIOR),
        ),
    ]);

    println!("Calling {CHAT_GET_INBOX_AND_UNBOX_LOCAL}...");
    let result = match client
        .call(CHAT_GET_INBOX_AND_UNBOX_LOCAL, vec![params])
        .await
    {
        Ok(value) => value,
        Err(err) => {
            eprintln!("RPC error: {err}");
            std::process::exit(1);
        }
    };

    println!("Top-level keys: {:?}", value_key_preview(&result));

    let conversations = find_first_map_by_key(&result, "conversations");
    let conv_array = conversations
        .and_then(|v| match v {
            Value::Array(items) => Some(items.clone()),
            _ => None,
        })
        .or_else(|| match &result {
            Value::Map(entries) => {
                for (k, v) in entries {
                    if k.as_str() == Some("conversations")
                        && let Value::Array(items) = v
                    {
                        return Some(items.clone());
                    }
                }
                None
            }
            _ => None,
        })
        .unwrap_or_default();

    println!("conversations count: {}", conv_array.len());

    for (i, conv) in conv_array.iter().enumerate() {
        println!("\n--- conversation[{i}] ---");

        // Use direct map_get like the adapter does (not recursive find_first_string)
        let info_direct = direct_map_get(conv, &["info", "i", "conv", "conversation"]);
        let info_val = info_direct.cloned().unwrap_or(conv.clone());

        let tlf_direct = direct_map_get(&info_val, &["tlfName", "n"])
            .and_then(|v| v.as_str())
            .unwrap_or("<none>");
        let topic_direct = direct_map_get(&info_val, &["topicName", "t"])
            .and_then(|v| v.as_str())
            .unwrap_or("<none>");
        let members_type_direct =
            direct_map_get(&info_val, &["membersType", "m"]).and_then(|v| v.as_i64());
        let conv_id_hex = direct_map_get(&info_val, &["id", "i"])
            .and_then(|v| match v {
                Value::Binary(bytes) => {
                    let mut s = String::with_capacity(bytes.len() * 2);
                    for b in bytes {
                        s.push_str(&format!("{:02x}", b));
                    }
                    Some(s)
                }
                _ => None,
            })
            .unwrap_or_else(|| "<none>".to_string());

        let reader_info = direct_map_get(conv, &["readerInfo", "ri", "r"])
            .cloned()
            .unwrap_or(Value::Nil);
        let member_status = direct_map_get(&reader_info, &["status", "s"]).and_then(|v| v.as_i64());

        // Also show recursive result for comparison
        let tlf_recursive =
            find_first_string(&info_val, &["tlfName"]).unwrap_or_else(|| "<none>".to_string());

        println!("  tlfName(direct)={tlf_direct}");
        println!("  tlfName(recursive)={tlf_recursive}");
        println!("  topicName(direct)={topic_direct}");
        println!("  membersType={members_type_direct:?}");
        println!("  member_status={member_status:?} (0=active, 3=preview, 5=never_joined)");
        println!("  convID={conv_id_hex}");

        println!("  info keys: {:?}", value_key_preview(&info_val));
        if let Some(triple) = direct_map_get(&info_val, &["triple"]) {
            println!("  triple: {triple:?}");
        }

        let title_match = topic_direct.eq_ignore_ascii_case(channel_name);
        let team_match = tlf_direct.eq_ignore_ascii_case(team_name);
        let is_team = members_type_direct == Some(TEAM_MEMBERS_TYPE);
        println!(
            "  MATCH: title={title_match} team={team_match} is_team={is_team} → {}",
            if title_match && team_match && is_team {
                "YES ✓"
            } else {
                "NO"
            }
        );
    }

    if conv_array.is_empty() {
        println!("\nNo conversations returned. The RPC query returned empty results.");
        println!("This means getInboxAndUnboxLocal with tlfName+topicName filtering");
        println!("does NOT work for this team/channel combination.");
        println!("\nRaw result (first 2000 chars):");
        let raw = format!("{result:#?}");
        println!("{}", &raw[..raw.len().min(2000)]);
    }
}

async fn inspect_resolve_by_id(conv_hex: &str) {
    let raw_id = match hex_decode_bytes(conv_hex) {
        Ok(bytes) => bytes,
        Err(err) => {
            eprintln!("Invalid hex: {err}");
            std::process::exit(2);
        }
    };
    let socket: PathBuf = match keybase_paths::socket_path() {
        Some(path) => path,
        None => {
            eprintln!("Could not resolve Keybase socket path.");
            std::process::exit(2);
        }
    };
    println!("=== Resolve by convID: {conv_hex} ===");

    let transport = match FramedMsgpackTransport::connect(&socket).await {
        Ok(t) => t,
        Err(err) => {
            eprintln!("Failed to connect: {err}");
            std::process::exit(1);
        }
    };
    let mut client = KeybaseRpcClient::new(transport);

    let all_statuses = Value::Array(vec![
        Value::from(0i64),
        Value::from(1i64),
        Value::from(2i64),
        Value::from(3i64),
        Value::from(4i64),
        Value::from(5i64),
    ]);
    let query = Value::Map(vec![
        (Value::from("topicType"), Value::from(TOPIC_TYPE_CHAT)),
        (Value::from("status"), Value::Array(Vec::new())),
        (Value::from("memberStatus"), all_statuses),
        (
            Value::from("convIDs"),
            Value::Array(vec![Value::Binary(raw_id)]),
        ),
        (Value::from("unreadOnly"), Value::from(false)),
        (Value::from("readOnly"), Value::from(false)),
        (Value::from("computeActiveList"), Value::from(false)),
    ]);

    let params = Value::Map(vec![
        (Value::from("query"), query),
        (
            Value::from("identifyBehavior"),
            Value::from(RESOLVE_IDENTIFY_BEHAVIOR),
        ),
    ]);

    let result = match client
        .call(CHAT_GET_INBOX_AND_UNBOX_LOCAL, vec![params])
        .await
    {
        Ok(value) => value,
        Err(err) => {
            eprintln!("RPC error: {err}");
            std::process::exit(1);
        }
    };

    let conv_array = direct_map_get(&result, &["conversations"])
        .and_then(|v| match v {
            Value::Array(items) => Some(items.clone()),
            _ => None,
        })
        .unwrap_or_default();

    println!("conversations count: {}", conv_array.len());

    for (i, conv) in conv_array.iter().enumerate() {
        let info_val = direct_map_get(conv, &["info", "i", "conv", "conversation"])
            .cloned()
            .unwrap_or(conv.clone());
        let tlf = direct_map_get(&info_val, &["tlfName", "n"])
            .and_then(|v| v.as_str())
            .unwrap_or("<none>");
        let topic = direct_map_get(&info_val, &["topicName", "t"])
            .and_then(|v| v.as_str())
            .unwrap_or("<none>");
        let members_type =
            direct_map_get(&info_val, &["membersType", "m"]).and_then(|v| v.as_i64());
        let reader_info = direct_map_get(conv, &["readerInfo", "ri", "r"])
            .cloned()
            .unwrap_or(Value::Nil);
        let member_status = direct_map_get(&reader_info, &["status", "s"]).and_then(|v| v.as_i64());
        println!(
            "  [{i}] {tlf}#{topic} membersType={members_type:?} member_status={member_status:?}"
        );
    }
}

async fn inspect_find_all_channels(channel_name: &str) {
    let socket: PathBuf = match keybase_paths::socket_path() {
        Some(path) => path,
        None => {
            eprintln!("Could not resolve Keybase socket path.");
            std::process::exit(2);
        }
    };
    println!("=== Find all #{channel_name} channels ===");

    let transport = match FramedMsgpackTransport::connect(&socket).await {
        Ok(t) => t,
        Err(err) => {
            eprintln!("Failed to connect: {err}");
            std::process::exit(1);
        }
    };
    let mut client = KeybaseRpcClient::new(transport);

    // Explicitly include ALL member statuses (0-5) to find NEVER_JOINED channels
    let all_member_statuses = Value::Array(vec![
        Value::from(0i64), // ACTIVE
        Value::from(1i64), // REMOVED
        Value::from(2i64), // LEFT
        Value::from(3i64), // PREVIEW
        Value::from(4i64), // RESET
        Value::from(5i64), // NEVER_JOINED
    ]);
    let query = Value::Map(vec![
        (Value::from("topicType"), Value::from(TOPIC_TYPE_CHAT)),
        (Value::from("status"), Value::Array(Vec::new())),
        (Value::from("memberStatus"), all_member_statuses),
        (
            Value::from("topicName"),
            Value::from(channel_name.to_string()),
        ),
        (Value::from("membersType"), Value::from(TEAM_MEMBERS_TYPE)),
        (Value::from("unreadOnly"), Value::from(false)),
        (Value::from("readOnly"), Value::from(false)),
        (Value::from("computeActiveList"), Value::from(false)),
    ]);

    let params = Value::Map(vec![
        (Value::from("query"), query),
        (
            Value::from("identifyBehavior"),
            Value::from(RESOLVE_IDENTIFY_BEHAVIOR),
        ),
    ]);

    let result = match client
        .call(CHAT_GET_INBOX_AND_UNBOX_LOCAL, vec![params])
        .await
    {
        Ok(value) => value,
        Err(err) => {
            eprintln!("RPC error: {err}");
            std::process::exit(1);
        }
    };

    let conv_array = direct_map_get(&result, &["conversations"])
        .and_then(|v| match v {
            Value::Array(items) => Some(items.clone()),
            _ => None,
        })
        .unwrap_or_default();

    println!("Total channels named #{channel_name}: {}", conv_array.len());

    for (i, conv) in conv_array.iter().enumerate() {
        let info_val = direct_map_get(conv, &["info", "i", "conv", "conversation"])
            .cloned()
            .unwrap_or(conv.clone());
        let tlf = direct_map_get(&info_val, &["tlfName", "n"])
            .and_then(|v| v.as_str())
            .unwrap_or("<none>");
        let topic = direct_map_get(&info_val, &["topicName", "t"])
            .and_then(|v| v.as_str())
            .unwrap_or("<none>");
        let conv_id_hex = direct_map_get(&info_val, &["id", "i"])
            .and_then(|v| match v {
                Value::Binary(bytes) => {
                    let mut s = String::with_capacity(bytes.len() * 2);
                    for b in bytes {
                        s.push_str(&format!("{:02x}", b));
                    }
                    Some(s)
                }
                _ => None,
            })
            .unwrap_or_else(|| "<none>".to_string());
        let reader_info = direct_map_get(conv, &["readerInfo", "ri", "r"])
            .cloned()
            .unwrap_or(Value::Nil);
        let member_status = direct_map_get(&reader_info, &["status", "s"]).and_then(|v| v.as_i64());
        println!("  [{i}] {tlf}#{topic} convID={conv_id_hex} member_status={member_status:?}");
    }
}

fn main() {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("Failed to initialize Tokio runtime: {err}");
            std::process::exit(1);
        }
    };

    runtime.block_on(async_main());
}

async fn async_main() {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() {
        usage_and_exit();
    }

    if args[0] == "--user-search" {
        let Some(query) = args
            .get(1)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
        else {
            usage_and_exit();
        };
        inspect_user_search(query).await;
        return;
    }

    if args[0] == "--resolve" {
        let team = args.get(1).map(|v| v.trim()).filter(|v| !v.is_empty());
        let channel = args.get(2).map(|v| v.trim()).filter(|v| !v.is_empty());
        match (team, channel) {
            (Some(team), Some(channel)) => {
                inspect_resolve_channel(team, channel).await;
            }
            _ => {
                eprintln!("Usage: --resolve <team_name> <channel_name>");
                std::process::exit(2);
            }
        }
        return;
    }

    if args[0] == "--resolve-id" {
        let conv_hex = args.get(1).map(|v| v.trim()).filter(|v| !v.is_empty());
        let Some(conv_hex) = conv_hex else {
            eprintln!("Usage: --resolve-id <conversation_id_hex>");
            std::process::exit(2);
        };
        inspect_resolve_by_id(conv_hex).await;
        return;
    }

    if args[0] == "--test-join" || args[0] == "--test-leave" {
        let is_join = args[0] == "--test-join";
        let conv_hex = args.get(1).map(|v| v.trim()).filter(|v| !v.is_empty());
        let Some(conv_hex) = conv_hex else {
            eprintln!("Usage: {} <conversation_id_hex>", args[0]);
            std::process::exit(2);
        };
        inspect_test_join_leave(conv_hex, is_join).await;
        return;
    }

    if args[0] == "--find-all" {
        let channel = args.get(1).map(|v| v.trim()).filter(|v| !v.is_empty());
        let Some(channel) = channel else {
            eprintln!("Usage: --find-all <channel_name>");
            std::process::exit(2);
        };
        inspect_find_all_channels(channel).await;
        return;
    }

    if args[0] == "--listen" {
        let mut seconds: u64 = 10;
        let mut max: usize = 200;
        let mut i = 1usize;
        while i < args.len() {
            match args[i].as_str() {
                "--seconds" => {
                    if let Some(v) = args.get(i + 1).and_then(|s| s.parse::<u64>().ok()) {
                        seconds = v;
                    }
                    i += 2;
                }
                "--max" => {
                    if let Some(v) = args.get(i + 1).and_then(|s| s.parse::<usize>().ok()) {
                        max = v;
                    }
                    i += 2;
                }
                _ => i += 1,
            }
        }

        listen_for_new_chat_activity(max, seconds).await;
        return;
    }

    let conversation_hex = strip_conv_prefix(&args[0]);
    let target_message_id: i64 = args.get(1).and_then(|s| s.parse::<i64>().ok()).unwrap_or(1);

    let raw_conversation_id = match hex_decode_bytes(conversation_hex) {
        Ok(bytes) => bytes,
        Err(err) => {
            eprintln!("Invalid conversation id hex: {err}");
            std::process::exit(2);
        }
    };

    let socket: PathBuf = match keybase_paths::socket_path() {
        Some(path) => path,
        None => {
            eprintln!(
                "Could not resolve Keybase socket path (set KEYBASE_SOCKET_PATH to override)."
            );
            std::process::exit(2);
        }
    };

    let transport = match FramedMsgpackTransport::connect(&socket).await {
        Ok(t) => t,
        Err(err) => {
            eprintln!("Failed to connect to Keybase socket at {socket:?}: {err}");
            std::process::exit(1);
        }
    };
    let mut client = KeybaseRpcClient::new(transport);

    // Query around the target message id (try NEWER and OLDER).
    for (label, mode) in [
        ("newer", MESSAGE_ID_CONTROL_MODE_NEWER),
        ("older", MESSAGE_ID_CONTROL_MODE_OLDER),
    ] {
        let query = Value::Map(vec![(
            Value::from("messageIDControl"),
            Value::Map(vec![
                (Value::from("pivot"), Value::from(target_message_id)),
                (Value::from("mode"), Value::from(mode)),
                (Value::from("num"), Value::from(50i64)),
            ]),
        )]);

        let params = Value::Map(vec![
            (
                Value::from("conversationID"),
                Value::Binary(raw_conversation_id.clone()),
            ),
            (
                Value::from("reason"),
                Value::from(GET_THREAD_REASON_FOREGROUND),
            ),
            (Value::from("query"), query),
            (Value::from("pagination"), Value::Nil),
            (
                Value::from("identifyBehavior"),
                Value::from(IDENTIFY_BEHAVIOR_CHAT_GUI),
            ),
        ]);

        let result = match client
            .call("chat.1.local.getThreadLocal", vec![params])
            .await
        {
            Ok(value) => value,
            Err(err) => {
                eprintln!("RPC error calling getThreadLocal ({label}): {err}");
                continue;
            }
        };

        println!("\n=== getThreadLocal ({label}) ===");
        let Some(messages) = find_thread_messages_root(&result) else {
            println!(
                "No messages field found. Top-level keys: {:?}",
                value_key_preview(&result)
            );
            continue;
        };

        let Value::Array(entries) = messages else {
            println!(
                "messages is not an array. messages keys: {:?}",
                value_key_preview(messages)
            );
            continue;
        };

        let mut found_any = false;
        for entry in entries {
            let id = extract_message_id_candidate(entry);
            if id != Some(target_message_id) {
                continue;
            }
            found_any = true;
            let body = extract_message_body_candidate(entry);
            println!(
                "Found message id={target_message_id}. Entry keys: {:?}",
                value_key_preview(entry)
            );
            match body {
                Some(body) => {
                    println!("messageBody keys: {:?}", value_key_preview(body));
                    println!("messageBody: {body:?}");
                }
                None => {
                    println!("No messageBody field found for this entry.");
                }
            }
        }

        if !found_any {
            let ids = entries
                .iter()
                .filter_map(extract_message_id_candidate)
                .take(20)
                .collect::<Vec<_>>();
            println!(
                "Did not find id={target_message_id} in returned page. First ids (up to 20): {ids:?}"
            );
        }
    }
}

async fn inspect_test_join_leave(conv_hex: &str, is_join: bool) {
    let hex = strip_conv_prefix(conv_hex);
    let raw_id = match hex_decode_bytes(hex) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Bad hex: {e}");
            return;
        }
    };
    let socket: PathBuf = match keybase_paths::socket_path() {
        Some(path) => path,
        None => {
            eprintln!("No Keybase socket found");
            return;
        }
    };
    let action = if is_join { "Join" } else { "Leave" };
    println!("=== Test {action}: conv_id={hex} ===");
    println!("socket: {socket:?}");
    let transport = match FramedMsgpackTransport::connect(&socket).await {
        Ok(t) => t,
        Err(e) => {
            eprintln!("Connect error: {e}");
            return;
        }
    };
    let mut client = KeybaseRpcClient::new(transport);
    let method = if is_join {
        "chat.1.local.joinConversationByIDLocal"
    } else {
        "chat.1.local.leaveConversationLocal"
    };
    println!("Calling {method}...");
    let result = client
        .call(
            method,
            vec![Value::Map(vec![
                (Value::from("convID"), Value::Binary(raw_id.to_vec())),
                (
                    Value::from("identifyBehavior"),
                    Value::from(IDENTIFY_BEHAVIOR_CHAT_GUI),
                ),
            ])],
        )
        .await;
    match result {
        Ok(response) => {
            println!("Success! Response keys: {:?}", value_key_preview(&response));
            println!("Response: {response:?}");
        }
        Err(e) => {
            println!("ERROR: {e}");
        }
    }
}
