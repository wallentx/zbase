use std::{env, path::PathBuf};

use rmpv::Value;
use tokio::{sync::mpsc, time};

#[path = "../services/backends/keybase/rpc/client.rs"]
mod client;
#[path = "../services/backends/keybase/paths.rs"]
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
        "Usage:\n  Inspect a thread message:\n    cargo run --bin keybase_rpc_inspect -- <conversation_id_hex|kb_conv:...> [message_id]\n\n  Listen for NotifyChat.NewChatActivity frames:\n    cargo run --bin keybase_rpc_inspect -- --listen [--seconds N] [--max N]\n\n  Probe user search RPC:\n    cargo run --bin keybase_rpc_inspect -- --user-search <query>\n\nExamples:\n  cargo run --bin keybase_rpc_inspect -- kb_conv:0000722768... 1\n  cargo run --bin keybase_rpc_inspect -- --listen --seconds 10 --max 200\n  cargo run --bin keybase_rpc_inspect -- --user-search cameron\n"
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
