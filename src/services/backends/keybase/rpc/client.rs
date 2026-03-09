use std::{collections::BTreeMap, io};

use rmpv::Value;
use tokio::sync::mpsc;

use super::transport::FramedMsgpackTransport;

const NOTIFY_CTL_SET_NOTIFICATIONS: &str = "keybase.1.notifyCtl.setNotifications";

#[derive(Clone, Debug)]
pub struct RpcNotification {
    pub method: String,
    pub params: Value,
}

#[derive(Debug, Default, Clone)]
pub struct NotificationChannels {
    values: BTreeMap<&'static str, bool>,
}

impl NotificationChannels {
    pub fn all_enabled() -> Self {
        let mut values = BTreeMap::new();
        for key in [
            "session",
            "users",
            "kbfs",
            "kbfsdesktop",
            "kbfslegacy",
            "kbfssubscription",
            "notifysimplefs",
            "tracking",
            "favorites",
            "paperkeys",
            "keyfamily",
            "service",
            "app",
            "chat",
            "pgp",
            "kbfsrequest",
            "badges",
            "reachability",
            "team",
            "ephemeral",
            "teambot",
            "chatkbfsedits",
            "chatdev",
            "chatemoji",
            "chatemojicross",
            "deviceclone",
            "chatattachments",
            "wallet",
            "audit",
            "runtimestats",
            "featuredBots",
            "saltpack",
            // Keep false while bringing up event plumbing so we do not suppress
            // chat notifications the daemon decides are skippable for GUI clients.
            // We can revisit this later for perf tuning once UI mappings are in place.
            "chatarchive",
        ] {
            values.insert(key, true);
        }
        values.insert("allowChatNotifySkips", false);
        Self { values }
    }

    fn to_value(&self) -> Value {
        Value::Map(
            self.values
                .iter()
                .map(|(key, value)| (Value::from(*key), Value::from(*value)))
                .collect(),
        )
    }
}

pub struct KeybaseRpcClient {
    transport: FramedMsgpackTransport,
    next_msg_id: i64,
}

impl KeybaseRpcClient {
    pub fn new(transport: FramedMsgpackTransport) -> Self {
        Self {
            transport,
            next_msg_id: 1,
        }
    }

    pub async fn set_notifications(&mut self, channels: NotificationChannels) -> io::Result<()> {
        let params = Value::Map(vec![(Value::from("channels"), channels.to_value())]);
        self.call(NOTIFY_CTL_SET_NOTIFICATIONS, vec![params])
            .await
            .map(|_| ())
    }

    pub async fn call(&mut self, method: &str, args: Vec<Value>) -> io::Result<Value> {
        let msgid = self.next_message_id();
        let payload = Value::Array(vec![
            Value::from(0), // request
            Value::from(msgid),
            Value::from(method),
            Value::Array(args),
        ]);
        self.transport.write_value(&payload).await?;
        self.wait_for_response(msgid).await
    }

    pub async fn run_notification_loop(
        mut self,
        sender: mpsc::Sender<RpcNotification>,
    ) -> io::Result<()> {
        loop {
            let message = self.transport.read_value().await?;
            if let Some(notification) = parse_notification_message(message.clone()) {
                if sender.send(notification).await.is_err() {
                    return Ok(());
                }
                continue;
            }

            if let Some((msgid, notification)) = parse_request_message(message.clone()) {
                if sender.send(notification).await.is_err() {
                    return Ok(());
                }

                // Some Keybase notify protocols are emitted as full RPC calls that expect
                // a response. Acking them keeps the daemon->client stream unblocked.
                let response = Value::Array(vec![
                    Value::from(1), // response
                    Value::from(msgid),
                    Value::Nil, // error
                    Value::Nil, // result
                ]);
                self.transport.write_value(&response).await?;
                continue;
            }

            // Keep visibility into wire-level packets we are not yet decoding.
            if sender
                .send(RpcNotification {
                    method: "kbui.internal.unparsed_rpc_frame".to_string(),
                    params: message,
                })
                .await
                .is_err()
            {
                return Ok(());
            }
        }
    }

    async fn wait_for_response(&mut self, expected_msgid: i64) -> io::Result<Value> {
        loop {
            let message = self.transport.read_value().await?;
            match parse_response_message(message.clone()) {
                Some((msgid, error, result)) if msgid == expected_msgid => {
                    if let Some(error_value) = error {
                        return Err(io::Error::other(format!("rpc error: {error_value:?}")));
                    }
                    return Ok(result.unwrap_or(Value::Nil));
                }
                _ => {}
            }

            if let Some((msgid, _)) = parse_request_message(message.clone()) {
                let response = Value::Array(vec![
                    Value::from(1), // response
                    Value::from(msgid),
                    Value::Nil, // error
                    Value::Nil, // result
                ]);
                self.transport.write_value(&response).await?;
            }
        }
    }

    fn next_message_id(&mut self) -> i64 {
        let value = self.next_msg_id;
        self.next_msg_id += 1;
        value
    }
}

fn parse_notification_message(value: Value) -> Option<RpcNotification> {
    let Value::Array(parts) = value else {
        return None;
    };
    if parts.len() != 3 {
        return None;
    }
    if parts[0].as_i64()? != 2 {
        return None;
    }
    let method = parts[1].as_str()?.to_string();
    let params = parts[2].clone();
    Some(RpcNotification { method, params })
}

fn parse_response_message(value: Value) -> Option<(i64, Option<Value>, Option<Value>)> {
    let Value::Array(parts) = value else {
        return None;
    };
    if parts.len() != 4 {
        return None;
    }
    if parts[0].as_i64()? != 1 {
        return None;
    }
    let msgid = parts[1].as_i64()?;
    let error = if parts[2].is_nil() {
        None
    } else {
        Some(parts[2].clone())
    };
    let result = if parts[3].is_nil() {
        None
    } else {
        Some(parts[3].clone())
    };
    Some((msgid, error, result))
}

fn parse_request_message(value: Value) -> Option<(i64, RpcNotification)> {
    let Value::Array(parts) = value else {
        return None;
    };
    if parts.len() != 4 {
        return None;
    }
    if parts[0].as_i64()? != 0 {
        return None;
    }
    let msgid = parts[1].as_i64()?;
    let method = parts[2].as_str()?.to_string();
    let params = parts[3].clone();
    Some((msgid, RpcNotification { method, params }))
}
