use chrono::{DateTime, Datelike, Duration, Local};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn title_case(input: &str) -> String {
    let mut chars = input.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

/// Formats a message timestamp following the Keybase client convention:
///   - Today:            "4:34 PM"
///   - Yesterday:        "4:34 PM \u{2022} Yesterday"
///   - Within 7 days:    "4:34 PM \u{2022} Wed"
///   - Within 30 days:   "4:34 PM \u{2022} 5 Jan"
///   - Older:            "4:34 PM \u{2022} 5 Jan 24"
///
/// All spaces are replaced with non-breaking spaces (\u{00A0}) to prevent
/// line breaks inside the timestamp.
pub fn message_timestamp_label(timestamp_ms: Option<i64>) -> String {
    let Some(timestamp_ms) = timestamp_ms else {
        return "Now".to_string();
    };

    let Some(dt) = DateTime::from_timestamp_millis(timestamp_ms) else {
        return "Now".to_string();
    };
    let dt: DateTime<Local> = dt.with_timezone(&Local);
    let now = Local::now();

    let time_str = dt.format("%-I:%M %p").to_string();

    let label = if is_same_day(&dt, &now) {
        time_str
    } else if is_yesterday(&dt, &now) {
        format!("{time_str} \u{2022} Yesterday")
    } else if now.signed_duration_since(dt) < Duration::days(7) && dt < now {
        let day = dt.format("%a");
        format!("{time_str} \u{2022} {day}")
    } else if now.signed_duration_since(dt) < Duration::days(30) && dt < now {
        let date = dt.format("%-d %b");
        format!("{time_str} \u{2022} {date}")
    } else {
        let date = dt.format("%-d %b %y");
        format!("{time_str} \u{2022} {date}")
    };

    label.replace(' ', "\u{00A0}")
}

fn is_same_day(a: &DateTime<Local>, b: &DateTime<Local>) -> bool {
    a.year() == b.year() && a.ordinal() == b.ordinal()
}

fn is_yesterday(dt: &DateTime<Local>, now: &DateTime<Local>) -> bool {
    let yesterday = *now - Duration::days(1);
    is_same_day(dt, &yesterday)
}

pub fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}
