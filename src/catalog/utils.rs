use chrono::{DateTime, Utc};

pub fn format_timestamptz(ts: std::time::SystemTime) -> String {
    let dt: DateTime<Utc> = ts.into();
    dt.format("%Y-%m-%d %H:%M:%S%.6f+00").to_string()
}
