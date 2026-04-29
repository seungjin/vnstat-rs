use anyhow::{Result};
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::models::InterfaceStats;

pub fn get_machine_id() -> Result<String> {
    if let Ok(id) = fs::read_to_string("/etc/machine-id") {
        return Ok(id.trim().to_string());
    }
    if let Ok(id) = fs::read_to_string("/var/lib/dbus/machine-id") {
        return Ok(id.trim().to_string());
    }
    Err(anyhow::anyhow!("Failed to read machine-id"))
}

pub fn parse_net_dev() -> Result<Vec<InterfaceStats>> {
    let content = fs::read_to_string("/proc/net/dev")?;
    let hostname = hostname::get()?.to_string_lossy().to_string();
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
    let mut stats = Vec::new();

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || !line.contains(':') {
            continue;
        }

        let parts: Vec<&str> = line.splitn(2, ':').collect();
        if parts.len() < 2 {
            continue;
        }

        let name = parts[0].trim().to_string();
        let data_parts: Vec<&str> = parts[1].split_whitespace().collect();
        
        if data_parts.len() < 10 {
            continue;
        }

        let rx_bytes = data_parts[0].parse::<u64>().unwrap_or(0);
        let rx_packets = data_parts[1].parse::<u64>().unwrap_or(0);
        let tx_bytes = data_parts[8].parse::<u64>().unwrap_or(0);
        let tx_packets = data_parts[9].parse::<u64>().unwrap_or(0);

        stats.push(InterfaceStats {
            name,
            alias: None,
            rx_bytes,
            tx_bytes,
            rx_packets,
            tx_packets,
            hostname: hostname.clone(),
            created: now,
            updated: now,
        });
    }

    Ok(stats)
}

pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.2} TiB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GiB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MiB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KiB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.00 KiB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MiB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GiB");
        assert_eq!(format_bytes(1024 * 1024 * 1024 * 1024), "1.00 TiB");
    }

    #[test]
    fn test_time_truncation() {
        // 2024-05-15 12:34:56 UTC
        let now = 1715776496; 
        let dt = chrono::DateTime::from_timestamp(now, 0).unwrap();
        let naive = dt.naive_utc();
        
        // day: 2024-05-15 00:00:00
        let day_dt = naive.date().and_hms_opt(0, 0, 0).unwrap();
        let day = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(day_dt, chrono::Utc).timestamp();
        assert_eq!(day, 1715731200);

        // month: 2024-05-01 00:00:00
        let month_dt = naive.date().with_day(1).unwrap().and_hms_opt(0, 0, 0).unwrap();
        let month = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(month_dt, chrono::Utc).timestamp();
        assert_eq!(month, 1714521600);

        // year: 2024-01-01 00:00:00
        let year_dt = naive.date().with_day(1).unwrap().with_month(1).unwrap().and_hms_opt(0, 0, 0).unwrap();
        let year = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(year_dt, chrono::Utc).timestamp();
        assert_eq!(year, 1704067200);
    }
}
