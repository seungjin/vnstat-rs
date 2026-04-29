use anyhow::{Result};
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::models::InterfaceStats;
use std::path::{Path, PathBuf};

pub fn expand_tilde<P: AsRef<Path>>(path: P) -> PathBuf {
    let p = path.as_ref();
    if !p.starts_with("~") {
        return p.to_path_buf();
    }
    if let Ok(home) = std::env::var("HOME") {
        if p == Path::new("~") {
            return PathBuf::from(home);
        }
        if let Ok(suffix) = p.strip_prefix("~") {
            return PathBuf::from(home).join(suffix);
        }
    }
    p.to_path_buf()
}

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

    // Get MAC addresses for all interfaces using pnet
    let ifaces = pnet_datalink::interfaces();

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

        // Find the MAC address for this interface
        let mac_address = ifaces.iter()
            .find(|i| i.name == name)
            .and_then(|i| i.mac)
            .map(|m| m.to_string());

        stats.push(InterfaceStats {
            name,
            alias: None,
            mac_address,
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
