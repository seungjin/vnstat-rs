use crate::models::InterfaceStats;
use anyhow::Result;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

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

        // Detect granular interface type
        let interface_type =
            if Path::new(&format!("/sys/class/net/{}/device", name)).exists() {
                // Physical
                if Path::new(&format!("/sys/class/net/{}/wireless", name))
                    .exists()
                    || Path::new(&format!("/sys/class/net/{}/phy80211", name))
                        .exists()
                {
                    Some(1) // wireless
                } else if Path::new(&format!("/sys/class/net/{}/wwan", name))
                    .exists()
                {
                    Some(2) // mobile
                } else {
                    Some(0) // ethernet (default physical)
                }
            } else {
                // Virtual
                if name == "lo" {
                    Some(101) // loopback
                } else if Path::new(&format!("/sys/class/net/{}/bridge", name))
                    .exists()
                    || name.starts_with("br-")
                {
                    Some(102) // bridge
                } else if Path::new(&format!(
                    "/sys/class/net/{}/tun_flags",
                    name
                ))
                .exists()
                    || name.starts_with("tun")
                    || name.starts_with("tap")
                    || name.starts_with("wg")
                {
                    Some(105) // vpn/tun
                } else if name.starts_with("veth") || name.starts_with("docker")
                {
                    Some(106) // veth/docker
                } else {
                    Some(100) // generic virtual
                }
            };

        // Find the MAC address for this interface
        let mac_address = ifaces
            .iter()
            .find(|i| i.name == name)
            .and_then(|i| i.mac)
            .map(|m| m.to_string());

        stats.push(InterfaceStats {
            name,
            alias: None,
            interface_type,
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

pub fn get_load_average() -> Result<(f64, f64, f64)> {
    let content = fs::read_to_string("/proc/loadavg")?;
    let parts: Vec<&str> = content.split_whitespace().collect();
    if parts.len() < 3 {
        return Err(anyhow::anyhow!("Invalid /proc/loadavg format"));
    }
    let one = parts[0].parse::<f64>()?;
    let five = parts[1].parse::<f64>()?;
    let fifteen = parts[2].parse::<f64>()?;
    Ok((one, five, fifteen))
}

pub fn get_num_cores() -> usize {
    unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) as usize }
}
