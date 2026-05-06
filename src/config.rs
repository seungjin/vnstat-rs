use std::path::{Path, PathBuf};
use std::fs;
use crate::utils::expand_tilde;

#[derive(Default, Debug)]
pub struct Config {
    pub database: Option<PathBuf>,
    pub url: Option<String>,
    pub token: Option<String>,
    pub update_interval: u64,
    pub sync_interval: u64,
    pub five_minute_hours: u32,
    pub hourly_days: u32,
    pub daily_days: u32,
    pub monthly_months: u32,
    pub yearly_years: i32,
    pub top_day_entries: u32,
    pub daemon_socket: Option<PathBuf>,
    pub hostname_override: Option<String>,
    pub max_bandwidth: u64,
}

pub fn load_config(path: &Path) -> Result<Config, std::io::Error> {
    println!("Loading config from {:?}...", path);
    let mut config = Config {
        update_interval: 30,
        sync_interval: 300,
        five_minute_hours: 48,
        hourly_days: 4,
        daily_days: 62,
        monthly_months: 25,
        yearly_years: -1,
        top_day_entries: 20,
        max_bandwidth: 1000,
        ..Default::default()
    };

    let is_root = unsafe { libc::getuid() == 0 };

    let content = fs::read_to_string(path)?;
    let mut database_dir: Option<PathBuf> = None;
    let mut database_file: Option<String> = None;

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }

        let parts: Vec<&str> = line.splitn(2, |c: char| c.is_whitespace()).map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
        if parts.len() < 2 {
            continue;
        }

        let key = parts[0];
        let value = parts[1].trim_matches('"');

        match key {
            "DatabaseDir" => database_dir = Some(expand_tilde(value)),
            "Database" => database_file = Some(value.to_string()),
            "TursoUrl" | "LibsqlUrl" => config.url = Some(value.to_string()),
            "TursoToken" | "LibsqlToken" => config.token = Some(value.to_string()),
            "UpdateInterval" => {
                if let Ok(v) = value.parse() { config.update_interval = v; }
            }
            "SyncInterval" => {
                if let Ok(v) = value.parse() { config.sync_interval = v; }
            }
            "5MinuteHours" => {
                if let Ok(v) = value.parse() { config.five_minute_hours = v; }
            }
            "HourlyDays" => {
                if let Ok(v) = value.parse() { config.hourly_days = v; }
            }
            "DailyDays" => {
                if let Ok(v) = value.parse() { config.daily_days = v; }
            }
            "MonthlyMonths" => {
                if let Ok(v) = value.parse() { config.monthly_months = v; }
            }
            "YearlyYears" => {
                if let Ok(v) = value.parse() { config.yearly_years = v; }
            }
            "TopDayEntries" => {
                if let Ok(v) = value.parse() { config.top_day_entries = v; }
            }
            "MaxBandwidth" => {
                if let Ok(v) = value.parse() { config.max_bandwidth = v; }
            }
            "DaemonSocket" => config.daemon_socket = Some(expand_tilde(value)),
            "Hostname" => config.hostname_override = Some(value.to_string()),
            _ => {}
        }
    }

    // Combine DatabaseDir and Database if both are present, or use what's available
    config.database = match (database_dir, database_file) {
        (Some(dir), Some(file)) => Some(dir.join(file)),
        (None, Some(file)) => Some(expand_tilde(file)),
        (Some(dir), None) => Some(dir.join("vnstat-rs.db")),
        (None, None) => {
            if is_root {
                Some(PathBuf::from("/var/lib/vnstat-rs/vnstat-rs.db"))
            } else {
                let home = std::env::var("HOME").unwrap_or_default();
                Some(PathBuf::from(home).join(".local/share/vnstat-rs/vnstat-rs.db"))
            }
        }
    };

    if config.daemon_socket.is_none() {
        if is_root {
            config.daemon_socket = Some(PathBuf::from("/var/run/vnstat-rs.sock"));
        } else {
            let home = std::env::var("HOME").unwrap_or_default();
            config.daemon_socket = Some(PathBuf::from(home).join(".local/share/vnstat-rs/vnstat-rs.sock"));
        }
    }

    Ok(config)
}

pub fn get_default_config(is_root: bool) -> Config {
    let mut config = Config {
        update_interval: 30,
        sync_interval: 300,
        five_minute_hours: 48,
        hourly_days: 4,
        daily_days: 62,
        monthly_months: 25,
        yearly_years: -1,
        top_day_entries: 20,
        max_bandwidth: 1000,
        ..Default::default()
    };

    if is_root {
        config.database = Some(PathBuf::from("/var/lib/vnstat-rs/vnstat-rs.db"));
        config.daemon_socket = Some(PathBuf::from("/var/run/vnstat-rs.sock"));
    } else {
        let home = std::env::var("HOME").unwrap_or_default();
        config.database = Some(PathBuf::from(home.clone()).join(".local/share/vnstat-rs/vnstat-rs.db"));
        config.daemon_socket = Some(PathBuf::from(home).join(".local/share/vnstat-rs/vnstat-rs.sock"));
    }

    config
}

pub fn load_best_config() -> Config {
    let is_root = unsafe { libc::getuid() == 0 };
    let etc_config = PathBuf::from("/etc/vnstat-rs/vnstat-rs.conf");
    let home = std::env::var("HOME").unwrap_or_default();
    let user_config = PathBuf::from(home).join(".config/vnstat-rs/vnstat-rs.conf");
    let local_config = PathBuf::from("vnstat-rs.conf");

    // 1. Try /etc/vnstat-rs/vnstat-rs.conf
    if etc_config.exists() {
        if let Ok(c) = load_config(&etc_config) {
            return c;
        }
    }

    // 2. Try ~/.config/vnstat-rs/vnstat-rs.conf
    if user_config.exists() {
        if let Ok(c) = load_config(&user_config) {
            return c;
        }
    }

    // 3. Try current directory (convenient for testing)
    if local_config.exists() {
        if let Ok(c) = load_config(&local_config) {
            return c;
        }
    }

    println!("No config file found. Using defaults.");
    get_default_config(is_root)
}
