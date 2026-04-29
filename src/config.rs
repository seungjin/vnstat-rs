use std::path::{Path, PathBuf};
use std::fs;

#[derive(Default, Debug)]
pub struct Config {
    pub database: Option<PathBuf>,
    pub url: Option<String>,
    pub token: Option<String>,
    pub update_interval: u64,
    pub sync_interval: u64,
    pub daemon_socket: Option<PathBuf>,
}

pub fn load_config(path: &Path) -> Result<Config, std::io::Error> {
    let mut config = Config {
        update_interval: 30,
        sync_interval: 300,
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
            "DatabaseDir" => database_dir = Some(PathBuf::from(value)),
            "Database" => database_file = Some(value.to_string()),
            "TursoUrl" => config.url = Some(value.to_string()),
            "TursoToken" => config.token = Some(value.to_string()),
            "UpdateInterval" => {
                if let Ok(v) = value.parse() { config.update_interval = v; }
            }
            "SyncInterval" => {
                if let Ok(v) = value.parse() { config.sync_interval = v; }
            }
            "DaemonSocket" => config.daemon_socket = Some(PathBuf::from(value)),
            _ => {}
        }
    }

    // Combine DatabaseDir and Database if both are present, or use what's available
    config.database = match (database_dir, database_file) {
        (Some(dir), Some(file)) => Some(dir.join(file)),
        (None, Some(file)) => Some(PathBuf::from(file)),
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
