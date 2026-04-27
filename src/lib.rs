use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub enum DbType {
    Local(turso::Database),
    Sync(turso::sync::Database),
}

pub struct Db {
    pub db: DbType,
    pub conn: turso::Connection,
    pub hostname: String,
    pub machine_id: String,
}

#[derive(Debug)]
pub struct InterfaceStats {
    pub name: String,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
}

#[derive(Default, Debug)]
pub struct Config {
    pub database: Option<PathBuf>,
    pub url: Option<String>,
    pub token: Option<String>,
    pub update_interval: u64,
    pub sync_interval: u64,
}

impl Db {
    pub async fn open(path: PathBuf, url: Option<String>, token: Option<String>) -> Result<Self> {
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                println!("Creating database directory {}...", parent.display());
                fs::create_dir_all(parent).context("Failed to create database directory")?;
            }
        }

        let path_str = path.to_string_lossy().to_string();
        
        let (db_type, conn) = if let (Some(url), Some(token)) = (url, token) {
            println!("Opening remote replica at {}...", url);
            let db = turso::sync::Builder::new_remote(&path_str)
                .with_remote_url(url)
                .with_auth_token(token)
                .build()
                .await?;
            println!("Initial sync (pull)...");
            if let Err(e) = db.pull().await {
                eprintln!("Warning: Initial pull failed: {}", e);
            }
            let conn = db.connect().await?;
            (DbType::Sync(db), conn)
        } else {
            let db = turso::Builder::new_local(&path_str).build().await?;
            let conn = db.connect()?;
            (DbType::Local(db), conn)
        };

        let hostname = hostname::get()?.to_string_lossy().to_string();
        let machine_id = get_machine_id()?;

        Ok(Self { db: db_type, conn, hostname, machine_id })
    }

    pub async fn sync(&self) -> Result<()> {
        if let DbType::Sync(ref db) = self.db {
            println!("Syncing with remote (pull & push)...");
            db.pull().await?;
            db.push().await?;
            println!("Sync complete.");
        } else {
            println!("Skipping sync: No remote database configured.");
        }
        Ok(())
    }

    pub async fn init_schema(&self) -> Result<()> {
        // Info table
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS info (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                value TEXT NOT NULL
            )",
            (),
        ).await?;

        // Set version if not exists
        self.conn.execute(
            "INSERT OR IGNORE INTO info (name, value) VALUES ('version', '2')",
            (),
        ).await?;

        // Interface table (vnStat 2.x compatible + machine_id/hostname)
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS interface (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                name         TEXT NOT NULL,
                alias        TEXT,
                active       INTEGER NOT NULL DEFAULT 1,
                created      INTEGER NOT NULL,
                updated      INTEGER NOT NULL,
                rxcounter    INTEGER NOT NULL DEFAULT 0,
                txcounter    INTEGER NOT NULL DEFAULT 0,
                rxtotal      INTEGER NOT NULL DEFAULT 0,
                txtotal      INTEGER NOT NULL DEFAULT 0,
                machine_id   TEXT NOT NULL,
                hostname     TEXT NOT NULL,
                UNIQUE(machine_id, name)
            )",
            (),
        ).await?;

        // Resolution tables
        let tables = ["fiveminute", "hour", "day", "month", "year", "top"];
        for table in tables {
            self.conn.execute(
                &format!(
                    "CREATE TABLE IF NOT EXISTS {} (
                        id           INTEGER PRIMARY KEY AUTOINCREMENT,
                        interface    INTEGER REFERENCES interface(id) ON DELETE CASCADE,
                        date         INTEGER NOT NULL,
                        rx           INTEGER NOT NULL,
                        tx           INTEGER NOT NULL,
                        CONSTRAINT u UNIQUE (interface, date)
                    )",
                    table
                ),
                (),
            ).await?;
        }

        Ok(())
    }

    pub async fn get_interface(&self, name: &str) -> Result<Option<(i64, u64, u64)>> {
        let mut rows = self.conn.query(
            "SELECT id, rxcounter, txcounter FROM interface WHERE machine_id = ? AND name = ?", 
            (self.machine_id.clone(), name)
        ).await?;
        
        if let Some(row) = rows.next().await? {
            return Ok(Some((row.get(0)?, row.get::<i64>(1)? as u64, row.get::<i64>(2)? as u64)));
        }
        Ok(None)
    }

    pub async fn create_interface(&self, name: &str, rx: u64, tx: u64) -> Result<i64> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        self.conn.execute(
            "INSERT INTO interface (name, created, updated, rxcounter, txcounter, machine_id, hostname) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (name, now, now, rx as i64, tx as i64, self.machine_id.clone(), self.hostname.clone()),
        ).await?;

        let mut rows = self.conn.query(
            "SELECT id FROM interface WHERE machine_id = ? AND name = ?", 
            (self.machine_id.clone(), name)
        ).await?;
        
        if let Some(row) = rows.next().await? {
            return Ok(row.get(0)?);
        }
        Err(anyhow::anyhow!("Failed to create interface"))
    }

    pub async fn update_interface_counters(&self, id: i64, rx: u64, tx: u64, rx_delta: u64, tx_delta: u64) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        self.conn.execute(
            "UPDATE interface SET updated = ?, rxcounter = ?, txcounter = ?, rxtotal = rxtotal + ?, txtotal = txtotal + ? WHERE id = ?",
            (now, rx as i64, tx as i64, rx_delta as i64, tx_delta as i64, id),
        ).await?;
        Ok(())
    }

    pub async fn add_traffic(&self, interface_id: i64, table: &str, date: i64, rx: u64, tx: u64) -> Result<()> {
        self.conn.execute(
            &format!(
                "INSERT INTO {} (interface, date, rx, tx) VALUES (?, ?, ?, ?)
                 ON CONFLICT(interface, date) DO UPDATE SET rx = rx + excluded.rx, tx = tx + excluded.tx",
                table
            ),
            (interface_id, date, rx as i64, tx as i64),
        ).await?;
        Ok(())
    }

    pub async fn update_stats(&self, filter_iface: Option<&str>) -> Result<()> {
        let stats = parse_net_dev()?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;

        for stat in stats {
            if let Some(f) = filter_iface {
                if stat.name != f {
                    continue;
                }
            }
            if let Some((id, last_rx, last_tx)) = self.get_interface(&stat.name).await? {
                let rx_delta = if stat.rx_bytes >= last_rx {
                    stat.rx_bytes - last_rx
                } else {
                    stat.rx_bytes // reset
                };
                let tx_delta = if stat.tx_bytes >= last_tx {
                    stat.tx_bytes - last_tx
                } else {
                    stat.tx_bytes // reset
                };

                if rx_delta > 0 || tx_delta > 0 {
                    // Update interface counters and totals
                    self.update_interface_counters(id, stat.rx_bytes, stat.tx_bytes, rx_delta, tx_delta).await?;

                    // Aggregate into resolution tables
                    // fiveminute
                    let five_min = (now / 300) * 300;
                    self.add_traffic(id, "fiveminute", five_min, rx_delta, tx_delta).await?;

                    // hour
                    let hour = (now / 3600) * 3600;
                    self.add_traffic(id, "hour", hour, rx_delta, tx_delta).await?;

                    // day
                    let day = (now / 86400) * 86400;
                    self.add_traffic(id, "day", day, rx_delta, tx_delta).await?;

                    // month (approximate to 1st of month)
                    let month = now - (now % 2592000); 
                    self.add_traffic(id, "month", month, rx_delta, tx_delta).await?;

                    println!("Updated {}: +rx={} +tx={}", stat.name, format_bytes(rx_delta), format_bytes(tx_delta));
                }
            } else {
                self.create_interface(&stat.name, stat.rx_bytes, stat.tx_bytes).await?;
                println!("New interface found: {} (host: {})", stat.name, self.hostname);
            }
        }
        Ok(())
    }
}

pub fn load_config(path: &Path) -> Config {
    let mut config = Config {
        update_interval: 30,
        sync_interval: 300,
        ..Default::default()
    };

    if let Ok(content) = fs::read_to_string(path) {
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
                _ => {}
            }
        }

        // Combine DatabaseDir and Database if both are present, or use what's available
        config.database = match (database_dir, database_file) {
            (Some(dir), Some(file)) => Some(dir.join(file)),
            (None, Some(file)) => Some(PathBuf::from(file)),
            (Some(dir), None) => Some(dir.join("vnstat-rs.db")),
            (None, None) => Some(PathBuf::from("/var/lib/vnstat-rs/vnstat-rs.db")),
        };
    }

    config
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
    let mut stats = Vec::new();

    for line in content.lines().skip(2) {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 17 {
            continue;
        }

        let name = parts[0].trim_end_matches(':').to_string();
        let rx_bytes = parts[1].parse::<u64>().context("rx_bytes parse error")?;
        let tx_bytes = parts[9].parse::<u64>().context("tx_bytes parse error")?;

        stats.push(InterfaceStats {
            name,
            rx_bytes,
            tx_bytes,
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
