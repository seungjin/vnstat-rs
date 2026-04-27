use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use libsql::{params, Builder, Connection, Database};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Path to the database file
    #[arg(short, long, value_name = "FILE")]
    database: Option<PathBuf>,

    /// URL for remote libSQL/Turso database
    #[arg(long)]
    url: Option<String>,

    /// Token for remote libSQL/Turso database
    #[arg(long)]
    token: Option<String>,

    /// Path to config file
    #[arg(short, long, value_name = "FILE", default_value = "/etc/vnstat.conf")]
    config: PathBuf,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize the database
    Init,
    /// Update statistics once
    Update,
    /// Show statistics
    Show,
    /// Sync with remote database
    Sync,
    /// Run as a daemon to update statistics periodically
    Daemon {
        /// Update interval in seconds
        #[arg(short, long)]
        interval: Option<u64>,
        /// Sync interval in seconds
        #[arg(long)]
        sync_interval: Option<u64>,
    },
}

#[derive(Default, Debug)]
struct Config {
    database: Option<PathBuf>,
    url: Option<String>,
    token: Option<String>,
    update_interval: u64,
    sync_interval: u64,
}

struct Db {
    db: Database,
    conn: Connection,
    hostname: String,
    machine_id: String,
}

#[derive(Debug)]
struct InterfaceStats {
    name: String,
    rx_bytes: u64,
    tx_bytes: u64,
}

impl Db {
    async fn open(path: PathBuf, url: Option<String>, token: Option<String>) -> Result<Self> {
        let path_str = path.to_string_lossy().to_string();
        
        let db = if let (Some(url), Some(token)) = (url, token) {
            println!("Opening remote replica at {}...", url);
            Builder::new_remote_replica(path_str, url, token)
                .build()
                .await?
        } else {
            Builder::new_local(&path_str).build().await?
        };

        let conn = db.connect()?;
        let hostname = hostname::get()?.to_string_lossy().to_string();
        let machine_id = get_machine_id()?;

        Ok(Self { db, conn, hostname, machine_id })
    }

    async fn sync(&self) -> Result<()> {
        println!("Syncing with remote...");
        self.db.sync().await?;
        println!("Sync complete.");
        Ok(())
    }

    async fn init_schema(&self) -> Result<()> {
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS interface (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                machine_id TEXT NOT NULL,
                hostname TEXT NOT NULL,
                name TEXT NOT NULL,
                alias TEXT,
                added INTEGER,
                active INTEGER DEFAULT 1,
                maxbw INTEGER DEFAULT 0,
                last_rx INTEGER DEFAULT 0,
                last_tx INTEGER DEFAULT 0,
                UNIQUE(machine_id, name)
            )",
            (),
        ).await?;

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS traffic (
                interface_id INTEGER,
                timestamp INTEGER,
                rx INTEGER,
                tx INTEGER,
                FOREIGN KEY (interface_id) REFERENCES interface(id) ON DELETE CASCADE
            )",
            (),
        ).await?;

        Ok(())
    }

    async fn get_interface(&self, name: &str) -> Result<Option<(i64, u64, u64)>> {
        let mut rows = self.conn.query(
            "SELECT id, last_rx, last_tx FROM interface WHERE machine_id = ? AND name = ?", 
            params![self.machine_id.clone(), name]
        ).await?;
        
        if let Some(row) = rows.next().await? {
            return Ok(Some((row.get(0)?, row.get::<i64>(1)? as u64, row.get::<i64>(2)? as u64)));
        }
        Ok(None)
    }

    async fn create_interface(&self, name: &str, rx: u64, tx: u64) -> Result<i64> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        self.conn.execute(
            "INSERT INTO interface (machine_id, hostname, name, added, last_rx, last_tx) VALUES (?, ?, ?, ?, ?, ?)",
            params![self.machine_id.clone(), self.hostname.clone(), name, now, rx as i64, tx as i64],
        ).await?;

        let mut rows = self.conn.query(
            "SELECT id FROM interface WHERE machine_id = ? AND name = ?", 
            params![self.machine_id.clone(), name]
        ).await?;
        
        if let Some(row) = rows.next().await? {
            return Ok(row.get(0)?);
        }
        Err(anyhow::anyhow!("Failed to create interface"))
    }

    async fn update_interface(&self, id: i64, rx: u64, tx: u64) -> Result<()> {
        self.conn.execute(
            "UPDATE interface SET last_rx = ?, last_tx = ? WHERE id = ?",
            params![rx as i64, tx as i64, id],
        ).await?;
        Ok(())
    }

    async fn record_traffic_delta(&self, interface_id: i64, rx_delta: u64, tx_delta: u64) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        self.conn.execute(
            "INSERT INTO traffic (interface_id, timestamp, rx, tx) VALUES (?, ?, ?, ?)",
            params![interface_id, now, rx_delta as i64, tx_delta as i64],
        ).await?;
        Ok(())
    }

    async fn update_stats(&self) -> Result<()> {
        let stats = parse_net_dev()?;
        for stat in stats {
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
                    self.record_traffic_delta(id, rx_delta, tx_delta).await?;
                    self.update_interface(id, stat.rx_bytes, stat.tx_bytes).await?;
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

fn load_config(path: &Path) -> Config {
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

            // vnStat config uses whitespace as separator
            let parts: Vec<&str> = line.splitn(2, |c: char| c.is_whitespace()).map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
            if parts.len() < 2 {
                continue;
            }

            let key = parts[0];
            let value = parts[1].trim_matches('"');

            match key {
                "DatabaseDir" => database_dir = Some(PathBuf::from(value)),
                "Database" => database_file = Some(value.to_string()),
                "LibsqlUrl" => config.url = Some(value.to_string()),
                "LibsqlToken" => config.token = Some(value.to_string()),
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
            (Some(dir), None) => Some(dir.join("vnstat.db")),
            (None, None) => None,
        };
    }

    config
}

fn get_machine_id() -> Result<String> {
    if let Ok(id) = fs::read_to_string("/etc/machine-id") {
        return Ok(id.trim().to_string());
    }
    if let Ok(id) = fs::read_to_string("/var/lib/dbus/machine-id") {
        return Ok(id.trim().to_string());
    }
    Err(anyhow::anyhow!("Failed to read machine-id"))
}

fn parse_net_dev() -> Result<Vec<InterfaceStats>> {
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

fn format_bytes(bytes: u64) -> String {
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

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let file_config = load_config(&cli.config);
    
    let db_path = cli.database
        .or(file_config.database)
        .unwrap_or_else(|| PathBuf::from("vnstat.db"));
    
    let url = cli.url.or(file_config.url);
    let token = cli.token.or(file_config.token);

    let db = Db::open(db_path, url, token).await?;

    match cli.command {
        Some(Commands::Init) | None => {
            println!("Initializing database for host: {} ({})", db.hostname, db.machine_id);
            db.init_schema().await?;
            println!("Database initialized.");
        }
        Some(Commands::Update) => {
            db.update_stats().await?;
        }
        Some(Commands::Sync) => {
            db.sync().await?;
        }
        Some(Commands::Daemon { interval, sync_interval }) => {
            let interval = interval.unwrap_or(file_config.update_interval);
            let sync_interval = sync_interval.unwrap_or(file_config.sync_interval);
            
            println!("Running as daemon (host: {}, update: {}s, sync: {}s)...", db.hostname, interval, sync_interval);
            let mut last_sync = SystemTime::now();
            loop {
                if let Err(e) = db.update_stats().await {
                    eprintln!("Error updating stats: {}", e);
                }
                
                if last_sync.elapsed()?.as_secs() >= sync_interval {
                    if let Err(e) = db.sync().await {
                        eprintln!("Error syncing: {}", e);
                    }
                    last_sync = SystemTime::now();
                }

                sleep(Duration::from_secs(interval)).await;
            }
        }
        Some(Commands::Show) => {
            let mut rows = db.conn.query("
                SELECT i.hostname, i.name, SUM(t.rx), SUM(t.tx) 
                FROM interface i 
                JOIN traffic t ON i.id = t.interface_id 
                GROUP BY i.machine_id, i.name
                ORDER BY i.hostname, i.name", ()).await?;
            
            println!("{:<20} {:<15} {:<15} {:<15} {:<15}", "Host", "Interface", "Total RX", "Total TX", "Total");
            while let Some(row) = rows.next().await? {
                let host: String = row.get(0)?;
                let name: String = row.get(1)?;
                let rx: i64 = row.get(2)?;
                let tx: i64 = row.get(3)?;
                let total = rx + tx;
                println!("{:<20} {:<15} {:<15} {:<15} {:<15}", 
                    host,
                    name, 
                    format_bytes(rx as u64), 
                    format_bytes(tx as u64), 
                    format_bytes(total as u64)
                );
            }
        }
    }

    Ok(())
}
