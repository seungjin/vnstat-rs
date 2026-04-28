use anyhow::{Context, Result};
use chrono::Datelike;
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
    pub host_id: i64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct InterfaceStats {
    pub name: String,
    pub alias: Option<String>,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub rx_packets: u64,
    pub tx_packets: u64,
    pub hostname: String,
    pub created: i64,
    pub updated: i64,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct VnStatJson {
    pub vnstatversion: String,
    pub jsonversion: String,
    pub interfaces: Vec<JsonInterface>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct JsonInterface {
    pub name: String,
    pub alias: String,
    pub created: JsonTimestamp,
    pub updated: JsonTimestamp,
    pub traffic: JsonTraffic,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct JsonTimestamp {
    pub date: JsonDate,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<JsonTime>,
    pub timestamp: i64,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy)]
pub struct JsonDate {
    pub year: i32,
    pub month: u32,
    pub day: u32,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy)]
pub struct JsonTime {
    pub hour: u32,
    pub minute: u32,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Default)]
pub struct JsonTraffic {
    pub total: JsonTotal,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub fiveminute: Vec<JsonHistoryEntry>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub hour: Vec<JsonHistoryEntry>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub day: Vec<JsonHistoryEntry>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub month: Vec<JsonHistoryEntry>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub year: Vec<JsonHistoryEntry>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub top: Vec<JsonHistoryEntry>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Default)]
pub struct JsonTotal {
    pub rx: u64,
    pub tx: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct JsonHistoryEntry {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<i64>,
    pub date: JsonDate,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<JsonTime>,
    pub timestamp: i64,
    pub rx: u64,
    pub tx: u64,
}

impl JsonTimestamp {
    pub fn from_timestamp(ts: i64, include_time: bool) -> Self {
        let dt = chrono::DateTime::from_timestamp(ts, 0).unwrap_or_default();
        Self {
            date: JsonDate {
                year: dt.year(),
                month: dt.month(),
                day: dt.day(),
            },
            time: if include_time {
                use chrono::Timelike;
                Some(JsonTime {
                    hour: dt.hour(),
                    minute: dt.minute(),
                })
            } else {
                None
            },
            timestamp: ts,
        }
    }
}

impl HistoryEntry {
    pub fn to_json(&self, include_time: bool) -> JsonHistoryEntry {
        let ts = JsonTimestamp::from_timestamp(self.date, include_time);
        JsonHistoryEntry {
            id: None,
            date: ts.date,
            time: ts.time,
            timestamp: self.date,
            rx: self.rx,
            tx: self.tx,
        }
    }
}

impl InterfaceStats {
    pub fn to_json(&self) -> JsonInterface {
        JsonInterface {
            name: self.name.clone(),
            alias: self.alias.clone().unwrap_or_default(),
            created: JsonTimestamp::from_timestamp(self.created, false),
            updated: JsonTimestamp::from_timestamp(self.updated, true),
            traffic: JsonTraffic {
                total: JsonTotal {
                    rx: self.rx_bytes,
                    tx: self.tx_bytes,
                },
                ..Default::default()
            },
        }
    }
}

impl JsonDate {
    pub fn to_xml(&self) -> String {
        format!("<date><year>{}</year><month>{:02}</month><day>{:02}</day></date>", self.year, self.month, self.day)
    }
}

impl JsonTime {
    pub fn to_xml(&self) -> String {
        format!("<time><hour>{:02}</hour><minute>{:02}</minute></time>", self.hour, self.minute)
    }
}

impl JsonTimestamp {
    pub fn to_xml(&self, tag: &str) -> String {
        let mut out = format!("<{}>{}", tag, self.date.to_xml());
        if let Some(ref t) = self.time {
            out.push_str(&t.to_xml());
        }
        out.push_str(&format!("<timestamp>{}</timestamp></{}>", self.timestamp, tag));
        out
    }
}

impl JsonHistoryEntry {
    pub fn to_xml(&self, tag: &str) -> String {
        let mut out = format!("<{} id=\"{}\">{}", tag, self.id.unwrap_or(0), self.date.to_xml());
        if let Some(ref t) = self.time {
            out.push_str(&t.to_xml());
        }
        out.push_str(&format!("<timestamp>{}</timestamp><rx>{}</rx><tx>{}</tx></{}>", self.timestamp, self.rx, self.tx, tag));
        out
    }
}

impl JsonTraffic {
    pub fn to_xml(&self) -> String {
        let mut out = String::from("<traffic>");
        out.push_str(&format!("<total><rx>{}</rx><tx>{}</tx></total>", self.total.rx, self.total.tx));
        
        let write_entries = |entries: &[JsonHistoryEntry], plural: &str, singular: &str| -> String {
            if entries.is_empty() { return String::new(); }
            let mut s = format!("<{}>", plural);
            for entry in entries {
                s.push_str(&entry.to_xml(singular));
            }
            s.push_str(&format!("</{}>", plural));
            s
        };

        out.push_str(&write_entries(&self.fiveminute, "fiveminutes", "fiveminute"));
        out.push_str(&write_entries(&self.hour, "hours", "hour"));
        out.push_str(&write_entries(&self.day, "days", "day"));
        out.push_str(&write_entries(&self.month, "months", "month"));
        out.push_str(&write_entries(&self.year, "years", "year"));
        out.push_str(&write_entries(&self.top, "tops", "top"));

        out.push_str("</traffic>");
        out
    }
}

impl JsonInterface {
    pub fn to_xml(&self) -> String {
        let mut out = format!(" <interface name=\"{}\">", self.name);
        out.push_str(&format!("<name>{}</name>", self.name));
        out.push_str(&format!("<alias>{}</alias>", self.alias));
        out.push_str(&self.created.to_xml("created"));
        out.push_str(&self.updated.to_xml("updated"));
        out.push_str(&self.traffic.to_xml());
        out.push_str(" </interface>");
        out
    }
}

impl VnStatJson {
    pub fn to_xml(&self) -> String {
        let mut out = format!("<vnstat version=\"{}\" xmlversion=\"2\">\n", self.vnstatversion);
        for iface in &self.interfaces {
            out.push_str(&iface.to_xml());
            out.push('\n');
        }
        out.push_str("</vnstat>");
        out
    }
    
    pub fn new(stats: Vec<InterfaceStats>) -> Self {
        Self {
            vnstatversion: env!("CARGO_PKG_VERSION").to_string(),
            jsonversion: "2".to_string(),
            interfaces: stats.into_iter().map(|s| s.to_json()).collect(),
        }
    }

    pub fn from_history(history: Vec<HistoryEntry>, table: &str) -> Self {
        let mut json = Self::new(vec![]);
        json.insert_history(history, table);
        json
    }

    pub fn insert_history(&mut self, history: Vec<HistoryEntry>, table: &str) {
        for entry in history {
            if let Some(iface) = self.interfaces.iter_mut().find(|i| i.name == entry.interface) {
                let json_entry = entry.to_json(table == "fiveminute" || table == "hour");
                match table {
                    "fiveminute" => iface.traffic.fiveminute.push(json_entry),
                    "hour" => iface.traffic.hour.push(json_entry),
                    "day" => iface.traffic.day.push(json_entry),
                    "month" => iface.traffic.month.push(json_entry),
                    "year" => iface.traffic.year.push(json_entry),
                    "top" => iface.traffic.top.push(json_entry),
                    _ => {}
                }
            } else {
                // If interface not found in stats (unlikely but possible), create a dummy one
                let mut traffic = JsonTraffic::default();
                let json_entry = entry.to_json(table == "fiveminute" || table == "hour");
                match table {
                    "fiveminute" => traffic.fiveminute.push(json_entry),
                    "hour" => traffic.hour.push(json_entry),
                    "day" => traffic.day.push(json_entry),
                    "month" => traffic.month.push(json_entry),
                    "year" => traffic.year.push(json_entry),
                    "top" => traffic.top.push(json_entry),
                    _ => {}
                }
                self.interfaces.push(JsonInterface {
                    name: entry.interface.clone(),
                    alias: String::new(),
                    created: JsonTimestamp::from_timestamp(0, false),
                    updated: JsonTimestamp::from_timestamp(0, false),
                    traffic,
                });
            }
        }
    }
}

#[derive(Default, Debug)]
pub struct Config {
    pub database: Option<PathBuf>,
    pub url: Option<String>,
    pub token: Option<String>,
    pub update_interval: u64,
    pub sync_interval: u64,
    pub daemon_socket: Option<PathBuf>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum IpcRequest {
    GetStats { interface: Option<String> },
    GetHistory { 
        table: String, 
        interface: Option<String>, 
        limit: usize,
        begin: Option<i64>,
        end: Option<i64>,
    },
    GetSummary { interface: Option<String> },
    GetInfo,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum IpcResponse {
    Stats(Vec<InterfaceStats>),
    History(Vec<HistoryEntry>),
    Summary(Vec<SummaryData>),
    Info { hostname: String, machine_id: String },
    Error(String),
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct SummaryData {
    pub name: String,
    pub today: (u64, u64),
    pub yesterday: (u64, u64),
    pub this_month: (u64, u64),
    pub last_month: (u64, u64),
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct HistoryEntry {
    pub hostname: String,
    pub interface: String,
    pub date: i64,
    pub rx: u64,
    pub tx: u64,
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
            
            let conn = db.connect().await?;
            let db_type = DbType::Sync(db);
            
            (db_type, conn)
        } else {
            let db = turso::Builder::new_local(&path_str).build().await?;
            let conn = db.connect()?;
            (DbType::Local(db), conn)
        };

        let hostname = hostname::get()?.to_string_lossy().to_string();
        let machine_id = get_machine_id()?;

        let mut db_obj = Self { db: db_type, conn, hostname, machine_id, host_id: 0 };
        db_obj.init_schema().await?;
        db_obj.host_id = db_obj.get_or_create_host().await?;

        // Push initial schema and host info to remote if sync is enabled
        if let DbType::Sync(_) = db_obj.db {
            let _ = db_obj.sync().await;
        }

        Ok(db_obj)
    }

    pub async fn sync(&self) -> Result<()> {
        if let DbType::Sync(ref db) = self.db {
            println!("Syncing with remote (push)...");
            db.push().await?;
            println!("Sync complete.");
        } else {
            println!("Skipping sync: No remote database configured.");
        }
        Ok(())
    }
}

const SCHEMA_SQL: &str = include_str!("../schema.sql");
const CURRENT_VERSION: i32 = 3;

impl Db {
    pub async fn execute_batch(&self, sql: &str) -> Result<()> {
        let mut clean_sql = String::new();
        for line in sql.lines() {
            let trimmed = line.trim();
            if !trimmed.is_empty() && !trimmed.starts_with("--") {
                // Handle inline comments if necessary, but line-start is most common
                if let Some(pos) = trimmed.find("--") {
                    clean_sql.push_str(&trimmed[..pos]);
                } else {
                    clean_sql.push_str(trimmed);
                }
                clean_sql.push(' ');
            }
        }

        for statement in clean_sql.split(';') {
            let trimmed = statement.trim();
            if !trimmed.is_empty() {
                self.conn.execute(trimmed, ()).await?;
            }
        }
        Ok(())
    }

    pub async fn init_schema(&self) -> Result<()> {
        // Run the base schema (includes CREATE TABLE IF NOT EXISTS)
        self.execute_batch(SCHEMA_SQL).await?;

        // Get current version from info table
        let mut rows = self.conn.query("SELECT value FROM info WHERE name = 'version'", ()).await?;
        let mut version: i32 = if let Some(row) = rows.next().await? {
            row.get::<String>(0)?.parse().unwrap_or(1)
        } else {
            1
        };

        if version < CURRENT_VERSION {
            println!("Migrating database from version {} to {}...", version, CURRENT_VERSION);
            
            // Version 2 to 3 migration (Host table and normalization)
            if version == 2 {
                // Check if interface table needs host_id (double check)
                let mut rows = self.conn.query("PRAGMA table_info(interface)", ()).await?;
                let mut has_host_id = false;
                while let Some(row) = rows.next().await? {
                    let name: String = row.get(1)?;
                    if name == "host_id" {
                        has_host_id = true;
                        break;
                    }
                }

                if !has_host_id {
                    println!("Applying v3 migration: Normalizing interface table with host_id...");
                    
                    self.execute_batch("DROP TABLE IF EXISTS interface_old").await?;
                    self.conn.execute("ALTER TABLE interface RENAME TO interface_old", ()).await?;
                    
                    self.execute_batch("
                        CREATE TABLE interface (
                            id           INTEGER PRIMARY KEY AUTOINCREMENT,
                            host_id      INTEGER NOT NULL REFERENCES host(id) ON DELETE CASCADE,
                            name         TEXT NOT NULL,
                            alias        TEXT,
                            active       INTEGER NOT NULL DEFAULT 1,
                            created      INTEGER NOT NULL,
                            updated      INTEGER NOT NULL,
                            rxcounter    INTEGER NOT NULL DEFAULT 0,
                            txcounter    INTEGER NOT NULL DEFAULT 0,
                            rxtotal      INTEGER NOT NULL DEFAULT 0,
                            txtotal      INTEGER NOT NULL DEFAULT 0,
                            CONSTRAINT u_host_name UNIQUE(host_id, name)
                        );
                    ").await?;

                    // Create initial host record for migration
                    self.conn.execute(
                        "INSERT OR IGNORE INTO host (machine_id, hostname) VALUES (?, ?)",
                        (self.machine_id.clone(), self.hostname.clone()),
                    ).await?;
                    
                    let mut host_rows = self.conn.query("SELECT id FROM host WHERE machine_id = ?", (self.machine_id.clone(),)).await?;
                    if let Some(host_row) = host_rows.next().await? {
                        let host_id: i64 = host_row.get(0)?;
                        
                        // Copy data from old table
                        self.conn.execute(
                            &format!("INSERT INTO interface (id, host_id, name, alias, active, created, updated, rxcounter, txcounter, rxtotal, txtotal) 
                                     SELECT id, {}, name, alias, active, created, updated, rxcounter, txcounter, rxtotal, txtotal FROM interface_old", host_id),
                            (),
                        ).await?;
                    }
                    
                    let _ = self.conn.execute("DROP TABLE interface_old", ()).await;
                }
                version = 3;
            }

            // Update version in info table
            self.conn.execute(
                "UPDATE info SET value = ? WHERE name = 'version'",
                (version.to_string(),),
            ).await?;
            
            println!("Migration to version {} complete.", version);
        }

        Ok(())
    }

    pub async fn get_or_create_host(&self) -> Result<i64> {
        self.conn.execute(
            "INSERT OR IGNORE INTO host (machine_id, hostname) VALUES (?, ?)",
            (self.machine_id.clone(), self.hostname.clone()),
        ).await?;

        // Update hostname if it changed for the same machine_id
        self.conn.execute(
            "UPDATE host SET hostname = ? WHERE machine_id = ?",
            (self.hostname.clone(), self.machine_id.clone()),
        ).await?;

        let mut rows = self.conn.query(
            "SELECT id FROM host WHERE machine_id = ?",
            (self.machine_id.clone(),),
        ).await?;

        if let Some(row) = rows.next().await? {
            return Ok(row.get(0)?);
        }
        Err(anyhow::anyhow!("Failed to get or create host"))
    }

    pub async fn get_interface(&self, name: &str) -> Result<Option<(i64, u64, u64)>> {
        let mut rows = self.conn.query(
            "SELECT id, rxcounter, txcounter FROM interface WHERE host_id = ? AND name = ?", 
            (self.host_id, name)
        ).await?;
        
        if let Some(row) = rows.next().await? {
            return Ok(Some((row.get(0)?, row.get::<i64>(1)? as u64, row.get::<i64>(2)? as u64)));
        }
        Ok(None)
    }

    pub async fn create_interface(&self, name: &str, rx: u64, tx: u64) -> Result<i64> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        self.conn.execute(
            "INSERT INTO interface (host_id, name, created, updated, rxcounter, txcounter) VALUES (?, ?, ?, ?, ?, ?)",
            (self.host_id, name, now, now, rx as i64, tx as i64),
        ).await?;

        let mut rows = self.conn.query(
            "SELECT id FROM interface WHERE host_id = ? AND name = ?", 
            (self.host_id, name)
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
                    let dt = chrono::DateTime::from_timestamp(now, 0).unwrap();
                    let naive = dt.naive_utc();

                    // fiveminute
                    let five_min = (now / 300) * 300;
                    self.add_traffic(id, "fiveminute", five_min, rx_delta, tx_delta).await?;

                    // hour
                    let hour = (now / 3600) * 3600;
                    self.add_traffic(id, "hour", hour, rx_delta, tx_delta).await?;

                    // day
                    let day_dt = naive.date().and_hms_opt(0, 0, 0).unwrap();
                    let day = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(day_dt, chrono::Utc).timestamp();
                    self.add_traffic(id, "day", day, rx_delta, tx_delta).await?;

                    // month
                    let month_dt = naive.date().with_day(1).unwrap().and_hms_opt(0, 0, 0).unwrap();
                    let month = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(month_dt, chrono::Utc).timestamp();
                    self.add_traffic(id, "month", month, rx_delta, tx_delta).await?;

                    // year
                    let year_dt = naive.date().with_day(1).unwrap().with_month(1).unwrap().and_hms_opt(0, 0, 0).unwrap();
                    let year = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(year_dt, chrono::Utc).timestamp();
                    self.add_traffic(id, "year", year, rx_delta, tx_delta).await?;

                    // top (same as day, but in top table)
                    self.add_traffic(id, "top", day, rx_delta, tx_delta).await?;

                    println!("Updated {}: +rx={} +tx={}", stat.name, format_bytes(rx_delta), format_bytes(tx_delta));
                }
            } else {
                self.create_interface(&stat.name, stat.rx_bytes, stat.tx_bytes).await?;
                println!("New interface found: {} (host: {})", stat.name, self.hostname);
            }
        }
        Ok(())
    }

    pub async fn get_all_interface_stats(&self, filter_iface: Option<&str>) -> Result<Vec<InterfaceStats>> {
        let mut query = "SELECT i.name, i.alias, i.rxtotal, i.txtotal, h.hostname, i.created, i.updated 
                         FROM interface i 
                         JOIN host h ON i.host_id = h.id".to_string();
        if let Some(iface) = filter_iface {
            query.push_str(&format!(" WHERE i.name = '{}' ", iface));
        }
        
        let mut rows = self.conn.query(&query, ()).await?;
        let mut stats = Vec::new();
        while let Some(row) = rows.next().await? {
            stats.push(InterfaceStats {
                name: row.get(0)?,
                alias: row.get(1)?,
                rx_bytes: row.get::<i64>(2)? as u64,
                tx_bytes: row.get::<i64>(3)? as u64,
                rx_packets: 0,
                tx_packets: 0,
                hostname: row.get(4)?,
                created: row.get(5)?,
                updated: row.get(6)?,
            });
        }
        Ok(stats)
    }

    pub async fn get_history(&self, table: &str, filter_iface: Option<&str>, limit: usize, begin: Option<i64>, end: Option<i64>) -> Result<Vec<HistoryEntry>> {
        let mut query = format!(
            "SELECT h.hostname, i.name, t.date, t.rx, t.tx 
             FROM interface i 
             JOIN host h ON i.host_id = h.id
             JOIN {} t ON i.id = t.interface WHERE 1=1 ", table);
        
        if let Some(iface) = filter_iface {
            query.push_str(&format!("AND i.name = '{}' ", iface));
        }

        if let Some(b) = begin {
            query.push_str(&format!("AND t.date >= {} ", b));
        }

        if let Some(e) = end {
            query.push_str(&format!("AND t.date <= {} ", e));
        }

        if table == "top" {
            query.push_str(&format!("ORDER BY (t.rx + t.tx) DESC LIMIT {}", limit));
        } else {
            query.push_str(&format!("ORDER BY t.date DESC LIMIT {}", limit));
        }

        let mut rows = self.conn.query(&query, ()).await?;
        let mut history = Vec::new();
        while let Some(row) = rows.next().await? {
            history.push(HistoryEntry {
                hostname: row.get(0)?,
                interface: row.get(1)?,
                date: row.get(2)?,
                rx: row.get::<i64>(3)? as u64,
                tx: row.get::<i64>(4)? as u64,
            });
        }
        Ok(history)
    }

    pub async fn get_summary(&self, filter_iface: Option<&str>) -> Result<Vec<SummaryData>> {
        let mut ifaces_query = "SELECT id, name FROM interface".to_string();
        if let Some(iface) = filter_iface {
            ifaces_query.push_str(&format!(" WHERE name = '{}'", iface));
        }
        ifaces_query.push_str(" ORDER BY name");

        let mut iface_rows = self.conn.query(&ifaces_query, ()).await?;
        let mut interfaces = Vec::new();
        while let Some(row) = iface_rows.next().await? {
            interfaces.push((row.get::<i64>(0)?, row.get::<String>(1)?));
        }

        let now = chrono::Utc::now();
        let today_start = now.date_naive().and_hms_opt(0, 0, 0).unwrap();
        let today_ts = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(today_start, chrono::Utc).timestamp();
        let yesterday_ts = today_ts - 86400;

        let this_month_start = now.date_naive().with_day(1).unwrap().and_hms_opt(0, 0, 0).unwrap();
        let this_month_ts = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(this_month_start, chrono::Utc).timestamp();
        
        let last_month_date = if now.month() == 1 {
            now.date_naive().with_year(now.year() - 1).unwrap().with_month(12).unwrap().with_day(1).unwrap()
        } else {
            now.date_naive().with_month(now.month() - 1).unwrap().with_day(1).unwrap()
        };
        let last_month_ts = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(last_month_date.and_hms_opt(0, 0, 0).unwrap(), chrono::Utc).timestamp();

        let mut summaries = Vec::new();

        for (id, name) in interfaces {
            let mut stats = std::collections::HashMap::new();
            
            // Month stats
            let mut m_rows = self.conn.query("SELECT date, rx, tx FROM month WHERE interface = ? AND date IN (?, ?)", (id, this_month_ts, last_month_ts)).await?;
            while let Some(row) = m_rows.next().await? {
                stats.insert(format!("m_{}", row.get::<i64>(0)?), (row.get::<i64>(1)? as u64, row.get::<i64>(2)? as u64));
            }

            // Day stats
            let mut d_rows = self.conn.query("SELECT date, rx, tx FROM day WHERE interface = ? AND date IN (?, ?)", (id, today_ts, yesterday_ts)).await?;
            while let Some(row) = d_rows.next().await? {
                stats.insert(format!("d_{}", row.get::<i64>(0)?), (row.get::<i64>(1)? as u64, row.get::<i64>(2)? as u64));
            }

            summaries.push(SummaryData {
                name,
                today: stats.get(&format!("d_{}", today_ts)).cloned().unwrap_or((0, 0)),
                yesterday: stats.get(&format!("d_{}", yesterday_ts)).cloned().unwrap_or((0, 0)),
                this_month: stats.get(&format!("m_{}", this_month_ts)).cloned().unwrap_or((0, 0)),
                last_month: stats.get(&format!("m_{}", last_month_ts)).cloned().unwrap_or((0, 0)),
            });
        }

        Ok(summaries)
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
                "DaemonSocket" => config.daemon_socket = Some(PathBuf::from(value)),
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

        if config.daemon_socket.is_none() {
            config.daemon_socket = Some(PathBuf::from("/var/run/vnstat-rs.sock"));
        }
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

        // Standard Linux /proc/net/dev columns:
        // RX: bytes(0) packets(1) errs(2) drop(3) fifo(4) frame(5) compressed(6) multicast(7)
        // TX: bytes(8) packets(9) errs(10) drop(11) fifo(12) colls(13) carrier(14) compressed(15)
        
        let rx_bytes = data_parts[0].parse::<u64>().unwrap_or_else(|_| {
            // If it's the first time we see this, maybe it's not a data line
            0
        });
        let rx_packets = data_parts[1].parse::<u64>().unwrap_or(0);
        let tx_bytes = data_parts[8].parse::<u64>().unwrap_or(0);
        let tx_packets = data_parts[9].parse::<u64>().unwrap_or(0);

        if rx_bytes == 0 && rx_packets == 0 && tx_bytes == 0 && tx_packets == 0 {
            // Check if this interface has any data at all, if not skip
            // (But we want to keep it if it's just idle)
        }

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

    #[test]
    fn test_vnstat_xml_format() {
        let stats = vec![InterfaceStats {
            name: "eth0".to_string(),
            alias: Some("lan".to_string()),
            rx_bytes: 1000,
            tx_bytes: 2000,
            rx_packets: 10,
            tx_packets: 20,
            hostname: "test-host".to_string(),
            created: 1700000000,
            updated: 1700003600,
        }];

        let json = VnStatJson::new(stats);
        let xml = json.to_xml();
        
        assert!(xml.contains("<vnstat version="));
        assert!(xml.contains("xmlversion=\"2\""));
        assert!(xml.contains("<interface name=\"eth0\">"));
        assert!(xml.contains("<name>eth0</name>"));
        assert!(xml.contains("<alias>lan</alias>"));
        assert!(xml.contains("<traffic>"));
        assert!(xml.contains("<total><rx>1000</rx><tx>2000</tx></total>"));
    }

    #[test]
    fn test_vnstat_json_history_format() {
        let history = vec![HistoryEntry {
            hostname: "test-host".to_string(),
            interface: "eth0".to_string(),
            date: 1700000000,
            rx: 500,
            tx: 600,
        }];

        let mut json = VnStatJson::new(vec![]);
        json.insert_history(history, "day");
        let serialized = serde_json::to_string(&json).unwrap();
        
        assert!(serialized.contains("\"day\":["));
        assert!(serialized.contains("\"rx\":500,\"tx\":600"));
        assert!(serialized.contains("\"timestamp\":1700000000"));
    }

    #[test]
    fn test_vnstat_json_format() {
        let stats = vec![InterfaceStats {
            name: "eth0".to_string(),
            alias: Some("lan".to_string()),
            rx_bytes: 1000,
            tx_bytes: 2000,
            rx_packets: 10,
            tx_packets: 20,
            hostname: "test-host".to_string(),
            created: 1700000000,
            updated: 1700003600,
        }];

        let json = VnStatJson::new(stats);
        let serialized = serde_json::to_string(&json).unwrap();
        
        // Basic checks for expected fields
        assert!(serialized.contains("\"vnstatversion\":"));
        assert!(serialized.contains("\"jsonversion\":\"2\""));
        assert!(serialized.contains("\"interfaces\":"));
        assert!(serialized.contains("\"name\":\"eth0\""));
        assert!(serialized.contains("\"alias\":\"lan\""));
        assert!(serialized.contains("\"traffic\":"));
        assert!(serialized.contains("\"total\":{\"rx\":1000,\"tx\":2000}"));
    }

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
