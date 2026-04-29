use anyhow::{Context, Result};
use chrono::Datelike;
use std::fs;
use std::path::{PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::Arc;
use crate::models::{InterfaceStats, HistoryEntry, SummaryData};
use crate::utils::{get_machine_id, parse_net_dev, format_bytes};
use serde::Deserialize;
use libsql::{Builder, Connection, Database, params};

pub enum DbType {
    Local(Arc<Database>),
    Remote(Arc<Database>),
}

pub struct Db {
    pub db: DbType,
    pub conn: Connection,
    pub hostname: String,
    pub machine_id: String,
    pub host_id: String,
}

#[derive(Deserialize)]
struct Schema {
    version: i64,
    sql: String,
}

const SCHEMA_TOML: &str = include_str!("../schema.sql.toml");

struct Migration {
    version: i32,
    description: &'static str,
}

const MIGRATIONS: &[Migration] = &[
    Migration { version: 4, description: "Convert IDs to TEXT for multi-host support" },
];

impl Db {
    pub async fn open(path: PathBuf, url: Option<String>, token: Option<String>) -> Result<Self> {
        let (db_type, database) = if let (Some(url), Some(token)) = (url, token) {
            println!("Connecting directly to remote database at {}...", url);
            let db = Builder::new_remote(url, token).build().await?;
            let db_arc = Arc::new(db);
            (DbType::Remote(Arc::clone(&db_arc)), db_arc)
        } else {
            if let Some(parent) = path.parent() {
                if !parent.exists() {
                    println!("Creating database directory {}...", parent.display());
                    fs::create_dir_all(parent).context("Failed to create database directory")?;
                }
            }
            let path_str = path.to_string_lossy().to_string();
            let db = Builder::new_local(path_str).build().await?;
            let db_arc = Arc::new(db);
            (DbType::Local(Arc::clone(&db_arc)), db_arc)
        };

        let conn = database.connect()?;
        let hostname = hostname::get()?.to_string_lossy().to_string();
        let machine_id = get_machine_id()?;

        let mut db_obj = Self { db: db_type, conn, hostname, machine_id, host_id: String::new() };
        db_obj.init_schema().await?;
        db_obj.host_id = db_obj.get_or_create_host().await?;

        Ok(db_obj)
    }

    pub async fn sync(&self) -> Result<()> {
        // Direct remote connections using Hrana (libsql remote) don't need manual push/pull
        Ok(())
    }

    pub async fn execute_batch(&self, sql: &str) -> Result<()> {
        self.conn.execute_batch(sql).await?;
        Ok(())
    }

    pub async fn get_info(&self, name: &str) -> Result<Option<String>> {
        let mut rows = self.conn.query("SELECT value FROM info WHERE name = ?", [name]).await?;
        if let Some(row) = rows.next().await? {
            return Ok(Some(row.get(0)?));
        }
        Ok(None)
    }

    pub async fn set_info(&self, name: &str, value: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO info (name, value) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET value = excluded.value",
            [name, value],
        ).await?;
        Ok(())
    }

    pub async fn get_current_version(&self) -> Result<i32> {
        self.conn.execute("CREATE TABLE IF NOT EXISTS database_schema (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)", ()).await?;
        
        let mut rows = self.conn.query("SELECT MAX(version) FROM database_schema", ()).await?;
        if let Some(row) = rows.next().await? {
            if let Ok(v) = row.get::<i32>(0) {
                return Ok(v);
            }
        }

        let mut v = 0;
        if let Ok(mut r) = self.conn.query("SELECT value FROM info WHERE name = 'schema_version'", ()).await {
            if let Some(row) = r.next().await? {
                v = row.get::<String>(0)?.parse().unwrap_or(0);
            }
        } 
        if v == 0 {
            if let Ok(mut r) = self.conn.query("SELECT value FROM info WHERE name = 'version'", ()).await {
                if let Some(row) = r.next().await? {
                    v = row.get::<String>(0)?.parse().unwrap_or(0);
                    let _ = self.conn.execute("UPDATE info SET name = 'schema_version' WHERE name = 'version'", ()).await;
                }
            }
        }

        if v > 0 {
            let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
            let _ = self.conn.execute("INSERT OR IGNORE INTO database_schema (version, applied_at) VALUES (?, ?)", (v, now)).await;
        }

        Ok(v)
    }

    pub async fn init_schema(&self) -> Result<()> {
        let schema: Schema = toml::from_str(SCHEMA_TOML).context("Failed to parse schema.sql.toml")?;
        
        let mut current = self.get_current_version().await?;

        if current == 0 {
            println!("Initializing fresh database schema (v{})...", schema.version);
            self.execute_batch(&schema.sql).await?;
            let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
            self.conn.execute("INSERT INTO database_schema (version, applied_at) VALUES (?, ?)", (schema.version, now)).await?;
            self.set_info("schema_version", &schema.version.to_string()).await?;
            current = schema.version as i32;
        }

        for migration in MIGRATIONS {
            if migration.version > current {
                self.run_migration(migration.version, migration.description).await?;
            }
        }

        Ok(())
    }

    async fn run_migration(&self, version: i32, description: &str) -> Result<()> {
        let current = self.get_current_version().await?;
        if current >= version {
            return Ok(());
        }

        println!("Applying migration v{}: {}...", version, description);

        match version {
            4 => {
                let mut needs_v4 = true;
                if let Ok(mut r) = self.conn.query("SELECT name FROM sqlite_master WHERE type='table' AND name='host'", ()).await {
                    if r.next().await?.is_some() {
                    }
                }

                if needs_v4 {
                    self.conn.execute("CREATE TEMP TABLE host_mig AS SELECT * FROM host", ()).await?;
                    self.conn.execute("CREATE TEMP TABLE interface_mig AS SELECT * FROM interface", ()).await?;
                    self.conn.execute("CREATE TEMP TABLE traffic_mig AS SELECT * FROM day", ()).await?;

                    let tables = vec!["host", "interface", "fiveminute", "hour", "day", "month", "year", "top"];
                    for table in &tables {
                        let _ = self.conn.execute(&format!("DROP TABLE IF EXISTS {}", table), ()).await;
                    }

                    let schema: Schema = toml::from_str(SCHEMA_TOML).context("Failed to parse schema.sql.toml")?;
                    self.execute_batch(&schema.sql).await?;

                    let mut host_mig_rows = self.conn.query("SELECT count(*) FROM host_mig", ()).await?;
                    let host_count: i64 = if let Some(row) = host_mig_rows.next().await? {
                        row.get(0)?
                    } else { 0 };

                    if host_count > 0 {
                        self.conn.execute("INSERT OR IGNORE INTO host (id, machine_id, hostname) SELECT machine_id, machine_id, hostname FROM host_mig", ()).await?;
                        self.conn.execute("
                            INSERT OR IGNORE INTO interface (id, host_id, name, alias, active, created, updated, rxcounter, txcounter, rxtotal, txtotal)
                            SELECT h.machine_id || ':' || i.name, h.machine_id, i.name, i.alias, i.active, i.created, i.updated, i.rxcounter, i.txcounter, i.rxtotal, i.txtotal
                            FROM interface_mig i JOIN host_mig h ON i.host_id = h.id
                        ", ()).await?;
                        
                        self.conn.execute("
                            INSERT OR IGNORE INTO day (interface, date, rx, tx)
                            SELECT h.machine_id || ':' || i.name, t.date, t.rx, t.tx
                            FROM traffic_mig t
                            JOIN interface_mig i ON t.interface = i.id
                            JOIN host_mig h ON i.host_id = h.id
                        ", ()).await?;
                    }

                    let _ = self.conn.execute("DROP TABLE IF EXISTS host_mig", ()).await;
                    let _ = self.conn.execute("DROP TABLE IF EXISTS interface_mig", ()).await;
                    let _ = self.conn.execute("DROP TABLE IF EXISTS traffic_mig", ()).await;
                }
            },
            _ => return Err(anyhow::anyhow!("Unknown migration version {}", version)),
        }

        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        self.conn.execute("INSERT INTO database_schema (version, applied_at) VALUES (?, ?)", (version as i64, now)).await?;
        self.set_info("schema_version", &version.to_string()).await?;
        
        println!("Migration v{} complete.", version);
        Ok(())
    }

    pub async fn get_or_create_host(&self) -> Result<String> {
        self.conn.execute(
            "INSERT OR IGNORE INTO host (id, machine_id, hostname) VALUES (?, ?, ?)",
            [self.machine_id.clone(), self.machine_id.clone(), self.hostname.clone()],
        ).await?;

        self.conn.execute(
            "UPDATE host SET hostname = ? WHERE id = ?",
            [self.hostname.clone(), self.machine_id.clone()],
        ).await?;

        Ok(self.machine_id.clone())
    }

    pub async fn get_interface(&self, name: &str) -> Result<Option<(String, u64, u64)>> {
        let mut rows = self.conn.query(
            "SELECT id, rxcounter, txcounter FROM interface WHERE host_id = ? AND name = ?", 
            [self.host_id.clone(), name.to_string()]
        ).await?;
        
        if let Some(row) = rows.next().await? {
            return Ok(Some((row.get(0)?, row.get::<i64>(1)? as u64, row.get::<i64>(2)? as u64)));
        }
        Ok(None)
    }

    pub async fn create_interface(&self, name: &str, rx: u64, tx: u64) -> Result<String> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        let id = format!("{}:{}", self.host_id, name);
        self.conn.execute(
            "INSERT INTO interface (id, host_id, name, created, updated, rxcounter, txcounter) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (id.clone(), self.host_id.clone(), name.to_string(), now, now, rx as i64, tx as i64),
        ).await?;

        Ok(id)
    }

    pub async fn update_interface_counters(&self, id: &str, rx: u64, tx: u64, rx_delta: u64, tx_delta: u64) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        self.conn.execute(
            "UPDATE interface SET updated = ?, rxcounter = ?, txcounter = ?, rxtotal = rxtotal + ?, txtotal = txtotal + ? WHERE id = ?",
            (now, rx as i64, tx as i64, rx_delta as i64, tx_delta as i64, id.to_string()),
        ).await?;
        Ok(())
    }

    pub async fn add_traffic(&self, interface_id: &str, table: &str, date: i64, rx: u64, tx: u64) -> Result<()> {
        let sql = format!(
                "INSERT INTO {} (interface, date, rx, tx) VALUES (?, ?, ?, ?)
                 ON CONFLICT(interface, date) DO UPDATE SET rx = rx + excluded.rx, tx = tx + excluded.tx",
                table
            );
        self.conn.execute(
            &sql,
            (interface_id.to_string(), date, rx as i64, tx as i64),
        ).await?;
        Ok(())
    }

    pub async fn add_history_entry(&self, id: &str, rx_delta: u64, tx_delta: u64) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        let dt = chrono::DateTime::from_timestamp(now, 0).unwrap();
        let naive = dt.naive_utc();

        let five_min = (now / 300) * 300;
        self.add_traffic(id, "fiveminute", five_min, rx_delta, tx_delta).await?;

        let hour = (now / 3600) * 3600;
        self.add_traffic(id, "hour", hour, rx_delta, tx_delta).await?;

        let day_dt = naive.date().and_hms_opt(0, 0, 0).unwrap();
        let day = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(day_dt, chrono::Utc).timestamp();
        self.add_traffic(id, "day", day, rx_delta, tx_delta).await?;

        let month_dt = naive.date().with_day(1).unwrap().and_hms_opt(0, 0, 0).unwrap();
        let month = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(month_dt, chrono::Utc).timestamp();
        self.add_traffic(id, "month", month, rx_delta, tx_delta).await?;

        let year_dt = naive.date().with_day(1).unwrap().with_month(1).unwrap().and_hms_opt(0, 0, 0).unwrap();
        let year = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(year_dt, chrono::Utc).timestamp();
        self.add_traffic(id, "year", year, rx_delta, tx_delta).await?;

        self.add_traffic(id, "top", day, rx_delta, tx_delta).await?;
        Ok(())
    }

    pub async fn update_stats(&self, filter_iface: Option<&str>) -> Result<()> {
        let stats = parse_net_dev()?;

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
                    self.update_interface_counters(&id, stat.rx_bytes, stat.tx_bytes, rx_delta, tx_delta).await?;
                    self.add_history_entry(&id, rx_delta, tx_delta).await?;
                    println!("Updated {}: +rx={} +tx={}", stat.name, format_bytes(rx_delta), format_bytes(tx_delta));
                }
            } else {
                let id = self.create_interface(&stat.name, stat.rx_bytes, stat.tx_bytes).await?;
                // Add initial 0 delta to ensure history record is created for today
                self.add_history_entry(&id, 0, 0).await?;
                println!("New interface found: {} (host: {})", stat.name, self.hostname);
            }
        }
        Ok(())
    }

    pub async fn get_all_interface_stats(&self, filter_iface: Option<&str>) -> Result<Vec<InterfaceStats>> {
        let mut query_str = "SELECT i.name, i.alias, i.rxtotal, i.txtotal, h.hostname, i.created, i.updated 
                         FROM interface i 
                         JOIN host h ON i.host_id = h.id".to_string();
        if let Some(iface) = filter_iface {
            query_str.push_str(&format!(" WHERE i.name = '{}' ", iface));
        }
        
        let mut rows = self.conn.query(&query_str, ()).await?;
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
        let mut query_str = format!(
            "SELECT h.hostname, i.name, t.date, t.rx, t.tx 
             FROM interface i 
             JOIN host h ON i.host_id = h.id
             JOIN {} t ON i.id = t.interface WHERE 1=1 ", table);
        
        if let Some(iface) = filter_iface {
            query_str.push_str(&format!("AND i.name = '{}' ", iface));
        }

        if let Some(b) = begin {
            query_str.push_str(&format!("AND t.date >= {} ", b));
        }

        if let Some(e) = end {
            query_str.push_str(&format!("AND t.date <= {} ", e));
        }

        if table == "top" {
            query_str.push_str(&format!("ORDER BY (t.rx + t.tx) DESC LIMIT {}", limit));
        } else {
            query_str.push_str(&format!("ORDER BY t.date DESC LIMIT {}", limit));
        }

        let mut rows = self.conn.query(&query_str, ()).await?;
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
        let mut ifaces_query = format!("SELECT id, name FROM interface WHERE host_id = '{}'", self.host_id);
        if let Some(iface) = filter_iface {
            ifaces_query.push_str(&format!(" AND name = '{}'", iface));
        }
        ifaces_query.push_str(" ORDER BY name");

        let mut iface_rows = self.conn.query(&ifaces_query, ()).await?;
        let mut interfaces = Vec::new();
        while let Some(row) = iface_rows.next().await? {
            interfaces.push((row.get::<String>(0)?, row.get::<String>(1)?));
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
            
            let mut m_rows = self.conn.query("SELECT date, rx, tx FROM month WHERE interface = ? AND date IN (?, ?)", (id.clone(), this_month_ts, last_month_ts)).await?;
            while let Some(row) = m_rows.next().await? {
                stats.insert(format!("m_{}", row.get::<i64>(0)?), (row.get::<i64>(1)? as u64, row.get::<i64>(2)? as u64));
            }

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
