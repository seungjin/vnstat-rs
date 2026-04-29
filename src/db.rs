use anyhow::{Context, Result};
use chrono::Datelike;
use std::fs;
use std::path::{PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::models::{InterfaceStats, HistoryEntry, SummaryData};
use crate::utils::{get_machine_id, parse_net_dev, format_bytes};

pub enum DbType {
    Local(turso::Database),
    Sync(turso::sync::Database),
}

pub struct Db {
    pub db: DbType,
    pub conn: turso::Connection,
    pub hostname: String,
    pub machine_id: String,
    pub host_id: String,
}

const SCHEMA_SQL: &str = include_str!("../schema.sql");
const CURRENT_VERSION: i32 = 4;

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

        let mut db_obj = Self { db: db_type, conn, hostname, machine_id, host_id: String::new() };
        db_obj.init_schema().await?;
        db_obj.host_id = db_obj.get_or_create_host().await?;

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

    pub async fn execute_batch(&self, sql: &str) -> Result<()> {
        let mut clean_sql = String::new();
        for line in sql.lines() {
            let trimmed = line.trim();
            if !trimmed.is_empty() && !trimmed.starts_with("--") {
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
        let mut rows = self.conn.query("SELECT value FROM info WHERE name = 'version'", ()).await?;
        let mut version: i32 = if let Some(row) = rows.next().await? {
            row.get::<String>(0)?.parse().unwrap_or(1)
        } else {
            0
        };

        if version < CURRENT_VERSION {
            if version == 0 {
                self.execute_batch(SCHEMA_SQL).await?;
                self.conn.execute("INSERT OR IGNORE INTO info (name, value) VALUES ('version', '4')", ()).await?;
            } else {
                println!("Migrating database from version {} to {}...", version, CURRENT_VERSION);
                
                if version == 2 {
                    version = 3;
                }

                if version == 3 {
                    println!("Applying v4 migration: Converting IDs to TEXT for multi-host sync support...");
                    
                    let tables = vec!["host", "interface", "fiveminute", "hour", "day", "month", "year", "top"];
                    for table in &tables {
                        self.execute_batch(&format!("DROP TABLE IF EXISTS {}_old", table)).await?;
                        let _ = self.conn.execute(&format!("ALTER TABLE {} RENAME TO {}_old", table, table), ()).await;
                    }

                    self.execute_batch(SCHEMA_SQL).await?;

                    self.execute_batch("INSERT INTO host (id, machine_id, hostname) SELECT machine_id, machine_id, hostname FROM host_old").await?;

                    self.execute_batch("
                        INSERT INTO interface (id, host_id, name, alias, active, created, updated, rxcounter, txcounter, rxtotal, txtotal)
                        SELECT h.machine_id || ':' || i.name, h.machine_id, i.name, i.alias, i.active, i.created, i.updated, i.rxcounter, i.txcounter, i.rxtotal, i.txtotal
                        FROM interface_old i JOIN host_old h ON i.host_id = h.id
                    ").await?;

                    for table in &vec!["fiveminute", "hour", "day", "month", "year", "top"] {
                        self.execute_batch(&format!("
                            INSERT INTO {} (interface, date, rx, tx)
                            SELECT h.machine_id || ':' || i.name, t.date, t.rx, t.tx
                            FROM {}_old t
                            JOIN interface_old i ON t.interface = i.id
                            JOIN host_old h ON i.host_id = h.id
                        ", table, table)).await?;
                    }

                    for table in &tables {
                        let _ = self.conn.execute(&format!("DROP TABLE {}_old", table), ()).await;
                    }
                    
                    version = 4;
                }

                self.conn.execute(
                    "UPDATE info SET value = ? WHERE name = 'version'",
                    (version.to_string(),),
                ).await?;
                println!("Migration to version {} complete.", version);
            }
        } else {
            self.execute_batch(SCHEMA_SQL).await?;
        }

        Ok(())
    }

    pub async fn get_or_create_host(&self) -> Result<String> {
        self.conn.execute(
            "INSERT OR IGNORE INTO host (id, machine_id, hostname) VALUES (?, ?, ?)",
            (self.machine_id.clone(), self.machine_id.clone(), self.hostname.clone()),
        ).await?;

        self.conn.execute(
            "UPDATE host SET hostname = ? WHERE id = ?",
            (self.hostname.clone(), self.machine_id.clone()),
        ).await?;

        Ok(self.machine_id.clone())
    }

    pub async fn get_interface(&self, name: &str) -> Result<Option<(String, u64, u64)>> {
        let mut rows = self.conn.query(
            "SELECT id, rxcounter, txcounter FROM interface WHERE host_id = ? AND name = ?", 
            (self.host_id.clone(), name)
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
            (id.clone(), self.host_id.clone(), name, now, now, rx as i64, tx as i64),
        ).await?;

        Ok(id)
    }

    pub async fn update_interface_counters(&self, id: &str, rx: u64, tx: u64, rx_delta: u64, tx_delta: u64) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        self.conn.execute(
            "UPDATE interface SET updated = ?, rxcounter = ?, txcounter = ?, rxtotal = rxtotal + ?, txtotal = txtotal + ? WHERE id = ?",
            (now, rx as i64, tx as i64, rx_delta as i64, tx_delta as i64, id),
        ).await?;
        Ok(())
    }

    pub async fn add_traffic(&self, interface_id: &str, table: &str, date: i64, rx: u64, tx: u64) -> Result<()> {
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
                    self.update_interface_counters(&id, stat.rx_bytes, stat.tx_bytes, rx_delta, tx_delta).await?;
                    let dt = chrono::DateTime::from_timestamp(now, 0).unwrap();
                    let naive = dt.naive_utc();

                    let five_min = (now / 300) * 300;
                    self.add_traffic(&id, "fiveminute", five_min, rx_delta, tx_delta).await?;

                    let hour = (now / 3600) * 3600;
                    self.add_traffic(&id, "hour", hour, rx_delta, tx_delta).await?;

                    let day_dt = naive.date().and_hms_opt(0, 0, 0).unwrap();
                    let day = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(day_dt, chrono::Utc).timestamp();
                    self.add_traffic(&id, "day", day, rx_delta, tx_delta).await?;

                    let month_dt = naive.date().with_day(1).unwrap().and_hms_opt(0, 0, 0).unwrap();
                    let month = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(month_dt, chrono::Utc).timestamp();
                    self.add_traffic(&id, "month", month, rx_delta, tx_delta).await?;

                    let year_dt = naive.date().with_day(1).unwrap().with_month(1).unwrap().and_hms_opt(0, 0, 0).unwrap();
                    let year = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(year_dt, chrono::Utc).timestamp();
                    self.add_traffic(&id, "year", year, rx_delta, tx_delta).await?;

                    self.add_traffic(&id, "top", day, rx_delta, tx_delta).await?;
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
