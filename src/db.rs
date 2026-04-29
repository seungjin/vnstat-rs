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
    migrations: Option<Vec<MigrationEntry>>,
}

#[derive(Deserialize)]
struct MigrationEntry {
    version: i64,
    sql: String,
}

const SCHEMA_TOML: &str = include_str!("../schema.sql.toml");

impl Db {
    pub async fn open(path: PathBuf, url: Option<String>, token: Option<String>, hostname_override: Option<String>) -> Result<Self> {
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
        let hostname = hostname_override.unwrap_or_else(|| {
            hostname::get().ok().and_then(|h| h.into_string().ok()).unwrap_or_else(|| "local".to_string())
        });
        let machine_id = get_machine_id()?;

        let mut db_obj = Self { db: db_type, conn, hostname, machine_id, host_id: String::new() };
        db_obj.init_schema().await?;
        db_obj.host_id = db_obj.get_or_create_host().await?;

        Ok(db_obj)
    }

    pub async fn sync(&self) -> Result<()> {
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

    pub async fn get_schema_version(&self) -> Result<i64> {
        self.conn.execute("CREATE TABLE IF NOT EXISTS info (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL, value TEXT NOT NULL)", params![]).await?;
        
        if let Some(v) = self.get_info("schema_version").await? {
            return Ok(v.parse().unwrap_or(0));
        }

        if let Some(v) = self.get_info("version").await? {
            let ver = v.parse::<i64>().unwrap_or(0);
            if ver > 0 && ver < 10000 {
                return Ok(0); 
            }
            return Ok(ver);
        }

        Ok(0)
    }

    pub async fn init_schema(&self) -> Result<()> {
        let schema: Schema = toml::from_str(SCHEMA_TOML).context("Failed to parse schema.sql.toml")?;
        let current = self.get_schema_version().await?;

        if current == 0 {
            println!("Initializing fresh database schema (v{})...", schema.version);
            self.execute_batch(&schema.sql).await?;
            self.set_info("schema_version", &schema.version.to_string()).await?;
        } else if current < schema.version {
            println!("Migrating database from v{} to v{}...", current, schema.version);
            
            if let Some(migrations) = schema.migrations {
                for m in migrations {
                    if m.version > current && m.version <= schema.version {
                        println!("Applying migration v{}...", m.version);
                        let _ = self.execute_batch(&m.sql).await;
                    }
                }
            }
            
            self.set_info("schema_version", &schema.version.to_string()).await?;
        }

        Ok(())
    }

    pub async fn get_or_create_host(&self) -> Result<String> {
        let mac = pnet_datalink::interfaces().iter()
            .find(|iface| iface.name != "lo" && iface.mac.is_some())
            .and_then(|iface| iface.mac)
            .map(|m| m.to_string());

        self.conn.execute(
            "INSERT OR IGNORE INTO host (id, machine_id, hostname, mac_address) VALUES (?, ?, ?, ?)",
            (self.machine_id.clone(), self.machine_id.clone(), self.hostname.clone(), mac.clone()),
        ).await?;

        self.conn.execute(
            "UPDATE host SET hostname = ?, mac_address = ? WHERE id = ?",
            (self.hostname.clone(), mac, self.machine_id.clone()),
        ).await?;

        Ok(self.machine_id.clone())
    }

    pub async fn get_interface(&self, name: &str) -> Result<Option<(String, u64, u64, Option<String>)>> {
        let mut rows = self.conn.query(
            "SELECT id, rxcounter, txcounter, mac_address FROM interface WHERE host_id = ? AND name = ?", 
            [self.host_id.clone(), name.to_string()]
        ).await?;
        
        if let Some(row) = rows.next().await? {
            return Ok(Some((
                row.get(0)?, 
                row.get::<i64>(1)? as u64, 
                row.get::<i64>(2)? as u64,
                row.get(3)?
            )));
        }
        Ok(None)
    }

    pub async fn create_interface(&self, name: &str, rx: u64, tx: u64, mac: Option<String>) -> Result<String> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        let id = format!("{}:{}", self.host_id, name);
        self.conn.execute(
            "INSERT INTO interface (id, host_id, name, mac_address, created, updated, rxcounter, txcounter) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (id.clone(), self.host_id.clone(), name.to_string(), mac, now, now, rx as i64, tx as i64),
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

    pub async fn update_interface_mac(&self, id: &str, mac: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE interface SET mac_address = ? WHERE id = ?",
            (mac.to_string(), id.to_string()),
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
            if let Some((id, last_rx, last_tx, current_mac)) = self.get_interface(&stat.name).await? {
                if current_mac.is_none() || current_mac.as_ref().map(|m| m.is_empty()).unwrap_or(true) {
                    if let Some(ref new_mac) = stat.mac_address {
                        let _ = self.update_interface_mac(&id, new_mac).await;
                    }
                }

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
                let id = self.create_interface(&stat.name, stat.rx_bytes, stat.tx_bytes, stat.mac_address).await?;
                // Add initial 0 delta to ensure history record is created for today
                self.add_history_entry(&id, 0, 0).await?;
                println!("New interface found: {} (host: {})", stat.name, self.hostname);
            }
        }
        Ok(())
    }

    pub async fn get_all_hosts(&self) -> Result<Vec<(String, String)>> {
        let mut rows = self.conn.query("SELECT hostname, machine_id FROM host ORDER BY hostname", params![]).await?;
        let mut hosts = Vec::new();
        while let Some(row) = rows.next().await? {
            hosts.push((row.get(0)?, row.get(1)?));
        }
        Ok(hosts)
    }

    pub async fn get_all_interface_stats(&self, filter_iface: Option<&str>, filter_host: Option<&str>) -> Result<Vec<InterfaceStats>> {
        let mut query_str = "SELECT i.name, i.alias, i.mac_address, i.rxtotal, i.txtotal, h.hostname, i.created, i.updated 
                         FROM interface i 
                         JOIN host h ON i.host_id = h.id WHERE 1=1 ".to_string();
        
        if let Some(iface) = filter_iface {
            query_str.push_str(&format!(" AND i.name = '{}' ", iface));
        }

        if let Some(host) = filter_host {
            query_str.push_str(&format!(" AND (h.hostname = '{}' OR h.machine_id = '{}') ", host, host));
        }
        
        let mut rows = self.conn.query(&query_str, params![]).await?;
        let mut stats = Vec::new();
        while let Some(row) = rows.next().await? {
            stats.push(InterfaceStats {
                name: row.get(0)?,
                alias: row.get(1)?,
                mac_address: row.get(2)?,
                rx_bytes: row.get::<i64>(3)? as u64,
                tx_bytes: row.get::<i64>(4)? as u64,
                rx_packets: 0,
                tx_packets: 0,
                hostname: row.get(5)?,
                created: row.get(6)?,
                updated: row.get(7)?,
            });
        }
        Ok(stats)
    }

    pub async fn get_history(&self, table: &str, filter_iface: Option<&str>, filter_host: Option<&str>, limit: usize, begin: Option<i64>, end: Option<i64>) -> Result<Vec<HistoryEntry>> {
        let mut query_str = format!(
            "SELECT h.hostname, i.name, t.date, t.rx, t.tx 
             FROM interface i 
             JOIN host h ON i.host_id = h.id
             JOIN {} t ON i.id = t.interface WHERE 1=1 ", table);
        
        if let Some(iface) = filter_iface {
            query_str.push_str(&format!("AND i.name = '{}' ", iface));
        }

        if let Some(host) = filter_host {
            query_str.push_str(&format!(" AND (h.hostname = '{}' OR h.machine_id = '{}') ", host, host));
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

        let mut rows = self.conn.query(&query_str, params![]).await?;
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

    pub async fn get_summary(&self, filter_iface: Option<&str>, filter_host: Option<&str>) -> Result<Vec<SummaryData>> {
        let mut ifaces_query = "SELECT i.id, i.name, h.hostname FROM interface i JOIN host h ON i.host_id = h.id WHERE 1=1 ".to_string();
        
        if let Some(iface) = filter_iface {
            ifaces_query.push_str(&format!(" AND i.name = '{}'", iface));
        }

        if let Some(host) = filter_host {
            ifaces_query.push_str(&format!(" AND (h.hostname = '{}' OR h.machine_id = '{}')", host, host));
        }

        ifaces_query.push_str(" ORDER BY h.hostname, i.name");

        let mut iface_rows = self.conn.query(&ifaces_query, params![]).await?;
        let mut interfaces = Vec::new();
        while let Some(row) = iface_rows.next().await? {
            interfaces.push((row.get::<String>(0)?, row.get::<String>(1)?, row.get::<String>(2)?));
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

        for (id, name, hostname) in interfaces {
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
                hostname,
                today: stats.get(&format!("d_{}", today_ts)).cloned().unwrap_or((0, 0)),
                yesterday: stats.get(&format!("d_{}", yesterday_ts)).cloned().unwrap_or((0, 0)),
                this_month: stats.get(&format!("m_{}", this_month_ts)).cloned().unwrap_or((0, 0)),
                last_month: stats.get(&format!("m_{}", last_month_ts)).cloned().unwrap_or((0, 0)),
            });
        }
        Ok(summaries)
    }
}
