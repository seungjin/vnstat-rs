use anyhow::{Context, Result};
use std::fs;
use std::path::{PathBuf};
use crate::utils::{get_machine_id};
use libsql::{Builder, Connection};

pub mod migrations;
pub mod host;
pub mod interface;
pub mod stats;

pub struct Db {
    pub local_conn: Connection,
    pub remote_conn: Option<Connection>,
    pub hostname: String,
    pub machine_id: String,
    pub host_id: String,
}

impl Db {
    pub async fn open(path: PathBuf, url: Option<String>, token: Option<String>, hostname_override: Option<String>) -> Result<Self> {
        // 1. Always open local database
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                println!("Creating database directory {}...", parent.display());
                fs::create_dir_all(parent).context("Failed to create database directory")?;
            }
        }
        let path_str = path.to_string_lossy().to_string();
        let local_db = Builder::new_local(path_str).build().await?;
        let local_conn = local_db.connect()?;

        // 2. Optionally open remote database
        let remote_conn = if let (Some(url), Some(token)) = (url, token) {
            println!("Connecting to remote database at {}...", url);
            match Builder::new_remote(url, token).build().await {
                Ok(remote_db) => match remote_db.connect() {
                    Ok(conn) => Some(conn),
                    Err(e) => {
                        eprintln!("Warning: Failed to connect to remote database: {}", e);
                        None
                    }
                },
                Err(e) => {
                    eprintln!("Warning: Failed to initialize remote database client: {}", e);
                    None
                }
            }
        } else {
            None
        };

        let hostname = hostname_override.unwrap_or_else(|| {
            hostname::get().ok().and_then(|h| h.into_string().ok()).unwrap_or_else(|| "local".to_string())
        });
        let machine_id = get_machine_id()?;

        let mut db_obj = Self { 
            local_conn, 
            remote_conn, 
            hostname, 
            machine_id, 
            host_id: String::new() 
        };

        db_obj.init_schema().await?;
        db_obj.host_id = db_obj.get_or_create_host().await?;

        Ok(db_obj)
    }

    pub async fn sync(&self) -> Result<()> {
        // In direct mode, synchronization is handled by dual-writing
        Ok(())
    }

    pub async fn execute_batch(&self, sql: &str) -> Result<()> {
        self.local_conn.execute_batch(sql).await?;
        if let Some(ref remote) = self.remote_conn {
            let _ = remote.execute_batch(sql).await;
        }
        Ok(())
    }

    pub async fn get_info(&self, name: &str) -> Result<Option<String>> {
        let mut rows = self.local_conn.query("SELECT value FROM info WHERE name = ?", [name]).await?;
        if let Some(row) = rows.next().await? {
            return Ok(Some(row.get(0)?));
        }
        Ok(None)
    }

    pub async fn set_info(&self, name: &str, value: &str) -> Result<()> {
        let sql = "INSERT INTO info (name, value) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET value = excluded.value";
        self.local_conn.execute(sql, [name, value]).await?;
        if let Some(ref remote) = self.remote_conn {
            let _ = remote.execute(sql, [name, value]).await;
        }
        Ok(())
    }

    pub async fn set_info_local(&self, name: &str, value: &str) -> Result<()> {
        let sql = "INSERT INTO info (name, value) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET value = excluded.value";
        self.local_conn.execute(sql, [name, value]).await?;
        Ok(())
    }
}
