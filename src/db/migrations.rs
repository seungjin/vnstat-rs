use anyhow::{Result};
use serde::Deserialize;
use libsql::params;
use crate::db::Db;

#[derive(Deserialize)]
pub struct Schema {
    pub version: i64,
    pub sql: String,
    pub migrations: Option<Vec<MigrationEntry>>,
}

#[derive(Deserialize)]
pub struct MigrationEntry {
    pub version: i64,
    pub sql: String,
}

pub const SCHEMA_TOML: &str = include_str!("../../schema.sql.toml");

impl Db {
    pub async fn get_schema_version_from(&self, conn: &libsql::Connection) -> Result<i64> {
        let _ = conn.execute("CREATE TABLE IF NOT EXISTS info (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL, value TEXT NOT NULL)", params![]).await;
        
        let mut rows = conn.query("SELECT value FROM info WHERE name = ?", ["schema_version"]).await?;
        if let Some(row) = rows.next().await? {
            let v: String = row.get(0)?;
            return Ok(v.parse().unwrap_or(0));
        }

        // Check old 'version' key
        let mut rows = conn.query("SELECT value FROM info WHERE name = ?", ["version"]).await?;
        if let Some(row) = rows.next().await? {
            let v: String = row.get(0)?;
            let ver = v.parse::<i64>().unwrap_or(0);
            if ver > 0 && ver < 10000 {
                return Ok(0); 
            }
            return Ok(ver);
        }

        Ok(0)
    }

    pub async fn init_schema(&self) -> Result<()> {
        let schema: Schema = toml::from_str(SCHEMA_TOML)?;
        
        // 1. Initial table creation (both)
        self.execute_batch(&schema.sql).await?;

        // 2. Handle Local Migrations
        let current_local = self.get_schema_version_from(&self.local_conn).await?;
        if current_local == 0 {
            println!("Initializing fresh local database schema (v{})...", schema.version);
            let _ = self.set_info_local("schema_version", &schema.version.to_string()).await;
        } else if current_local < schema.version {
            println!("Migrating local database from v{} to v{}...", current_local, schema.version);
            if let Some(ref migrations) = schema.migrations {
                for m in migrations {
                    if m.version > current_local && m.version <= schema.version {
                        println!("Applying local migration v{}...", m.version);
                        let _ = self.local_conn.execute_batch(&m.sql).await;
                    }
                }
            }
            let _ = self.set_info_local("schema_version", &schema.version.to_string()).await;
        }

        // 3. Handle Remote Migrations (Independent of Local)
        if let Some(ref remote) = self.remote_conn {
            let current_remote = self.get_schema_version_from(remote).await?;
            if current_remote == 0 {
                println!("Initializing fresh remote database schema (v{})...", schema.version);
                let _ = remote.execute(&format!("INSERT INTO info (name, value) VALUES ('schema_version', '{}') ON CONFLICT(name) DO UPDATE SET value = excluded.value", schema.version), params![]).await;
            } else if current_remote < schema.version {
                println!("Migrating remote database from v{} to v{}...", current_remote, schema.version);
                if let Some(ref migrations) = schema.migrations {
                    for m in migrations {
                        if m.version > current_remote && m.version <= schema.version {
                            println!("Applying remote migration v{}...", m.version);
                            if let Err(e) = remote.execute_batch(&m.sql).await {
                                eprintln!("Warning: Remote migration v{} failed: {}", m.version, e);
                            }
                        }
                    }
                }
                let _ = remote.execute(&format!("INSERT INTO info (name, value) VALUES ('schema_version', '{}') ON CONFLICT(name) DO UPDATE SET value = excluded.value", schema.version), params![]).await;
            }
        }

        Ok(())
    }
}
