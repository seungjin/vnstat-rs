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
    pub async fn get_schema_version(&self) -> Result<i64> {
        self.local_conn.execute("CREATE TABLE IF NOT EXISTS info (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL, value TEXT NOT NULL)", params![]).await?;
        
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
        let schema: Schema = toml::from_str(SCHEMA_TOML)?;
        
        // 1. Initial table creation (both)
        self.execute_batch(&schema.sql).await?;

        // 2. Handle migrations
        let current = self.get_schema_version().await?;

        if current == 0 {
            println!("Initializing fresh database schema (v{})...", schema.version);
            self.set_info("schema_version", &schema.version.to_string()).await?;
        } else if current < schema.version {
            println!("Migrating database from v{} to v{}...", current, schema.version);
            
            if let Some(migrations) = schema.migrations {
                for m in migrations {
                    if m.version > current && m.version <= schema.version {
                        println!("Applying migration v{}...", m.version);
                        // execute_batch applies to both local and remote
                        if let Err(e) = self.execute_batch(&m.sql).await {
                            eprintln!("Warning: Migration v{} failed: {}", m.version, e);
                        }
                    }
                }
            }
            
            self.set_info("schema_version", &schema.version.to_string()).await?;
        }

        Ok(())
    }
}
