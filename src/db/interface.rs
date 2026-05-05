use anyhow::{Result};
use crate::db::Db;
use std::time::{SystemTime, UNIX_EPOCH};

impl Db {
    pub async fn get_interface(&self, name: &str) -> Result<Option<(i64, u64, u64, Option<String>, i64)>> {
        if name == "lo" {
            return Ok(None);
        }
        let mut rows = self.local_conn.query(
            "SELECT id, rxcounter, txcounter, mac_address, updated FROM interface WHERE host_id = ? AND name = ?", 
            [self.host_id.clone(), name.to_string()]
        ).await?;
        
        if let Some(row) = rows.next().await? {
            let id: i64 = row.get(0)?;
            let rx: i64 = row.get(1)?;
            let tx: i64 = row.get(2)?;
            let mac: Option<String> = row.get(3)?;
            let updated: i64 = row.get(4)?;
            
            return Ok(Some((
                id, 
                rx as u64, 
                tx as u64,
                mac,
                updated
            )));
        }
        Ok(None)
    }

    pub async fn create_interface(&self, name: &str, rx: u64, tx: u64, mac: Option<String>) -> Result<i64> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        let sql = "INSERT OR IGNORE INTO interface (host_id, name, mac_address, created, updated, rxcounter, txcounter) VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        self.local_conn.execute(sql, (self.host_id.clone(), name.to_string(), mac.clone(), now, now, rx as i64, tx as i64)).await?;
        let id = self.local_conn.last_insert_rowid();

        if let Some(ref remote) = self.remote_conn {
            // On remote, we might not get the same ID, but that's okay for now as we use names to sync usually
            // but for a strict ID sync we'd need to handle it. For now let's just create it.
            let _ = remote.execute(sql, (self.host_id.clone(), name.to_string(), mac, now, now, rx as i64, tx as i64)).await;
        }

        Ok(id)
    }

    pub async fn update_interface_counters(&self, id: i64, rx: u64, tx: u64, rx_delta: u64, tx_delta: u64) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        let sql = "UPDATE interface SET updated = ?, rxcounter = ?, txcounter = ?, rxtotal = rxtotal + ?, txtotal = txtotal + ? WHERE id = ?";
        
        self.local_conn.execute(sql, (now, rx as i64, tx as i64, rx_delta as i64, tx_delta as i64, id)).await?;
        if let Some(ref remote) = self.remote_conn {
            if let Err(e) = remote.execute(sql, (now, rx as i64, tx as i64, rx_delta as i64, tx_delta as i64, id)).await {
                eprintln!("Warning: Failed to update interface counters on remote: {}", e);
            }
        }
        Ok(())
    }

    pub async fn update_interface_mac(&self, id: i64, mac: &str) -> Result<()> {
        let sql = "UPDATE interface SET mac_address = ? WHERE id = ?";
        self.local_conn.execute(sql, (mac.to_string(), id)).await?;
        if let Some(ref remote) = self.remote_conn {
            if let Err(e) = remote.execute(sql, (mac.to_string(), id)).await {
                eprintln!("Warning: Failed to update interface MAC on remote: {}", e);
            }
        }
        Ok(())
    }

    pub async fn update_interface_alias(&self, id: i64, alias: &str) -> Result<()> {
        let sql = "UPDATE interface SET alias = ? WHERE id = ?";
        self.local_conn.execute(sql, (alias.to_string(), id)).await?;
        if let Some(ref remote) = self.remote_conn {
            if let Err(e) = remote.execute(sql, (alias.to_string(), id)).await {
                eprintln!("Warning: Failed to update interface alias on remote: {}", e);
            }
        }
        Ok(())
    }

    pub async fn set_interface_active(&self, id: i64, active: bool) -> Result<()> {
        let sql = "UPDATE interface SET active = ? WHERE id = ?";
        let active_val = if active { 1 } else { 0 };
        self.local_conn.execute(sql, (active_val, id)).await?;
        if let Some(ref remote) = self.remote_conn {
            if let Err(e) = remote.execute(sql, (active_val, id)).await {
                eprintln!("Warning: Failed to set interface active status on remote: {}", e);
            }
        }
        Ok(())
    }

    pub async fn remove_interface(&self, name: &str) -> Result<()> {
        if let Some((id, _, _, _, _)) = self.get_interface(name).await? {
            // Delete traffic data first (cascading delete would be better, but let's be explicit)
            let tables = ["fiveminute", "hour", "day", "month", "year", "top"];
            for table in tables {
                let sql = format!("DELETE FROM {} WHERE interface = ?", table);
                self.local_conn.execute(&sql, [id]).await?;
                if let Some(ref remote) = self.remote_conn {
                    let _ = remote.execute(&sql, [id]).await;
                }
            }

            let sql = "DELETE FROM interface WHERE id = ?";
            self.local_conn.execute(sql, [id]).await?;
            if let Some(ref remote) = self.remote_conn {
                if let Err(e) = remote.execute(sql, [id]).await {
                    eprintln!("Warning: Failed to remove interface on remote: {}", e);
                }
            }
            Ok(())
        } else {
            Err(anyhow::anyhow!("Interface \"{}\" not found for host \"{}\"", name, self.hostname))
        }
    }

    pub async fn rename_interface(&self, old_name: &str, new_name: &str) -> Result<()> {
        if let Some((id, _, _, _, _)) = self.get_interface(old_name).await? {
            let sql = "UPDATE interface SET name = ? WHERE id = ?";
            self.local_conn.execute(sql, (new_name.to_string(), id)).await?;
            if let Some(ref remote) = self.remote_conn {
                if let Err(e) = remote.execute(sql, (new_name.to_string(), id)).await {
                    eprintln!("Warning: Failed to rename interface on remote: {}", e);
                }
            }
            Ok(())
        } else {
            Err(anyhow::anyhow!("Interface \"{}\" not found for host \"{}\"", old_name, self.hostname))
        }
    }
}
