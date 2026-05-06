use anyhow::{Result};
use crate::db::Db;
use std::time::{SystemTime, UNIX_EPOCH};

impl Db {
    pub async fn get_interface(&self, name: &str) -> Result<Option<(i64, u64, u64, Option<String>, i64, i64, u64, u64)>> {
        if name == "lo" {
            return Ok(None);
        }
        let mut rows = self.local_conn.query(
            "SELECT id, rxcounter, txcounter, mac_address, updated, created, rxtotal, txtotal FROM interface WHERE host_id = ? AND name = ?", 
            (self.host_id, name.to_string())
        ).await?;
        
        if let Some(row) = rows.next().await? {
            let id: i64 = row.get(0)?;
            let rx: i64 = row.get(1)?;
            let tx: i64 = row.get(2)?;
            let mac: Option<String> = row.get(3)?;
            let updated: i64 = row.get(4)?;
            let created: i64 = row.get(5)?;
            let rxtotal: i64 = row.get(6)?;
            let txtotal: i64 = row.get(7)?;
            
            return Ok(Some((
                id, 
                rx as u64, 
                tx as u64,
                mac,
                updated,
                created,
                rxtotal as u64,
                txtotal as u64
            )));
        }
        Ok(None)
    }

    pub async fn create_interface(&self, name: &str, rx: u64, tx: u64, mac: Option<String>) -> Result<i64> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        let local_sql = "INSERT OR IGNORE INTO interface (host_id, name, mac_address, created, updated, rxcounter, txcounter) VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        self.local_conn.execute(local_sql, (self.host_id, name.to_string(), mac.clone(), now, now, rx as i64, tx as i64)).await?;
        let id = self.local_conn.last_insert_rowid();

        if let Some(ref remote) = self.remote_conn {
            let remote_sql = "INSERT OR IGNORE INTO interface (host_id, name, mac_address, created, updated, rxcounter, txcounter) 
                              SELECT id, ?, ?, ?, ?, ?, ? FROM host WHERE machine_id = ?";
            if let Err(e) = remote.execute(remote_sql, (name.to_string(), mac, now, now, rx as i64, tx as i64, self.machine_id.clone())).await {
                eprintln!("Warning: Failed to create interface on remote: {}", e);
            }
        }

        Ok(id)
    }

    pub async fn update_interface_counters(&self, id: i64, name: &str, rx: u64, tx: u64, rx_delta: u64, tx_delta: u64, rxtotal: u64, txtotal: u64, created: i64, mac: Option<String>) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        let local_sql = "UPDATE interface SET updated = ?, rxcounter = ?, txcounter = ?, rxtotal = rxtotal + ?, txtotal = txtotal + ? WHERE id = ?";
        
        self.local_conn.execute(local_sql, (now, rx as i64, tx as i64, rx_delta as i64, tx_delta as i64, id)).await?;
        if let Some(ref remote) = self.remote_conn {
            let remote_sql = "INSERT INTO interface (host_id, name, mac_address, created, updated, rxcounter, txcounter, rxtotal, txtotal)
                              SELECT id, ?, ?, ?, ?, ?, ?, ?, ? FROM host WHERE machine_id = ?
                              ON CONFLICT(host_id, name) DO UPDATE SET
                                updated = excluded.updated,
                                rxcounter = excluded.rxcounter,
                                txcounter = excluded.txcounter,
                                rxtotal = rxtotal + ?,
                                txtotal = txtotal + ?";
            if let Err(e) = remote.execute(remote_sql, (
                name.to_string(), mac, created, now, rx as i64, tx as i64, rxtotal as i64, txtotal as i64, self.machine_id.clone(),
                rx_delta as i64, tx_delta as i64
            )).await {
                eprintln!("Warning: Failed to update interface counters on remote: {}", e);
            }
        }
        Ok(())
    }

    pub async fn update_interface_mac(&self, id: i64, name: &str, mac: &str) -> Result<()> {
        let local_sql = "UPDATE interface SET mac_address = ? WHERE id = ?";
        self.local_conn.execute(local_sql, (mac.to_string(), id)).await?;
        if let Some(ref remote) = self.remote_conn {
            let remote_sql = "UPDATE interface SET mac_address = ? WHERE name = ? AND host_id = (SELECT id FROM host WHERE machine_id = ?)";
            if let Err(e) = remote.execute(remote_sql, (mac.to_string(), name.to_string(), self.machine_id.clone())).await {
                eprintln!("Warning: Failed to update interface MAC on remote: {}", e);
            }
        }
        Ok(())
    }

    pub async fn update_interface_alias(&self, id: i64, name: &str, alias: &str) -> Result<()> {
        let local_sql = "UPDATE interface SET alias = ? WHERE id = ?";
        self.local_conn.execute(local_sql, (alias.to_string(), id)).await?;
        if let Some(ref remote) = self.remote_conn {
            let remote_sql = "UPDATE interface SET alias = ? WHERE name = ? AND host_id = (SELECT id FROM host WHERE machine_id = ?)";
            if let Err(e) = remote.execute(remote_sql, (alias.to_string(), name.to_string(), self.machine_id.clone())).await {
                eprintln!("Warning: Failed to update interface alias on remote: {}", e);
            }
        }
        Ok(())
    }

    pub async fn set_interface_active(&self, id: i64, name: &str, active: bool) -> Result<()> {
        let local_sql = "UPDATE interface SET active = ? WHERE id = ?";
        let active_val = if active { 1 } else { 0 };
        self.local_conn.execute(local_sql, (active_val, id)).await?;
        if let Some(ref remote) = self.remote_conn {
            let remote_sql = "UPDATE interface SET active = ? WHERE name = ? AND host_id = (SELECT id FROM host WHERE machine_id = ?)";
            if let Err(e) = remote.execute(remote_sql, (active_val, name.to_string(), self.machine_id.clone())).await {
                eprintln!("Warning: Failed to set interface active status on remote: {}", e);
            }
        }
        Ok(())
    }

    pub async fn remove_interface(&self, name: &str) -> Result<()> {
        if let Some((id, _, _, _, _, _, _, _)) = self.get_interface(name).await? {
            // Delete traffic data first
            let tables = ["fiveminute", "hour", "day", "month", "year", "top"];
            for table in tables {
                let local_sql = format!("DELETE FROM {} WHERE interface = ?", table);
                self.local_conn.execute(&local_sql, [id]).await?;
                if let Some(ref remote) = self.remote_conn {
                    let remote_sql = format!(
                        "DELETE FROM {table} WHERE interface = (SELECT id FROM interface WHERE name = ? AND host_id = (SELECT id FROM host WHERE machine_id = ?))"
                    );
                    let _ = remote.execute(&remote_sql, (name.to_string(), self.machine_id.clone())).await;
                }
            }

            let local_sql = "DELETE FROM interface WHERE id = ?";
            self.local_conn.execute(local_sql, [id]).await?;
            if let Some(ref remote) = self.remote_conn {
                let remote_sql = "DELETE FROM interface WHERE name = ? AND host_id = (SELECT id FROM host WHERE machine_id = ?)";
                if let Err(e) = remote.execute(remote_sql, (name.to_string(), self.machine_id.clone())).await {
                    eprintln!("Warning: Failed to remove interface on remote: {}", e);
                }
            }
            Ok(())
        } else {
            Err(anyhow::anyhow!("Interface \"{}\" not found for host \"{}\"", name, self.hostname))
        }
    }

    pub async fn rename_interface(&self, old_name: &str, new_name: &str) -> Result<()> {
        if let Some((id, _, _, _, _, _, _, _)) = self.get_interface(old_name).await? {
            let local_sql = "UPDATE interface SET name = ? WHERE id = ?";
            self.local_conn.execute(local_sql, (new_name.to_string(), id)).await?;
            if let Some(ref remote) = self.remote_conn {
                let remote_sql = "UPDATE interface SET name = ? WHERE name = ? AND host_id = (SELECT id FROM host WHERE machine_id = ?)";
                if let Err(e) = remote.execute(remote_sql, (new_name.to_string(), old_name.to_string(), self.machine_id.clone())).await {
                    eprintln!("Warning: Failed to rename interface on remote: {}", e);
                }
            }
            Ok(())
        } else {
            Err(anyhow::anyhow!("Interface \"{}\" not found for host \"{}\"", old_name, self.hostname))
        }
    }
}
