use anyhow::{Result};
use crate::db::Db;
use std::time::{SystemTime, UNIX_EPOCH};

impl Db {
    pub async fn get_interface(&self, name: &str) -> Result<Option<(String, u64, u64, Option<String>)>> {
        if name == "lo" {
            return Ok(None);
        }
        let mut rows = self.local_conn.query(
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
        let sql = "INSERT INTO interface (id, host_id, name, mac_address, created, updated, rxcounter, txcounter) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        
        self.local_conn.execute(sql, (id.clone(), self.host_id.clone(), name.to_string(), mac.clone(), now, now, rx as i64, tx as i64)).await?;
        if let Some(ref remote) = self.remote_conn {
            let _ = remote.execute(sql, (id.clone(), self.host_id.clone(), name.to_string(), mac, now, now, rx as i64, tx as i64)).await;
        }

        Ok(id)
    }

    pub async fn update_interface_counters(&self, id: &str, rx: u64, tx: u64, rx_delta: u64, tx_delta: u64) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        let sql = "UPDATE interface SET updated = ?, rxcounter = ?, txcounter = ?, rxtotal = rxtotal + ?, txtotal = txtotal + ? WHERE id = ?";
        
        self.local_conn.execute(sql, (now, rx as i64, tx as i64, rx_delta as i64, tx_delta as i64, id.to_string())).await?;
        if let Some(ref remote) = self.remote_conn {
            let _ = remote.execute(sql, (now, rx as i64, tx as i64, rx_delta as i64, tx_delta as i64, id.to_string())).await;
        }
        Ok(())
    }

    pub async fn update_interface_mac(&self, id: &str, mac: &str) -> Result<()> {
        let sql = "UPDATE interface SET mac_address = ? WHERE id = ?";
        self.local_conn.execute(sql, (mac.to_string(), id.to_string())).await?;
        if let Some(ref remote) = self.remote_conn {
            let _ = remote.execute(sql, (mac.to_string(), id.to_string())).await;
        }
        Ok(())
    }

    pub async fn set_interface_active(&self, id: &str, active: bool) -> Result<()> {
        let sql = "UPDATE interface SET active = ? WHERE id = ?";
        let active_val = if active { 1 } else { 0 };
        self.local_conn.execute(sql, (active_val, id.to_string())).await?;
        if let Some(ref remote) = self.remote_conn {
            let _ = remote.execute(sql, (active_val, id.to_string())).await;
        }
        Ok(())
    }
}
