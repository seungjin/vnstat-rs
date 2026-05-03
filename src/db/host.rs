use anyhow::{Result};
use crate::db::Db;
use libsql::params;

impl Db {
    pub async fn get_or_create_host(&self) -> Result<String> {
        let mac = pnet_datalink::interfaces().iter()
            .find(|iface| iface.name != "lo" && iface.mac.is_some())
            .and_then(|iface| iface.mac)
            .map(|m| m.to_string());

        let version = format!("{} ({})", env!("CARGO_PKG_VERSION"), env!("GIT_HASH"));
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;
        
        let insert_sql = "INSERT OR IGNORE INTO host (id, machine_id, hostname, mac_address, version, started) VALUES (?, ?, ?, ?, ?, ?)";
        let update_sql = "UPDATE host SET hostname = ?, mac_address = ?, version = ?, started = ? WHERE id = ?";

        self.local_conn.execute(insert_sql, (self.machine_id.clone(), self.machine_id.clone(), self.hostname.clone(), mac.clone(), version.clone(), now)).await?;
        self.local_conn.execute(update_sql, (self.hostname.clone(), mac.clone(), version.clone(), now, self.machine_id.clone())).await?;

        if let Some(ref remote) = self.remote_conn {
            let _ = remote.execute(insert_sql, (self.machine_id.clone(), self.machine_id.clone(), self.hostname.clone(), mac.clone(), version.clone(), now)).await;
            let _ = remote.execute(update_sql, (self.hostname.clone(), mac, version, now, self.machine_id.clone())).await;
        }

        Ok(self.machine_id.clone())
    }

    pub async fn get_all_hosts(&self) -> Result<Vec<(String, String, Option<String>, Option<i64>)>> {
        let conn = self.remote_conn.as_ref().unwrap_or(&self.local_conn);
        let mut rows = conn.query("SELECT hostname, machine_id, version, started FROM host ORDER BY hostname", params![]).await?;
        let mut hosts = Vec::new();
        while let Some(row) = rows.next().await? {
            hosts.push((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?));
        }
        Ok(hosts)
    }
}
