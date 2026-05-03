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
        let insert_sql = "INSERT OR IGNORE INTO host (id, machine_id, hostname, mac_address, version) VALUES (?, ?, ?, ?, ?)";
        let update_sql = "UPDATE host SET hostname = ?, mac_address = ?, version = ? WHERE id = ?";

        self.local_conn.execute(insert_sql, (self.machine_id.clone(), self.machine_id.clone(), self.hostname.clone(), mac.clone(), version.clone())).await?;
        self.local_conn.execute(update_sql, (self.hostname.clone(), mac.clone(), version.clone(), self.machine_id.clone())).await?;

        if let Some(ref remote) = self.remote_conn {
            let _ = remote.execute(insert_sql, (self.machine_id.clone(), self.machine_id.clone(), self.hostname.clone(), mac.clone(), version.clone())).await;
            let _ = remote.execute(update_sql, (self.hostname.clone(), mac, version, self.machine_id.clone())).await;
        }

        Ok(self.machine_id.clone())
    }

    pub async fn get_all_hosts(&self) -> Result<Vec<(String, String)>> {
        let conn = self.remote_conn.as_ref().unwrap_or(&self.local_conn);
        let mut rows = conn.query("SELECT hostname, machine_id FROM host ORDER BY hostname", params![]).await?;
        let mut hosts = Vec::new();
        while let Some(row) = rows.next().await? {
            hosts.push((row.get(0)?, row.get(1)?));
        }
        Ok(hosts)
    }
}
