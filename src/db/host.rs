use anyhow::{Result};
use crate::db::Db;

impl Db {
    pub async fn get_or_create_host(&self) -> Result<String> {
        let mac = pnet_datalink::interfaces().iter()
            .find(|iface| iface.name != "lo" && iface.mac.is_some())
            .and_then(|iface| iface.mac)
            .map(|m| m.to_string());

        let version = format!("{} ({})", env!("CARGO_PKG_VERSION"), env!("GIT_HASH"));
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;
        
        let insert_sql = "INSERT OR IGNORE INTO host (id, machine_id, hostname, mac_address, version, started, last_seen) VALUES (?, ?, ?, ?, ?, ?, ?)";
        let update_sql = "UPDATE host SET hostname = ?, mac_address = ?, version = ?, started = ?, last_seen = ? WHERE id = ?";

        self.local_conn.execute(insert_sql, (self.machine_id.clone(), self.machine_id.clone(), self.hostname.clone(), mac.clone(), version.clone(), now, now)).await?;
        self.local_conn.execute(update_sql, (self.hostname.clone(), mac.clone(), version.clone(), now, now, self.machine_id.clone())).await?;

        if let Some(ref remote) = self.remote_conn {
            let _ = remote.execute(insert_sql, (self.machine_id.clone(), self.machine_id.clone(), self.hostname.clone(), mac.clone(), version.clone(), now, now)).await;
            let _ = remote.execute(update_sql, (self.hostname.clone(), mac, version, now, now, self.machine_id.clone())).await;
        }

        Ok(self.machine_id.clone())
    }

    pub async fn update_host_last_seen(&self) -> Result<()> {
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;
        let sql = "UPDATE host SET last_seen = ? WHERE id = ?";
        self.local_conn.execute(sql, (now, self.machine_id.clone())).await?;
        if let Some(ref remote) = self.remote_conn {
            let _ = remote.execute(sql, (now, self.machine_id.clone())).await;
        }
        Ok(())
    }

    pub async fn get_all_hosts(&self, filter_host: Option<&str>) -> Result<Vec<(String, String, Option<String>, Option<i64>, Option<i64>)>> {
        let conn = self.remote_conn.as_ref().unwrap_or(&self.local_conn);
        let mut sql = "SELECT h.hostname, h.machine_id, h.version, h.started, COALESCE(MAX(i.updated), h.last_seen) FROM host h LEFT JOIN interface i ON h.id = i.host_id".to_string();
        let mut params = Vec::new();

        if let Some(host) = filter_host {
            sql.push_str(" WHERE h.machine_id = ? OR h.hostname = ?");
            params.push(host.to_string());
            params.push(host.to_string());
        }
        sql.push_str(" GROUP BY h.id ORDER BY h.hostname");

        let mut rows = conn.query(&sql, libsql::params_from_iter(params)).await?;
        let mut hosts = Vec::new();
        while let Some(row) = rows.next().await? {
            hosts.push((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?));
        }
        Ok(hosts)
    }
}
