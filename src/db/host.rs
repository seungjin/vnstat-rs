use anyhow::{Result};
use crate::db::Db;

impl Db {
    pub async fn get_or_create_host(&self) -> Result<i64> {
        let version = format!("{} ({})", env!("CARGO_PKG_VERSION"), env!("GIT_HASH"));
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;
        
        let insert_sql = "INSERT OR IGNORE INTO host (machine_id, hostname, version, started) VALUES (?, ?, ?, ?)";
        let update_sql = "UPDATE host SET hostname = ?, version = ?, started = ? WHERE machine_id = ?";

        self.local_conn.execute(insert_sql, (self.machine_id.clone(), self.hostname.clone(), version.clone(), now)).await?;
        self.local_conn.execute(update_sql, (self.hostname.clone(), version.clone(), now, self.machine_id.clone())).await?;

        let mut rows = self.local_conn.query("SELECT id FROM host WHERE machine_id = ?", [self.machine_id.clone()]).await?;
        let host_id = if let Some(row) = rows.next().await? {
            row.get(0)?
        } else {
            return Err(anyhow::anyhow!("Failed to retrieve host ID after creation"));
        };

        if let Some(ref remote) = self.remote_conn {
            let remote_upsert_sql = "INSERT INTO host (machine_id, hostname, version, started) VALUES (?, ?, ?, ?)
                                     ON CONFLICT(machine_id) DO UPDATE SET hostname = excluded.hostname, version = excluded.version, started = excluded.started";
            if let Err(e) = remote.execute(remote_upsert_sql, (self.machine_id.clone(), self.hostname.clone(), version, now)).await {
                eprintln!("Warning: Failed to upsert host on remote: {}", e);
            }
        }

        Ok(host_id)
    }

    pub async fn get_all_hosts(&self, filter_host: Option<&str>) -> Result<Vec<(String, String, Option<String>, Option<i64>, Option<i64>)>> {
        let conn = self.remote_conn.as_ref().unwrap_or(&self.local_conn);
        let mut sql = "SELECT hostname, machine_id, version, started, last_seen FROM host".to_string();
        let mut params = Vec::new();

        if let Some(host) = filter_host {
            sql.push_str(" WHERE machine_id = ? OR hostname = ?");
            params.push(host.to_string());
            params.push(host.to_string());
        }
        sql.push_str(" ORDER BY hostname");

        let mut rows = conn.query(&sql, libsql::params_from_iter(params)).await?;
        let mut hosts = Vec::new();
        while let Some(row) = rows.next().await? {
            let hostname: String = row.get(0)?;
            let machine_id: String = row.get(1)?;
            let version: Option<String> = row.get(2)?;
            let started: Option<i64> = row.get(3)?;
            let last_seen: Option<i64> = row.get(4)?;
            hosts.push((hostname, machine_id, version, started, last_seen));
        }
        Ok(hosts)
    }
}
