use anyhow::{Result};
use crate::db::Db;

impl Db {
    pub async fn get_or_create_host(&self) -> Result<String> {
        let version = format!("{} ({})", env!("CARGO_PKG_VERSION"), env!("GIT_HASH"));
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;
        
        let insert_sql = "INSERT OR IGNORE INTO host (id, machine_id, hostname, version, started) VALUES (?, ?, ?, ?, ?)";
        let update_sql = "UPDATE host SET hostname = ?, version = ?, started = ? WHERE id = ?";

        self.local_conn.execute(insert_sql, (self.machine_id.clone(), self.machine_id.clone(), self.hostname.clone(), version.clone(), now)).await?;
        self.local_conn.execute(update_sql, (self.hostname.clone(), version.clone(), now, self.machine_id.clone())).await?;

        if let Some(ref remote) = self.remote_conn {
            let _ = remote.execute(insert_sql, (self.machine_id.clone(), self.machine_id.clone(), self.hostname.clone(), version.clone(), now)).await;
            let _ = remote.execute(update_sql, (self.hostname.clone(), version, now, self.machine_id.clone())).await;
        }

        Ok(self.machine_id.clone())
    }

    pub async fn get_all_hosts(&self, filter_host: Option<&str>) -> Result<Vec<(String, String, Option<String>, Option<i64>, Option<i64>)>> {
        let conn = self.remote_conn.as_ref().unwrap_or(&self.local_conn);
        let mut sql = "SELECT h.hostname, h.machine_id, h.version, h.started, \
            MAX( \
                COALESCE((SELECT MAX(updated) FROM interface WHERE host_id = h.id), 0), \
                COALESCE((SELECT MAX(date) FROM fiveminute WHERE interface IN (SELECT id FROM interface WHERE host_id = h.id)), 0), \
                COALESCE((SELECT MAX(date) FROM hour WHERE interface IN (SELECT id FROM interface WHERE host_id = h.id)), 0), \
                COALESCE((SELECT MAX(date) FROM day WHERE interface IN (SELECT id FROM interface WHERE host_id = h.id)), 0), \
                COALESCE((SELECT MAX(date) FROM month WHERE interface IN (SELECT id FROM interface WHERE host_id = h.id)), 0), \
                COALESCE((SELECT MAX(date) FROM year WHERE interface IN (SELECT id FROM interface WHERE host_id = h.id)), 0), \
                COALESCE((SELECT MAX(date) FROM top WHERE interface IN (SELECT id FROM interface WHERE host_id = h.id)), 0) \
            ) as last_seen_agg \
            FROM host h".to_string();
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
