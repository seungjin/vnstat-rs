use anyhow::{Result};
use chrono::{Datelike};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::models::{InterfaceStats, HistoryEntry, SummaryData, NintyFifthData};
use crate::utils::{parse_net_dev};
use crate::db::Db;
use libsql::params;

impl Db {
    pub async fn add_traffic(&self, interface_id: &str, table: &str, date: i64, rx: u64, tx: u64) -> Result<()> {
        let sql = format!(
                "INSERT INTO {} (interface, date, rx, tx) VALUES (?, ?, ?, ?)
                 ON CONFLICT(interface, date) DO UPDATE SET rx = rx + excluded.rx, tx = tx + excluded.tx",
                table
            );
        self.local_conn.execute(&sql, (interface_id.to_string(), date, rx as i64, tx as i64)).await?;
        if let Some(ref remote) = self.remote_conn {
            let _ = remote.execute(&sql, (interface_id.to_string(), date, rx as i64, tx as i64)).await;
        }
        Ok(())
    }

    pub async fn add_history_entry(&self, id: &str, rx_delta: u64, tx_delta: u64) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        let dt = chrono::DateTime::from_timestamp(now, 0).unwrap();
        let naive = dt.naive_utc();

        let five_min = (now / 300) * 300;
        self.add_traffic(id, "fiveminute", five_min, rx_delta, tx_delta).await?;

        let hour = (now / 3600) * 3600;
        self.add_traffic(id, "hour", hour, rx_delta, tx_delta).await?;

        let day_dt = naive.date().and_hms_opt(0, 0, 0).unwrap();
        let day = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(day_dt, chrono::Utc).timestamp();
        self.add_traffic(id, "day", day, rx_delta, tx_delta).await?;

        let month_dt = naive.date().with_day(1).unwrap().and_hms_opt(0, 0, 0).unwrap();
        let month = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(month_dt, chrono::Utc).timestamp();
        self.add_traffic(id, "month", month, rx_delta, tx_delta).await?;

        let year_dt = naive.date().with_day(1).unwrap().with_month(1).unwrap().and_hms_opt(0, 0, 0).unwrap();
        let year = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(year_dt, chrono::Utc).timestamp();
        self.add_traffic(id, "year", year, rx_delta, tx_delta).await?;

        self.add_traffic(id, "top", day, rx_delta, tx_delta).await?;
        Ok(())
    }

    pub async fn prune_stats(&self, config: &crate::config::Config) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;

        // 5-minute data
        let five_min_cutoff = now - (config.five_minute_hours as i64 * 3600);
        let sql5 = "DELETE FROM fiveminute WHERE date < ?";
        self.local_conn.execute(sql5, [five_min_cutoff]).await?;
        if let Some(ref remote) = self.remote_conn {
            let _ = remote.execute(sql5, [five_min_cutoff]).await;
        }

        // Hourly data
        let hourly_cutoff = now - (config.hourly_days as i64 * 86400);
        let sqlh = "DELETE FROM hour WHERE date < ?";
        self.local_conn.execute(sqlh, [hourly_cutoff]).await?;
        if let Some(ref remote) = self.remote_conn {
            let _ = remote.execute(sqlh, [hourly_cutoff]).await;
        }

        // Daily data
        let daily_cutoff = now - (config.daily_days as i64 * 86400);
        let sqld = "DELETE FROM day WHERE date < ?";
        self.local_conn.execute(sqld, [daily_cutoff]).await?;
        if let Some(ref remote) = self.remote_conn {
            let _ = remote.execute(sqld, [daily_cutoff]).await;
        }

        // Monthly data (approximate 30 days per month for simplicity of cutoff)
        let monthly_cutoff = now - (config.monthly_months as i64 * 30 * 86400);
        let sqlm = "DELETE FROM month WHERE date < ?";
        self.local_conn.execute(sqlm, [monthly_cutoff]).await?;
        if let Some(ref remote) = self.remote_conn {
            let _ = remote.execute(sqlm, [monthly_cutoff]).await;
        }

        // Yearly data
        if config.yearly_years >= 0 {
            let yearly_cutoff = now - (config.yearly_years as i64 * 365 * 86400);
            let sqly = "DELETE FROM year WHERE date < ?";
            self.local_conn.execute(sqly, [yearly_cutoff]).await?;
            if let Some(ref remote) = self.remote_conn {
                let _ = remote.execute(sqly, [yearly_cutoff]).await;
            }
        }

        // Top days (keep only top N entries per interface)
        let mut rows = self.local_conn.query("SELECT DISTINCT interface FROM top", params![]).await?;
        let mut interfaces = Vec::new();
        while let Some(row) = rows.next().await? {
            interfaces.push(row.get::<String>(0)?);
        }

        for iface_id in interfaces {
            let delete_sql = "DELETE FROM top WHERE interface = ? AND date NOT IN (
                    SELECT date FROM top WHERE interface = ? ORDER BY (rx + tx) DESC LIMIT ?
                )";
            self.local_conn.execute(delete_sql, (iface_id.clone(), iface_id.clone(), config.top_day_entries as i64)).await?;
            if let Some(ref remote) = self.remote_conn {
                let _ = remote.execute(delete_sql, (iface_id.clone(), iface_id.clone(), config.top_day_entries as i64)).await;
            }
        }

        Ok(())
    }

    pub async fn update_stats(&self, filter_iface: Option<&str>) -> Result<()> {
        let stats = parse_net_dev()?;
        let mut seen_ids = std::collections::HashSet::new();

        for stat in stats {
            if let Some(f) = filter_iface {
                if stat.name != f {
                    continue;
                }
            }
            if let Some((id, last_rx, last_tx, current_mac)) = self.get_interface(&stat.name).await? {
                seen_ids.insert(id.clone());
                
                // Mark as active if it was inactive
                let _ = self.set_interface_active(&id, true).await;

                if current_mac.is_none() || current_mac.as_ref().map(|m| m.is_empty()).unwrap_or(true) {
                    if let Some(ref new_mac) = stat.mac_address {
                        let _ = self.update_interface_mac(&id, new_mac).await;
                    }
                }

                let rx_delta = if stat.rx_bytes >= last_rx {
                    stat.rx_bytes - last_rx
                } else {
                    stat.rx_bytes // reset
                };
                let tx_delta = if stat.tx_bytes >= last_tx {
                    stat.tx_bytes - last_tx
                } else {
                    stat.tx_bytes // reset
                };

                if rx_delta > 0 || tx_delta > 0 {
                    self.update_interface_counters(&id, stat.rx_bytes, stat.tx_bytes, rx_delta, tx_delta).await?;
                    self.add_history_entry(&id, rx_delta, tx_delta).await?;
                }
            } else {
                let id = self.create_interface(&stat.name, stat.rx_bytes, stat.tx_bytes, stat.mac_address).await?;
                seen_ids.insert(id.clone());
                self.add_history_entry(&id, 0, 0).await?;
                println!("New interface found: {} (host: {})", stat.name, self.hostname);
            }
        }

        // If not filtering, mark interfaces not seen in this pass as inactive
        if filter_iface.is_none() {
            let all_ifaces = self.get_all_interface_stats(None, Some(&self.machine_id)).await?;
            for iface_stat in all_ifaces {
                // We need the internal ID to mark inactive. 
                // Since get_all_interface_stats doesn't return ID, we'll use a new helper or get_interface
                if let Some((id, _, _, _)) = self.get_interface(&iface_stat.name).await? {
                    if !seen_ids.contains(&id) {
                        let _ = self.set_interface_active(&id, false).await;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn get_all_interface_stats(&self, filter_iface: Option<&str>, filter_host: Option<&str>) -> Result<Vec<InterfaceStats>> {
        let conn = if filter_host.is_some() && filter_host != Some(&self.machine_id) {
             self.remote_conn.as_ref().unwrap_or(&self.local_conn)
        } else {
             &self.local_conn
        };

        let mut query_str = "SELECT i.name, i.alias, i.mac_address, i.rxtotal, i.txtotal, h.hostname, i.created, i.updated 
                         FROM interface i 
                         JOIN host h ON i.host_id = h.id WHERE i.name != 'lo' AND i.active = 1 ".to_string();
        
        if let Some(iface) = filter_iface {
            query_str.push_str(&format!(" AND i.name = '{}' ", iface));
        }

        if let Some(host) = filter_host {
            query_str.push_str(&format!(" AND (h.hostname = '{}' OR h.machine_id = '{}') ", host, host));
        }
        
        let mut rows = conn.query(&query_str, params![]).await?;
        let mut stats = Vec::new();
        while let Some(row) = rows.next().await? {
            stats.push(InterfaceStats {
                name: row.get(0)?,
                alias: row.get(1)?,
                mac_address: row.get(2)?,
                rx_bytes: row.get::<i64>(3)? as u64,
                tx_bytes: row.get::<i64>(4)? as u64,
                rx_packets: 0,
                tx_packets: 0,
                hostname: row.get(5)?,
                created: row.get(6)?,
                updated: row.get(7)?,
            });
        }
        Ok(stats)
    }

    pub async fn get_history(&self, table: &str, filter_iface: Option<&str>, filter_host: Option<&str>, limit: usize, begin: Option<i64>, end: Option<i64>) -> Result<Vec<HistoryEntry>> {
        let conn = if filter_host.is_some() && filter_host != Some(&self.machine_id) {
             self.remote_conn.as_ref().unwrap_or(&self.local_conn)
        } else {
             &self.local_conn
        };

        let mut query_str = format!(
            "SELECT h.hostname, i.name, t.date, t.rx, t.tx 
             FROM interface i 
             JOIN host h ON i.host_id = h.id
             JOIN {} t ON i.id = t.interface WHERE i.name != 'lo' AND i.active = 1 ", table);
        
        if let Some(iface) = filter_iface {
            query_str.push_str(&format!("AND i.name = '{}' ", iface));
        }

        if let Some(host) = filter_host {
            query_str.push_str(&format!(" AND (h.hostname = '{}' OR h.machine_id = '{}') ", host, host));
        }

        if let Some(b) = begin {
            query_str.push_str(&format!("AND t.date >= {} ", b));
        }

        if let Some(e) = end {
            query_str.push_str(&format!("AND t.date <= {} ", e));
        }

        if table == "top" {
            query_str.push_str(&format!("ORDER BY (t.rx + t.tx) DESC LIMIT {}", limit));
        } else {
            query_str.push_str(&format!("ORDER BY t.date DESC LIMIT {}", limit));
        }

        let mut rows = conn.query(&query_str, params![]).await?;
        let mut history = Vec::new();
        while let Some(row) = rows.next().await? {
            history.push(HistoryEntry {
                hostname: row.get(0)?,
                interface: row.get(1)?,
                date: row.get(2)?,
                rx: row.get::<i64>(3)? as u64,
                tx: row.get::<i64>(4)? as u64,
            });
        }
        Ok(history)
    }

    pub async fn get_summary(&self, filter_iface: Option<&str>, filter_host: Option<&str>) -> Result<Vec<SummaryData>> {
        // For host-all, we must query remote to get other hosts
        let conn = if filter_host.is_none() || filter_host != Some(&self.machine_id) {
            self.remote_conn.as_ref().unwrap_or(&self.local_conn)
        } else {
            &self.local_conn
        };

        let mut ifaces_query = "SELECT i.id, i.name, h.hostname, h.machine_id FROM interface i JOIN host h ON i.host_id = h.id WHERE i.name != 'lo' AND i.active = 1".to_string();
        
        if let Some(iface) = filter_iface {
            ifaces_query.push_str(&format!(" AND i.name = '{}'", iface));
        }

        if let Some(host) = filter_host {
            ifaces_query.push_str(&format!(" AND (h.hostname = '{}' OR h.machine_id = '{}')", host, host));
        }

        ifaces_query.push_str(" ORDER BY h.hostname, i.name");

        let mut iface_rows = conn.query(&ifaces_query, params![]).await?;
        let mut interfaces = Vec::new();
        while let Some(row) = iface_rows.next().await? {
            interfaces.push((row.get::<String>(0)?, row.get::<String>(1)?, row.get::<String>(2)?, row.get::<String>(3)?));
        }

        let now = chrono::Utc::now();
        let today_start = now.date_naive().and_hms_opt(0, 0, 0).unwrap();
        let today_ts = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(today_start, chrono::Utc).timestamp();
        let yesterday_ts = today_ts - 86400;
        let this_month_start = now.date_naive().with_day(1).unwrap().and_hms_opt(0, 0, 0).unwrap();
        let this_month_ts = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(this_month_start, chrono::Utc).timestamp();
        
        let last_month_date = if now.month() == 1 {
            now.date_naive().with_year(now.year() - 1).unwrap().with_month(12).unwrap().with_day(1).unwrap()
        } else {
            now.date_naive().with_month(now.month() - 1).unwrap().with_day(1).unwrap()
        };
        let last_month_ts = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(last_month_date.and_hms_opt(0, 0, 0).unwrap(), chrono::Utc).timestamp();

        let mut summaries = Vec::new();

        for (id, name, hostname, mid) in interfaces {
            let active_conn = if mid == self.machine_id { &self.local_conn } else { conn };

            let mut stats = std::collections::HashMap::new();
            let mut m_rows = active_conn.query("SELECT date, rx, tx FROM month WHERE interface = ? AND date IN (?, ?)", (id.clone(), this_month_ts, last_month_ts)).await?;
            while let Some(row) = m_rows.next().await? {
                stats.insert(format!("m_{}", row.get::<i64>(0)?), (row.get::<i64>(1)? as u64, row.get::<i64>(2)? as u64));
            }

            let mut d_rows = active_conn.query("SELECT date, rx, tx FROM day WHERE interface = ? AND date IN (?, ?)", (id, today_ts, yesterday_ts)).await?;
            while let Some(row) = d_rows.next().await? {
                stats.insert(format!("d_{}", row.get::<i64>(0)?), (row.get::<i64>(1)? as u64, row.get::<i64>(2)? as u64));
            }

            summaries.push(SummaryData {
                name,
                hostname,
                today: stats.get(&format!("d_{}", today_ts)).cloned().unwrap_or((0, 0)),
                yesterday: stats.get(&format!("d_{}", yesterday_ts)).cloned().unwrap_or((0, 0)),
                this_month: stats.get(&format!("m_{}", this_month_ts)).cloned().unwrap_or((0, 0)),
                last_month: stats.get(&format!("m_{}", last_month_ts)).cloned().unwrap_or((0, 0)),
            });
        }
        Ok(summaries)
    }

    pub async fn get_95th_data(&self, filter_iface: Option<&str>, filter_host: Option<&str>) -> Result<NintyFifthData> {
        let conn = if filter_host.is_some() && filter_host != Some(&self.machine_id) {
             self.remote_conn.as_ref().unwrap_or(&self.local_conn)
        } else {
             &self.local_conn
        };

        // Find the specific interface
        let mut iface_query = "SELECT i.id, i.name, h.hostname FROM interface i JOIN host h ON i.host_id = h.id WHERE i.name != 'lo' AND i.active = 1 ".to_string();
        if let Some(iface) = filter_iface {
            iface_query.push_str(&format!(" AND i.name = '{}'", iface));
        }
        if let Some(host) = filter_host {
            iface_query.push_str(&format!(" AND (h.hostname = '{}' OR h.machine_id = '{}')", host, host));
        }
        iface_query.push_str(" LIMIT 1");

        let mut rows = conn.query(&iface_query, params![]).await?;
        let (iface_id, name, hostname) = if let Some(row) = rows.next().await? {
            (row.get::<String>(0)?, row.get::<String>(1)?, row.get::<String>(2)?)
        } else {
            return Err(anyhow::anyhow!("Interface not found"));
        };

        let now = chrono::Utc::now();
        let month_start = now.date_naive().with_day(1).unwrap().and_hms_opt(0, 0, 0).unwrap();
        let begin = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(month_start, chrono::Utc).timestamp();
        let end = now.timestamp();

        let mut data_rows = conn.query(
            "SELECT rx, tx FROM fiveminute WHERE interface = ? AND date >= ? AND date <= ? ORDER BY date ASC",
            (iface_id, begin, end)
        ).await?;

        let mut rx = Vec::new();
        let mut tx = Vec::new();
        while let Some(row) = data_rows.next().await? {
            rx.push(row.get::<i64>(0)? as u64);
            tx.push(row.get::<i64>(1)? as u64);
        }

        let total_expected = ((end - begin) / 300) as usize;
        let coverage = if total_expected > 0 {
            (rx.len() as f64 / total_expected as f64) * 100.0
        } else {
            100.0
        };

        Ok(NintyFifthData {
            interface: name,
            hostname,
            begin,
            end,
            count: rx.len(),
            coverage,
            rx,
            tx,
        })
    }
}
