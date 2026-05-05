use anyhow::{Result};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::models::{InterfaceStats, HistoryEntry, SummaryData, NintyFifthData};
use crate::utils::{parse_net_dev};
use crate::db::Db;
use libsql::params;
use chrono::{Datelike, Local, TimeZone, Utc, Timelike};

impl Db {
    pub async fn add_traffic(&self, interface_id: i64, table: &str, date: i64, rx: u64, tx: u64) -> Result<()> {
        let sql = format!(
                "INSERT INTO {} (interface, date, rx, tx) VALUES (?, ?, ?, ?)
                 ON CONFLICT(interface, date) DO UPDATE SET rx = rx + excluded.rx, tx = tx + excluded.tx",
                table
            );
        self.local_conn.execute(&sql, (interface_id, date, rx as i64, tx as i64)).await?;
        if let Some(ref remote) = self.remote_conn {
            if let Err(e) = remote.execute(&sql, (interface_id, date, rx as i64, tx as i64)).await {
                eprintln!("Warning: Failed to add traffic to remote (table {}): {}", table, e);
            }
        }
        Ok(())
    }

    pub async fn add_history_entry(&self, id: i64, rx_delta: u64, tx_delta: u64) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        let dt_utc = Utc::now();

        let five_min = (now / 300) * 300;
        self.add_traffic(id, "fiveminute", five_min, rx_delta, tx_delta).await?;

        let hour = (now / 3600) * 3600;
        self.add_traffic(id, "hour", hour, rx_delta, tx_delta).await?;

        let day_dt = dt_utc.date_naive().and_hms_opt(0, 0, 0).unwrap();
        let day = Utc.from_local_datetime(&day_dt).unwrap().timestamp();
        self.add_traffic(id, "day", day, rx_delta, tx_delta).await?;

        let month_dt = dt_utc.date_naive().with_day(1).unwrap().and_hms_opt(0, 0, 0).unwrap();
        let month = Utc.from_local_datetime(&month_dt).unwrap().timestamp();
        self.add_traffic(id, "month", month, rx_delta, tx_delta).await?;

        let year_dt = dt_utc.date_naive().with_day(1).unwrap().with_month(1).unwrap().and_hms_opt(0, 0, 0).unwrap();
        let year = Utc.from_local_datetime(&year_dt).unwrap().timestamp();
        self.add_traffic(id, "year", year, rx_delta, tx_delta).await?;

        self.add_traffic(id, "top", day, rx_delta, tx_delta).await?;
        Ok(())
    }

    pub async fn prune_stats(&self, config: &crate::config::Config) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        let host_id = self.host_id.clone();

        // 5-minute data
        let five_min_cutoff = now - (config.five_minute_hours as i64 * 3600);
        let sql5 = "DELETE FROM fiveminute WHERE date < ? AND interface IN (SELECT id FROM interface WHERE host_id = ?)";
        self.local_conn.execute(sql5, (five_min_cutoff, host_id.clone())).await?;
        if let Some(ref remote) = self.remote_conn {
            if let Err(e) = remote.execute(sql5, (five_min_cutoff, host_id.clone())).await {
                eprintln!("Warning: Failed to prune 5-minute data on remote: {}", e);
            }
        }

        // Hourly data
        let hourly_cutoff = now - (config.hourly_days as i64 * 86400);
        let sqlh = "DELETE FROM hour WHERE date < ? AND interface IN (SELECT id FROM interface WHERE host_id = ?)";
        self.local_conn.execute(sqlh, (hourly_cutoff, host_id.clone())).await?;
        if let Some(ref remote) = self.remote_conn {
            if let Err(e) = remote.execute(sqlh, (hourly_cutoff, host_id.clone())).await {
                eprintln!("Warning: Failed to prune hourly data on remote: {}", e);
            }
        }

        // Daily data
        let daily_cutoff = now - (config.daily_days as i64 * 86400);
        let sqld = "DELETE FROM day WHERE date < ? AND interface IN (SELECT id FROM interface WHERE host_id = ?)";
        self.local_conn.execute(sqld, (daily_cutoff, host_id.clone())).await?;
        if let Some(ref remote) = self.remote_conn {
            if let Err(e) = remote.execute(sqld, (daily_cutoff, host_id.clone())).await {
                eprintln!("Warning: Failed to prune daily data on remote: {}", e);
            }
        }

        // Monthly data (approximate 30 days per month for simplicity of cutoff)
        let monthly_cutoff = now - (config.monthly_months as i64 * 30 * 86400);
        let sqlm = "DELETE FROM month WHERE date < ? AND interface IN (SELECT id FROM interface WHERE host_id = ?)";
        self.local_conn.execute(sqlm, (monthly_cutoff, host_id.clone())).await?;
        if let Some(ref remote) = self.remote_conn {
            if let Err(e) = remote.execute(sqlm, (monthly_cutoff, host_id.clone())).await {
                eprintln!("Warning: Failed to prune monthly data on remote: {}", e);
            }
        }

        // Yearly data
        if config.yearly_years >= 0 {
            let yearly_cutoff = now - (config.yearly_years as i64 * 365 * 86400);
            let sqly = "DELETE FROM year WHERE date < ? AND interface IN (SELECT id FROM interface WHERE host_id = ?)";
            self.local_conn.execute(sqly, (yearly_cutoff, host_id.clone())).await?;
            if let Some(ref remote) = self.remote_conn {
                if let Err(e) = remote.execute(sqly, (yearly_cutoff, host_id.clone())).await {
                    eprintln!("Warning: Failed to prune yearly data on remote: {}", e);
                }
            }
        }

        // Top days (keep only top N entries per interface belonging to this host)
        let mut rows = self.local_conn.query("SELECT id FROM interface WHERE host_id = ?", [host_id.clone()]).await?;
        let mut interfaces = Vec::new();
        while let Some(row) = rows.next().await? {
            let id: i64 = row.get(0)?;
            interfaces.push(id);
        }

        for iface_id in interfaces {
            let delete_sql = "DELETE FROM top WHERE interface = ? AND date NOT IN (
                    SELECT date FROM top WHERE interface = ? ORDER BY (rx + tx) DESC LIMIT ?
                )";
            self.local_conn.execute(delete_sql, (iface_id, iface_id, config.top_day_entries as i64)).await?;
            if let Some(ref remote) = self.remote_conn {
                if let Err(e) = remote.execute(delete_sql, (iface_id, iface_id, config.top_day_entries as i64)).await {
                    eprintln!("Warning: Failed to prune top data on remote for interface {}: {}", iface_id, e);
                }
            }
        }

        Ok(())
    }

    pub async fn update_stats(&self, filter_iface: Option<&str>, config: &crate::config::Config) -> Result<()> {
        let stats = parse_net_dev()?;
        let mut seen_ids = std::collections::HashSet::new();

        for stat in stats {
            if let Some(f) = filter_iface {
                if stat.name != f {
                    continue;
                }
            }
            if let Some((id, last_rx, last_tx, current_mac, updated)) = self.get_interface(&stat.name).await? {
                seen_ids.insert(id);
                
                // Mark as active if it was inactive
                let _ = self.set_interface_active(id, true).await;

                if current_mac.is_none() || current_mac.as_ref().map(|m| m.is_empty()).unwrap_or(true) {
                    if let Some(ref new_mac) = stat.mac_address {
                        let _ = self.update_interface_mac(id, new_mac).await;
                    }
                }

                let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
                let time_diff = (now - updated).max(1) as u64;
                let max_bytes_per_sec = (config.max_bandwidth * 1_000_000) / 8;

                let calculate_delta = |current: u64, last: u64| -> u64 {
                    if current >= last {
                        current - last
                    } else {
                        // Potential rollover
                        let roll_32 = (u32::MAX as u64).saturating_sub(last).saturating_add(current).saturating_add(1);
                        let roll_64 = u64::MAX.saturating_sub(last).saturating_add(current).saturating_add(1);

                        if max_bytes_per_sec == 0 {
                            current // treat as reset if check is disabled
                        } else if last <= u32::MAX as u64 && (roll_32 / time_diff) <= max_bytes_per_sec {
                            roll_32
                        } else if (roll_64 / time_diff) <= max_bytes_per_sec {
                            roll_64
                        } else {
                            current // treat as reset
                        }
                    }
                };

                let rx_delta = calculate_delta(stat.rx_bytes, last_rx);
                let tx_delta = calculate_delta(stat.tx_bytes, last_tx);

                if rx_delta > 0 || tx_delta > 0 {
                    println!("Updating interface {} (+{} RX, +{} TX)...", stat.name, rx_delta, tx_delta);
                    self.update_interface_counters(id, stat.rx_bytes, stat.tx_bytes, rx_delta, tx_delta).await?;
                    self.add_history_entry(id, rx_delta, tx_delta).await?;
                }
            } else {
                let id = self.create_interface(&stat.name, stat.rx_bytes, stat.tx_bytes, stat.mac_address).await?;
                seen_ids.insert(id);
                self.add_history_entry(id, 0, 0).await?;
                println!("New interface found and registered: {} (host: {})", stat.name, self.hostname);
            }
        }

        // If not filtering, mark interfaces not seen in this pass as inactive
        if filter_iface.is_none() {
            let all_ifaces = self.get_all_interface_stats(None, Some(&self.machine_id)).await?;
            for iface_stat in all_ifaces {
                // We need the internal ID to mark inactive. 
                // Since get_all_interface_stats doesn't return ID, we'll use a new helper or get_interface
                if let Some((id, _, _, _, _)) = self.get_interface(&iface_stat.name).await? {
                    if !seen_ids.contains(&id) {
                        let _ = self.set_interface_active(id, false).await;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn get_all_interface_stats(&self, filter_iface: Option<&str>, filter_host: Option<&str>) -> Result<Vec<InterfaceStats>> {
        let conn = if filter_host.is_none() || filter_host != Some(&self.machine_id) {
             self.remote_conn.as_ref().unwrap_or(&self.local_conn)
        } else {
             &self.local_conn
        };

        let mut ifaces_query = "SELECT i.id, i.name, i.alias, i.mac_address, i.rxtotal, i.txtotal, h.hostname, i.created, i.updated, h.machine_id
                          FROM interface i
                          JOIN host h ON i.host_id = h.id WHERE i.name != 'lo' ".to_string();

        if let Some(iface) = filter_iface {
            ifaces_query.push_str(&format!(" AND i.name = '{}' ", iface));
        }

        if let Some(host) = filter_host {
            ifaces_query.push_str(&format!(" AND (h.hostname = '{}' OR h.machine_id = '{}') ", host, host));
        }

        let mut rows = conn.query(&ifaces_query, params![]).await?;
        let mut stats = Vec::new();
        while let Some(row) = rows.next().await? {
            let name: String = row.get(1)?;
            let alias: Option<String> = row.get(2)?;
            let mac: Option<String> = row.get(3)?;
            let rxtotal: i64 = row.get(4)?;
            let txtotal: i64 = row.get(5)?;
            let hostname: String = row.get(6)?;
            let created: i64 = row.get(7)?;
            let updated: i64 = row.get(8)?;

            stats.push(InterfaceStats {
                name,
                alias,
                mac_address: mac,
                rx_bytes: rxtotal as u64,
                tx_bytes: txtotal as u64,
                rx_packets: 0,
                tx_packets: 0,
                hostname,
                created,
                updated,
            });
        }
        Ok(stats)
    }
    pub async fn get_history(&self, table: &str, filter_iface: Option<&str>, filter_host: Option<&str>, limit: usize, begin: Option<i64>, end: Option<i64>) -> Result<Vec<HistoryEntry>> {
        let conn = if filter_host.is_none() || filter_host != Some(&self.machine_id) {
            self.remote_conn.as_ref().unwrap_or(&self.local_conn)
        } else {
            &self.local_conn
        };

        let mut ifaces_query = "SELECT i.id, i.name, h.hostname, h.machine_id FROM interface i JOIN host h ON i.host_id = h.id WHERE i.name != 'lo'".to_string();

        if let Some(iface) = filter_iface {
            ifaces_query.push_str(&format!(" AND i.name = '{}'", iface));
        }

        if let Some(host) = filter_host {
            ifaces_query.push_str(&format!(" AND (h.hostname = '{}' OR h.machine_id = '{}')", host, host));
        }

        let mut iface_rows = conn.query(&ifaces_query, params![]).await?;
        let mut interfaces = Vec::new();
        while let Some(row) = iface_rows.next().await? {
            interfaces.push((row.get::<i64>(0)?, row.get::<String>(1)?, row.get::<String>(2)?, row.get::<String>(3)?));
        }

        let mut history = Vec::new();
        for (id, name, hostname, _mid) in interfaces {
            let active_conn = conn;

            // Determine if we can aggregate from a smaller unit for better local accuracy
            let (source_table, group_by_table) = match table {
                "hour" => ("fiveminute", true),
                "day" => ("hour", true),
                "month" => ("day", true),
                "year" => ("day", true),
                _ => (table, false), // No aggregation for fiveminute or top
            };

            let mut query_str = format!("SELECT rx, tx, date FROM {} WHERE interface = ? ", source_table);
            if let Some(b) = begin {
                query_str.push_str(&format!("AND date >= {} ", b));
            }
            if let Some(e) = end {
                query_str.push_str(&format!("AND date <= {} ", e));
            }

            if group_by_table {
                 // Fetch a reasonable amount of data to satisfy the limit after grouping
                 let fetch_limit = match table {
                     "hour" => limit * 12, // 12 * 5min = 1 hour
                     "day" => limit * 24,  // 24 * 1 hour = 1 day
                     "month" => limit * 31,
                     "year" => limit * 366,
                     _ => limit,
                 };
                 query_str.push_str(&format!("ORDER BY date DESC LIMIT {}", fetch_limit));
            } else if table == "top" {
                query_str.push_str(&format!("ORDER BY (rx + tx) DESC LIMIT {}", limit));
            } else {
                query_str.push_str(&format!("ORDER BY date DESC LIMIT {}", limit));
            }

            let mut data_rows = active_conn.query(&query_str, [id]).await?;
            
            if group_by_table {
                let mut aggregated: std::collections::BTreeMap<i64, (u64, u64)> = std::collections::BTreeMap::new();
                while let Some(row) = data_rows.next().await? {
                    let rx: i64 = row.get(0)?;
                    let tx: i64 = row.get(1)?;
                    let date_utc: i64 = row.get(2)?;
                    
                    let dt_local = Local.from_utc_datetime(&chrono::DateTime::from_timestamp(date_utc, 0).unwrap().naive_utc());
                    let bucket_ts = match table {
                        "hour" => {
                            let dt = dt_local.date_naive().and_hms_opt(dt_local.hour(), 0, 0).unwrap();
                            Local.from_local_datetime(&dt).unwrap().timestamp()
                        },
                        "day" => {
                            let dt = dt_local.date_naive().and_hms_opt(0, 0, 0).unwrap();
                            Local.from_local_datetime(&dt).unwrap().timestamp()
                        },
                        "month" => {
                            let dt = dt_local.date_naive().with_day(1).unwrap().and_hms_opt(0, 0, 0).unwrap();
                            Local.from_local_datetime(&dt).unwrap().timestamp()
                        },
                        "year" => {
                            let dt = dt_local.date_naive().with_day(1).unwrap().with_month(1).unwrap().and_hms_opt(0, 0, 0).unwrap();
                            Local.from_local_datetime(&dt).unwrap().timestamp()
                        },
                        _ => date_utc,
                    };
                    
                    let entry = aggregated.entry(bucket_ts).or_insert((0, 0));
                    entry.0 += rx as u64;
                    entry.1 += tx as u64;
                }
                
                for (date, (rx, tx)) in aggregated.into_iter().rev().take(limit) {
                    history.push(HistoryEntry {
                        hostname: hostname.clone(),
                        interface: name.clone(),
                        date,
                        rx,
                        tx,
                    });
                }
            } else {
                while let Some(row) = data_rows.next().await? {
                    let date: i64 = row.get(2)?;
                    let rx: i64 = row.get(0)?;
                    let tx: i64 = row.get(1)?;
                    history.push(HistoryEntry {
                        hostname: hostname.clone(),
                        interface: name.clone(),
                        date,
                        rx: rx as u64,
                        tx: tx as u64,
                    });
                }
            }
        }

        // Sort overall if needed (for non-top tables, they are already sorted per interface)
        if table != "top" {
            history.sort_by(|a, b| b.date.cmp(&a.date));
        } else {
            history.sort_by(|a, b| (b.rx + b.tx).cmp(&(a.rx + a.tx)));
        }

        Ok(history)
    }

    pub async fn get_summary(&self, filter_iface: Option<&str>, filter_host: Option<&str>) -> Result<Vec<SummaryData>> {
        // For all-hosts, we must query remote to get other hosts
        let conn = if filter_host.is_none() || filter_host != Some(&self.machine_id) {
             self.remote_conn.as_ref().unwrap_or(&self.local_conn)
        } else {
             &self.local_conn
        };
        let mut ifaces_query = "SELECT i.id, i.name, h.hostname, h.machine_id FROM interface i JOIN host h ON i.host_id = h.id WHERE i.name != 'lo'".to_string();

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
            interfaces.push((row.get::<i64>(0)?, row.get::<String>(1)?, row.get::<String>(2)?, row.get::<String>(3)?));
        }

        let now_local = Local::now();
        let today_start = now_local.date_naive().and_hms_opt(0, 0, 0).unwrap();
        let today_ts_local = Local.from_local_datetime(&today_start).unwrap().timestamp();
        
        let yesterday_date = now_local.date_naive().pred_opt().unwrap();
        let yesterday_start = yesterday_date.and_hms_opt(0, 0, 0).unwrap();
        let yesterday_ts_local = Local.from_local_datetime(&yesterday_start).unwrap().timestamp();

        let this_month_start = now_local.date_naive().with_day(1).unwrap().and_hms_opt(0, 0, 0).unwrap();
        let this_month_ts_local = Local.from_local_datetime(&this_month_start).unwrap().timestamp();

        let last_month_date = if now_local.month() == 1 {
            now_local.date_naive().with_year(now_local.year() - 1).unwrap().with_month(12).unwrap().with_day(1).unwrap()
        } else {
            now_local.date_naive().with_month(now_local.month() - 1).unwrap().with_day(1).unwrap()
        };
        let last_month_ts_local = Local.from_local_datetime(&last_month_date.and_hms_opt(0, 0, 0).unwrap()).unwrap().timestamp();

        let mut summaries = Vec::new();

        for (id, name, hostname, _mid) in interfaces {
            let active_conn = conn;

            // To show accurate Local Analysis for UTC data, we need to query fine-grained buckets
            // and sum them according to local boundaries.
            
            // 1. Get Today/Yesterday stats from 'hour' table (UTC) and sum for Local Today/Yesterday
            let mut h_rows = active_conn.query("SELECT date, rx, tx FROM hour WHERE interface = ? AND date >= ?", (id.clone(), yesterday_ts_local - 3600)).await?;
            let mut today_rx = 0; let mut today_tx = 0;
            let mut yest_rx = 0; let mut yest_tx = 0;
            
            while let Some(row) = h_rows.next().await? {
                let date_utc: i64 = row.get(0)?;
                let rx: i64 = row.get(1)?;
                let tx: i64 = row.get(2)?;
                
                // Convert UTC bucket to Local to see where it falls
                let dt_local = Local.from_utc_datetime(&chrono::DateTime::from_timestamp(date_utc, 0).unwrap().naive_utc());
                let local_start_of_day = dt_local.date_naive().and_hms_opt(0, 0, 0).unwrap();
                let local_day_ts = Local.from_local_datetime(&local_start_of_day).unwrap().timestamp();
                
                if local_day_ts == today_ts_local {
                    today_rx += rx as u64; today_tx += tx as u64;
                } else if local_day_ts == yesterday_ts_local {
                    yest_rx += rx as u64; yest_tx += tx as u64;
                }
            }
            
            // 2. Get This Month / Last Month stats from 'day' table (UTC) and sum for Local Month
            let mut d_rows = active_conn.query("SELECT date, rx, tx FROM day WHERE interface = ? AND date >= ?", (id.clone(), last_month_ts_local - 86400)).await?;
            let mut this_m_rx = 0; let mut this_m_tx = 0;
            let mut last_m_rx = 0; let mut last_m_tx = 0;
            
            while let Some(row) = d_rows.next().await? {
                let date_utc: i64 = row.get(0)?;
                let rx: i64 = row.get(1)?;
                let tx: i64 = row.get(2)?;
                
                let dt_local = Local.from_utc_datetime(&chrono::DateTime::from_timestamp(date_utc, 0).unwrap().naive_utc());
                let local_start_of_month = dt_local.date_naive().with_day(1).unwrap().and_hms_opt(0, 0, 0).unwrap();
                let local_month_ts = Local.from_local_datetime(&local_start_of_month).unwrap().timestamp();
                
                if local_month_ts == this_month_ts_local {
                    this_m_rx += rx as u64; this_m_tx += tx as u64;
                } else if local_month_ts == last_month_ts_local {
                    last_m_rx += rx as u64; last_m_tx += tx as u64;
                }
            }

            summaries.push(SummaryData {
                name,
                hostname,
                today: (today_rx, today_tx),
                yesterday: (yest_rx, yest_tx),
                this_month: (this_m_rx, this_m_tx),
                last_month: (last_m_rx, last_m_tx),
            });
        }
        Ok(summaries)
    }

    pub async fn get_95th_data(&self, filter_iface: Option<&str>, filter_host: Option<&str>) -> Result<NintyFifthData> {
        let conn = if filter_host.is_none() || filter_host != Some(&self.machine_id) {
             self.remote_conn.as_ref().unwrap_or(&self.local_conn)
        } else {
             &self.local_conn
        };

        // Find the specific interface, prioritizing active ones with traffic
        let mut iface_query = "SELECT i.id, i.name, h.hostname, h.machine_id FROM interface i JOIN host h ON i.host_id = h.id WHERE i.name != 'lo' ".to_string();
        if let Some(iface) = filter_iface {
            iface_query.push_str(&format!(" AND i.name = '{}'", iface));
        }
        if let Some(host) = filter_host {
            iface_query.push_str(&format!(" AND (h.hostname = '{}' OR h.machine_id = '{}')", host, host));
        }
        
        // Prioritize active interfaces with the most total traffic
        iface_query.push_str(" ORDER BY i.active DESC, (i.rxtotal + i.txtotal) DESC LIMIT 1");

        let mut rows = conn.query(&iface_query, params![]).await?;
        let (iface_id, name, hostname, _mid) = if let Some(row) = rows.next().await? {
            (row.get::<i64>(0)?, row.get::<String>(1)?, row.get::<String>(2)?, row.get::<String>(3)?)
        } else {
            return Err(anyhow::anyhow!("Interface not found"));
        };

        let active_conn = conn;

        let now_local = Local::now();
        let month_start = now_local.date_naive().with_day(1).unwrap().and_hms_opt(0, 0, 0).unwrap();
        let begin = Local.from_local_datetime(&month_start).unwrap().timestamp();
        let end = now_local.timestamp();

        let mut data_rows = active_conn.query(
            "SELECT rx, tx FROM fiveminute WHERE interface = ? AND date >= ? AND date <= ? ORDER BY date ASC",
            (iface_id, begin, end)
        ).await?;
        let mut rx = Vec::new();
        let mut tx = Vec::new();
        while let Some(row) = data_rows.next().await? {
            let r: i64 = row.get(0)?;
            let t: i64 = row.get(1)?;
            rx.push(r as u64);
            tx.push(t as u64);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use crate::config::Config;

    #[tokio::test]
    async fn test_prune_stats_host_isolation() -> Result<()> {
        let db_path = PathBuf::from("test_prune.db");
        if db_path.exists() { let _ = std::fs::remove_file(&db_path); }

        let db = Db::open(db_path.clone(), None, None, Some("host-a".to_string())).await?;
        
        // Create another host manually
        let host_b_id = "host-b-id".to_string();
        db.local_conn.execute(
            "INSERT INTO host (id, machine_id, hostname) VALUES (?, ?, ?)",
            (host_b_id.clone(), host_b_id.clone(), "host-b")
        ).await?;

        // Create interfaces
        let iface_a = db.create_interface("eth0", 0, 0, None).await?;
        
        // Manually create interface for host B
        db.local_conn.execute(
            "INSERT INTO interface (host_id, name, created, updated) VALUES (?, ?, ?, ?)",
            (host_b_id.clone(), "eth0", 0, 0)
        ).await?;
        let iface_b = db.local_conn.last_insert_rowid();

        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        let old_date = now - (100 * 3600); // 100 hours ago

        // Add traffic for both
        db.add_traffic(iface_a, "fiveminute", old_date, 100, 100).await?;
        db.add_traffic(iface_b, "fiveminute", old_date, 200, 200).await?;

        // Configure retention: 5MinuteHours = 48
        let mut config = Config::default();
        config.five_minute_hours = 48;

        // Prune as Host A
        db.prune_stats(&config).await?;

        // Check if Host A's data is gone
        let mut rows_a = db.local_conn.query("SELECT count(*) FROM fiveminute WHERE interface = ?", [iface_a]).await?;
        let count_a: i64 = rows_a.next().await?.unwrap().get(0)?;
        assert_eq!(count_a, 0);

        // Check if Host B's data is still there
        let mut rows_b = db.local_conn.query("SELECT count(*) FROM fiveminute WHERE interface = ?", [iface_b]).await?;
        let count_b: i64 = rows_b.next().await?.unwrap().get(0)?;
        assert_eq!(count_b, 1);

        if db_path.exists() { let _ = std::fs::remove_file(&db_path); }
        Ok(())
    }

    #[test]
    fn test_delta_calculation_logic() {
        let max_bandwidth = 1000; // 1000 Mbit/s
        let time_diff = 10; // 10 seconds
        let max_bytes_per_sec = (max_bandwidth * 1_000_000) / 8;

        let calculate_delta = |current: u64, last: u64| -> u64 {
            if current >= last {
                current - last
            } else {
                let roll_32 = (u32::MAX as u64).saturating_sub(last).saturating_add(current).saturating_add(1);
                let roll_64 = u64::MAX.saturating_sub(last).saturating_add(current).saturating_add(1);

                if max_bytes_per_sec == 0 {
                    current
                } else if last <= u32::MAX as u64 && (roll_32 / time_diff) <= max_bytes_per_sec {
                    roll_32
                } else if (roll_64 / time_diff) <= max_bytes_per_sec {
                    roll_64
                } else {
                    current
                }
            }
        };

        // Normal increase
        assert_eq!(calculate_delta(1500, 1000), 500);

        // 32-bit rollover (valid)
        // last = 2^32 - 499, current = 200. Delta = 499 + 200 + 1 = 700.
        // 700 bytes / 10 sec = 70 bytes/sec. Well within 125MB/s (1000Mbit/s).
        let last_32 = u32::MAX as u64 - 499;
        assert_eq!(calculate_delta(200, last_32), 700);

        // 32-bit "rollover" that is actually a reboot (exceeds bandwidth)
        // last = 2^32 - 499, current = 2GB. 
        // Delta would be ~2GB + 500. 2GB / 10s = 200MB/s > 125MB/s.
        // Should treat as reset (delta = 2GB).
        let current_large = 2_000_000_000;
        assert_eq!(calculate_delta(current_large, last_32), current_large);

        // 64-bit rollover (extremely unlikely but handled)
        let last_64 = u64::MAX - 499;
        assert_eq!(calculate_delta(200, last_64), 700);
    }
}

