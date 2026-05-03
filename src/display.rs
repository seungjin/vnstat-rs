use crate::models::{HistoryEntry, SummaryData, NintyFifthData};
use crate::utils::format_bytes;
use chrono::{DateTime, Datelike, Local, TimeZone};

pub fn print_summary_table(summaries: Vec<SummaryData>, _machine_id: &str) {
    if summaries.is_empty() {
        println!("No data available for the selected host(s).");
        return;
    }

    // Group by hostname
    let mut by_host: std::collections::HashMap<String, Vec<SummaryData>> = std::collections::HashMap::new();
    for s in summaries {
        by_host.entry(s.hostname.clone()).or_default().push(s);
    }

    let mut hostnames: Vec<_> = by_host.keys().cloned().collect();
    hostnames.sort();

    let now = Local::now();
    let now_ts = now.timestamp();
    let today_start = now.date_naive().and_hms_opt(0, 0, 0).unwrap();
    let today_ts = Local.from_local_datetime(&today_start).unwrap().timestamp();

    let this_month_start = now.date_naive().with_day(1).unwrap().and_hms_opt(0, 0, 0).unwrap();
    let this_month_ts = Local.from_local_datetime(&this_month_start).unwrap().timestamp();
    
    let last_month_date = if now.month() == 1 {
        now.date_naive().with_year(now.year() - 1).unwrap().with_month(12).unwrap().with_day(1).unwrap()
    } else {
        now.date_naive().with_month(now.month() - 1).unwrap().with_day(1).unwrap()
    };
    let last_month_ts = Local.from_local_datetime(&last_month_date.and_hms_opt(0, 0, 0).unwrap()).unwrap().timestamp();

    for hostname in hostnames {
        println!();
        println!("{:-<73}", format!(" Host: {} ", hostname));
        println!("                      rx      /      tx      /     total    /   estimated");
        
        let mut host_summaries = by_host.remove(&hostname).unwrap();
        host_summaries.sort_by(|a, b| a.name.cmp(&b.name));

        for summary in host_summaries {
            if summary.name == "lo" {
                continue;
            }
            println!("   {}:", summary.name);

            let print_line = |label: &str, rx: u64, tx: u64, est: Option<String>| {
                let total = rx + tx;
                print!("      {:<12} {:>10}  /  {:>10}  /  {:>10}", 
                    label, format_bytes_short(rx), format_bytes_short(tx), format_bytes_short(total));
                if let Some(e) = est {
                    println!("  /  {:>10}", e);
                } else {
                    println!();
                }
            };

            // Monthly lines
            let last_month_label = DateTime::from_timestamp(last_month_ts, 0).unwrap().format("%Y-%m").to_string();
            let (lm_rx, lm_tx) = summary.last_month;
            print_line(&last_month_label, lm_rx, lm_tx, None);

            let this_month_label = DateTime::from_timestamp(this_month_ts, 0).unwrap().format("%Y-%m").to_string();
            let (tm_rx, tm_tx) = summary.this_month;
            
            let days_in_month = match now.month() {
                1|3|5|7|8|10|12 => 31,
                4|6|9|11 => 30,
                2 => if (now.year() % 4 == 0 && now.year() % 100 != 0) || (now.year() % 400 == 0) { 29 } else { 28 },
                _ => 30,
            };
            let current_day = now.day() as f64;
            let tm_est = if current_day > 0.0 {
                format_bytes_short(((tm_rx + tm_tx) as f64 * (days_in_month as f64 / current_day)) as u64)
            } else {
                "--".to_string()
            };
            print_line(&this_month_label, tm_rx, tm_tx, Some(tm_est));

            // Yesterday
            let (y_rx, y_tx) = summary.yesterday;
            print_line("yesterday", y_rx, y_tx, None);

            // Today
            let (t_rx, t_tx) = summary.today;
            let secs_passed = (now_ts - today_ts).max(1) as f64;
            let t_est = format_bytes_short(((t_rx + t_tx) as f64 * (86400.0 / secs_passed)) as u64);
            print_line("today", t_rx, t_tx, Some(t_est));
        }
    }
}

pub fn print_history_table(table: &str, mut history: Vec<HistoryEntry>, limit: usize) {
    if history.is_empty() {
        println!("No data available.");
        return;
    }

    history.sort_by_key(|h| h.date);

    let mut by_host_and_interface: std::collections::HashMap<String, std::collections::HashMap<String, Vec<HistoryEntry>>> = std::collections::HashMap::new();
    for entry in history {
        by_host_and_interface.entry(entry.hostname.clone()).or_default()
            .entry(entry.interface.clone()).or_default().push(entry);
    }

    let mut hostnames: Vec<_> = by_host_and_interface.keys().cloned().collect();
    hostnames.sort();

    let now = Local::now();
    let now_ts = now.timestamp();

    for hostname in hostnames {
        println!();
        println!("{:-<73}", format!(" Host: {} ", hostname));

        let interface_map = by_host_and_interface.remove(&hostname).unwrap();
        let mut interfaces: Vec<_> = interface_map.keys().cloned().collect();
        interfaces.sort();

        for iface in interfaces {
            if iface == "lo" {
                continue;
            }
            let entries = interface_map.get(&iface).unwrap();
            let title = match table {
                "fiveminute" => "five minute".to_string(),
                "hour" => "hourly".to_string(),
                "day" => "daily".to_string(),
                "month" => "monthly".to_string(),
                "year" => "yearly".to_string(),
                "top" => format!("top {}", limit),
                _ => table.to_string(),
            };

            println!("\n {}  /  {}\n", iface, title);
            
            let (label_header, separator_indent) = match table {
                "hour" => ("         hour        rx      ", 5),
                "fiveminute" => ("         time        rx      ", 5),
                "day" => ("          day         rx      ", 5),
                "month" => ("        month        rx      ", 5),
                "year" => ("          year        rx      ", 5),
                _ => ("          date        rx      ", 5),
            };

            println!("{}|     tx      |    total    |   avg. rate", label_header);
            println!("{:indent$}------------------------+-------------+-------------+---------------", "", indent = separator_indent);

            let mut last_date = String::new();

            for entry in entries {
                let dt = DateTime::from_timestamp(entry.date, 0).unwrap();
                let date_str = dt.format("%Y-%m-%d").to_string();
                
                if (table == "hour" || table == "fiveminute") && date_str != last_date {
                    println!("     {}", date_str);
                    last_date = date_str;
                }

                let label = match table {
                    "hour" | "fiveminute" => dt.format("    %H:%M").to_string(),
                    "day" => dt.format("%Y-%m-%d").to_string(),
                    "month" => dt.format("%Y-%m").to_string(),
                    "year" => dt.format("%Y").to_string(),
                    _ => dt.format("%Y-%m-%d").to_string(),
                };

                let total = entry.rx + entry.tx;
                let seconds = match table {
                    "fiveminute" => 300,
                    "hour" => 3600,
                    "day" => 86400,
                    "month" => {
                        let year = dt.year();
                        let month = dt.month();
                        let days = match month {
                            1|3|5|7|8|10|12 => 31,
                            4|6|9|11 => 30,
                            2 => if (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0) { 29 } else { 28 },
                            _ => 30,
                        };
                        days * 86400
                    },
                    "year" => {
                        if (dt.year() % 4 == 0 && dt.year() % 100 != 0) || (dt.year() % 400 == 0) { 366 * 86400 } else { 365 * 86400 }
                    },
                    _ => 86400,
                };

                let rate_bits = (total * 8) as f64 / seconds as f64;
                let rate_str = format_rate(rate_bits);

                let rx_str = format_bytes_short(entry.rx);
                let tx_str = format_bytes_short(entry.tx);
                let total_str = format_bytes_short(total);

                let label_part = match table {
                    "hour" | "fiveminute" => format!("         {:<10}{:>9} ", dt.format("%H:%M"), rx_str),
                    "month" => format!("       {:<7}    {:>10} ", label, rx_str),
                    "day" => format!("      {:<10}  {:>10} ", label, rx_str),
                    "year" => format!("        {:<4}       {:>10} ", label, rx_str),
                    _ => format!("     {:<16} {:>10} ", label, rx_str),
                };

                if table == "hour" || table == "fiveminute" {
                    println!("{}|  {:>10} |  {:>10} |  {:>13}", 
                        label_part, tx_str, total_str, rate_str);
                } else {
                    println!("{}|  {:>10} |  {:>10} |    {:>11}", 
                        label_part, tx_str, total_str, rate_str);
                }
            }

            println!("{:indent$}------------------------+-------------+-------------+---------------", "", indent = separator_indent);

            if let Some(latest) = entries.last() {
                let dt = DateTime::from_timestamp(latest.date, 0).unwrap();
                let is_current = match table {
                    "day" => dt.date_naive() == now.date_naive(),
                    "month" => dt.year() == now.year() && dt.month() == now.month(),
                    "year" => dt.year() == now.year(),
                    _ => false,
                };

                if is_current {
                    let (secs_passed, total_secs) = match table {
                        "day" => {
                            let today_start = now.date_naive().and_hms_opt(0, 0, 0).unwrap();
                            let start_ts = Local.from_local_datetime(&today_start).unwrap().timestamp();
                            ((now_ts - start_ts).max(1) as f64, 86400.0)
                        },
                        "month" => {
                            let month_start = now.date_naive().with_day(1).unwrap().and_hms_opt(0, 0, 0).unwrap();
                            let start_ts = Local.from_local_datetime(&month_start).unwrap().timestamp();
                            let days = match now.month() {
                                1|3|5|7|8|10|12 => 31,
                                4|6|9|11 => 30,
                                2 => if (now.year() % 4 == 0 && now.year() % 100 != 0) || (now.year() % 400 == 0) { 29 } else { 28 },
                                _ => 30,
                            };
                            ((now_ts - start_ts).max(1) as f64, (days * 86400) as f64)
                        },
                        "year" => {
                            let year_start = now.date_naive().with_month(1).unwrap().with_day(1).unwrap().and_hms_opt(0, 0, 0).unwrap();
                            let start_ts = Local.from_local_datetime(&year_start).unwrap().timestamp();
                            let days = if (now.year() % 4 == 0 && now.year() % 100 != 0) || (now.year() % 400 == 0) { 366 } else { 365 };
                            ((now_ts - start_ts).max(1) as f64, (days * 86400) as f64)
                        },
                        _ => (1.0, 1.0),
                    };

                    if total_secs > 1.0 {
                        let est_rx = (latest.rx as f64 * (total_secs / secs_passed)) as u64;
                        let est_tx = (latest.tx as f64 * (total_secs / secs_passed)) as u64;
                        let est_total = est_rx + est_tx;

                        println!("     {:<12} {:>10} |  {:>10} |  {:>10} |", 
                            "estimated", format_bytes_short(est_rx), format_bytes_short(est_tx), format_bytes_short(est_total));
                    }
                }
            }
        }
    }
}

pub fn print_95th_table(data: NintyFifthData, five_minute_hours: u32) {
    let now = Local::now();
    let days_in_month = match now.month() {
        1|3|5|7|8|10|12 => 31,
        4|6|9|11 => 30,
        2 => if (now.year() % 4 == 0 && now.year() % 100 != 0) || (now.year() % 400 == 0) { 29 } else { 28 },
        _ => 30,
    };
    let required_hours = days_in_month * 24;

    if five_minute_hours < required_hours {
        println!("\nWarning: Configuration \"5MinuteHours\" needs to be at least {} for 100% coverage.", required_hours);
        println!("         \"5MinuteHours\" is currently set at {}.\n", five_minute_hours);
    }

    println!(" {}  /  95th percentile\n", data.interface);
    
    let begin_dt = DateTime::from_timestamp(data.begin, 0).unwrap();
    let end_dt = DateTime::from_timestamp(data.end, 0).unwrap();
    
    println!(" {} - {} ({} entries, {:.1}% coverage)\n", 
        begin_dt.format("%Y-%m-%d %H:%M"), 
        end_dt.format("%Y-%m-%d %H:%M"),
        data.count, data.coverage);

    println!("                          rx       |       tx       |     total");
    println!("       ----------------------------+----------------+---------------");

    let calculate_stats = |v: &[u64]| -> (f64, f64, f64, f64) {
        if v.is_empty() { return (0.0, 0.0, 0.0, 0.0); }
        let mut sorted = v.to_vec();
        sorted.sort();
        
        let min = sorted[0] as f64;
        let max = sorted[sorted.len()-1] as f64;
        let avg = v.iter().sum::<u64>() as f64 / v.len() as f64;
        
        let idx = (0.95 * (sorted.len() as f64 - 1.0)) as usize;
        let ninty_fifth = sorted[idx] as f64;
        
        // Data is in bytes per 5 minutes. Convert to bits per second.
        (min * 8.0 / 300.0, avg * 8.0 / 300.0, max * 8.0 / 300.0, ninty_fifth * 8.0 / 300.0)
    };

    let (rx_min, rx_avg, rx_max, rx_95) = calculate_stats(&data.rx);
    let (tx_min, tx_avg, tx_max, tx_95) = calculate_stats(&data.tx);
    
    let total_v: Vec<u64> = data.rx.iter().zip(data.tx.iter()).map(|(r, t)| r + t).collect();
    let (total_min, total_avg, total_max, total_95) = calculate_stats(&total_v);

    println!("       {:<12} {:>14} | {:>14} | {:>14}", "minimum", format_rate(rx_min), format_rate(tx_min), format_rate(total_min));
    println!("       {:<12} {:>14} | {:>14} | {:>14}", "average", format_rate(rx_avg), format_rate(tx_avg), format_rate(total_avg));
    println!("       {:<12} {:>14} | {:>14} | {:>14}", "maximum", format_rate(rx_max), format_rate(tx_max), format_rate(total_max));
    println!("       ----------------------------+----------------+---------------");
    println!("        95th %      {:>14} | {:>14} | {:>14}", format_rate(rx_95), format_rate(tx_95), format_rate(total_95));
}

pub fn format_bytes_short(bytes: u64) -> String {
    format_bytes(bytes)
}

pub fn format_rate(bits_per_sec: f64) -> String {
    if bits_per_sec >= 1_000_000_000_000.0 {
        format!("{:.2} Tbit/s", bits_per_sec / 1_000_000_000_000.0)
    } else if bits_per_sec >= 1_000_000_000.0 {
        format!("{:.2} Gbit/s", bits_per_sec / 1_000_000_000.0)
    } else if bits_per_sec >= 1_000_000.0 {
        format!("{:.2} Mbit/s", bits_per_sec / 1_000_000.0)
    } else if bits_per_sec >= 1_000.0 {
        format!("{:.2} kbit/s", bits_per_sec / 1_000.0)
    } else {
        format!("{:.2} bit/s", bits_per_sec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_rate() {
        assert_eq!(format_rate(1_500_000_000_000.0), "1.50 Tbit/s");
        assert_eq!(format_rate(1_000_000_000.0), "1.00 Gbit/s");
        assert_eq!(format_rate(500_000_000.0), "500.00 Mbit/s");
        assert_eq!(format_rate(389_393_090.0), "389.39 Mbit/s");
        assert_eq!(format_rate(9_931_520.0), "9.93 Mbit/s");
        assert_eq!(format_rate(500_000.0), "500.00 kbit/s");
        assert_eq!(format_rate(500.0), "500.00 bit/s");
    }
}
