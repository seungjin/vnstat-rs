use anyhow::{Result};
use chrono::Datelike;
use clap::{Parser};
use std::path::{PathBuf};
use tokio::net::UnixStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use vnstat_rs::{Db, load_config, parse_net_dev, format_bytes, IpcRequest, IpcResponse};

async fn request_daemon(socket_path: &PathBuf, req: IpcRequest) -> Result<IpcResponse> {
    let mut stream = UnixStream::connect(socket_path).await?;
    let req_json = serde_json::to_vec(&req)?;
    stream.write_all(&req_json).await?;
    
    let mut response_buffer = Vec::new();
    stream.read_to_end(&mut response_buffer).await?;
    
    let resp: IpcResponse = serde_json::from_slice(&response_buffer)?;
    Ok(resp)
}

fn parse_date_arg(date_str: &str) -> Option<i64> {
    // Try YYYY-MM-DD
    if let Ok(ndt) = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
        return Some(ndt.and_hms_opt(0, 0, 0).unwrap().and_local_timezone(chrono::Utc).unwrap().timestamp());
    }
    // Try YYYY-MM
    if let Ok(ndt) = chrono::NaiveDate::parse_from_str(&format!("{}-01", date_str), "%Y-%m-%d") {
        return Some(ndt.and_hms_opt(0, 0, 0).unwrap().and_local_timezone(chrono::Utc).unwrap().timestamp());
    }
    None
}

async fn run_live(iface: Option<String>) -> Result<()> {
    let stats = vnstat_rs::parse_net_dev()?;
    let selected_iface = if let Some(filter) = iface {
        stats.iter().find(|s| s.name == filter).map(|s| s.name.clone())
            .ok_or_else(|| anyhow::anyhow!("Interface \"{}\" not found.", filter))?
    } else {
        // Find first interface with traffic, excluding lo
        stats.iter()
            .filter(|s| s.name != "lo" && (s.rx_bytes > 0 || s.tx_bytes > 0))
            .map(|s| s.name.clone())
            .next()
            .unwrap_or_else(|| {
                // Fallback to first non-lo
                stats.iter().find(|s| s.name != "lo").map(|s| s.name.clone())
                    .unwrap_or_else(|| stats[0].name.clone())
            })
    };

    println!("Monitoring {}...    (press CTRL-C to stop)", selected_iface);
    println!();

    let start_stats = stats.iter().find(|s| s.name == selected_iface).unwrap().clone();
    let mut last_stats = start_stats.clone();
    let start_time = std::time::Instant::now();
    let mut first_iteration = true;
    
    let mut max_rx_kbit = 0.0;
    let mut max_tx_kbit = 0.0;
    let mut total_rx_bytes = 0;
    let mut total_tx_bytes = 0;
    let mut total_rx_packets = 0;
    let mut total_tx_packets = 0;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\n");
                let duration = start_time.elapsed();
                let secs = duration.as_secs_f64();
                
                if secs >= 1.0 {
                    println!(" {}  /  traffic statistics", selected_iface);
                    println!();
                    println!("                           rx         |       tx");
                    println!("--------------------------------------+------------------");
                    println!("  bytes                {:>12}   |  {:>12}", 
                        vnstat_rs::format_bytes(total_rx_bytes), 
                        vnstat_rs::format_bytes(total_tx_bytes));
                    println!("--------------------------------------+------------------");
                    println!("  max                {:>10.2} kbit/s |  {:>10.2} kbit/s", max_rx_kbit, max_tx_kbit);
                    println!("  average            {:>10.2} kbit/s |  {:>10.2} kbit/s", 
                        (total_rx_bytes as f64 * 8.0) / (secs * 1000.0),
                        (total_tx_bytes as f64 * 8.0) / (secs * 1000.0));
                    println!("--------------------------------------+------------------");
                    println!("  packets              {:>12}   |  {:>12}", total_rx_packets, total_tx_packets);
                    println!("--------------------------------------+------------------");
                    println!("  average p/s          {:>10}     |  {:>10}", 
                        (total_rx_packets as f64 / secs) as u64,
                        (total_tx_packets as f64 / secs) as u64);
                    println!("--------------------------------------+------------------");
                    println!("  time                 {:>10.2} seconds", secs);
                }
                return Ok(());
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                let current_stats_all = vnstat_rs::parse_net_dev()?;
                let curr = current_stats_all.into_iter().find(|s| s.name == selected_iface)
                    .ok_or_else(|| anyhow::anyhow!("Interface {} disappeared", selected_iface))?;
                
                if !first_iteration {
                    print!("\x1B[1A");
                }
                first_iteration = false;

                let rx_bytes_delta = if curr.rx_bytes >= last_stats.rx_bytes { curr.rx_bytes - last_stats.rx_bytes } else { 0 };
                let tx_bytes_delta = if curr.tx_bytes >= last_stats.tx_bytes { curr.tx_bytes - last_stats.tx_bytes } else { 0 };
                let rx_packets_delta = if curr.rx_packets >= last_stats.rx_packets { curr.rx_packets - last_stats.rx_packets } else { 0 };
                let tx_packets_delta = if curr.tx_packets >= last_stats.tx_packets { curr.tx_packets - last_stats.tx_packets } else { 0 };
                
                let rx_kbit = (rx_bytes_delta as f64 * 8.0) / 1000.0;
                let tx_kbit = (tx_bytes_delta as f64 * 8.0) / 1000.0;

                if rx_kbit > max_rx_kbit { max_rx_kbit = rx_kbit; }
                if tx_kbit > max_tx_kbit { max_tx_kbit = tx_kbit; }
                total_rx_bytes += rx_bytes_delta;
                total_tx_bytes += tx_bytes_delta;
                total_rx_packets += rx_packets_delta;
                total_tx_packets += tx_packets_delta;

                println!("   rx: {:>10.2} kbit/s {:>5} p/s          tx: {:>10.2} kbit/s {:>5} p/s\x1B[K", 
                    rx_kbit, rx_packets_delta, tx_kbit, tx_packets_delta);

                last_stats = curr.clone();
                use std::io::Write;
                std::io::stdout().flush()?;
            }
        }
    }
}

#[derive(Parser)]
#[command(
    author, 
    about = "A Rust port of vnStat", 
    long_about = None, 
    disable_help_flag = true,
    disable_version_flag = true
)]
struct Cli {
    /// Show help
    #[arg(short = '?', long = "help")]
    help: bool,

    /// Print version
    #[arg(short = 'V', long = "version")]
    version: bool,

    /// Select interface
    #[arg(short, long, value_name = "iface")]
    iface: Option<String>,

    /// Show 5 minutes statistics
    #[arg(short = '5', long = "fiveminutes", num_args = 0..=1)]
    fiveminutes: Option<Option<usize>>,

    /// Show hourly statistics
    #[arg(short = 'h', long, num_args = 0..=1)]
    hours: Option<Option<usize>>,

    /// Show hours graph (not implemented)
    #[arg(long = "hoursgraph")]
    hoursgraph: bool,

    /// Show daily statistics
    #[arg(short = 'd', long, num_args = 0..=1)]
    days: Option<Option<usize>>,

    /// Show monthly statistics
    #[arg(short = 'm', long, num_args = 0..=1)]
    months: Option<Option<usize>>,

    /// Show yearly statistics
    #[arg(short = 'y', long, num_args = 0..=1)]
    years: Option<Option<usize>>,

    /// Show top 10 days
    #[arg(short, long, num_args = 0..=1)]
    top: Option<Option<usize>>,

    /// Set list begin date
    #[arg(short, long, value_name = "date")]
    begin: Option<String>,

    /// Set list end date
    #[arg(short, long, value_name = "date")]
    end: Option<String>,

    /// Show 95th percentile (not implemented)
    #[arg(long = "95th")]
    nintyfifth: bool,

    /// Show simple parsable format
    #[arg(long, num_args = 0..=1)]
    oneline: Option<Option<String>>,

    /// Show database in json format
    #[arg(long, num_args = 0..=2)]
    json: Option<Vec<String>>,

    /// Show database in xml format
    #[arg(long, num_args = 0..=2)]
    xml: Option<Vec<String>>,

    /// Calculate traffic
    #[arg(short = 't', long = "traffic", num_args = 0..=1)]
    traffic: Option<Option<String>>,

    /// Show transfer rate in real time
    #[arg(short = 'l', long = "live", num_args = 0..=1)]
    live: Option<Option<String>>,

    /// Use short output
    #[arg(short, long)]
    short: bool,

    /// Update database
    #[arg(short = 'u', long)]
    update: bool,

    /// Initialize the database
    #[arg(long)]
    init: bool,

    /// List interfaces
    #[arg(long)]
    iflist: bool,

    /// Path to the database directory/file
    #[arg(short = 'D', long, value_name = "FILE")]
    dbdir: Option<PathBuf>,

    /// Path to config file
    #[arg(long, value_name = "FILE", default_value = "/etc/vnstat-rs.conf")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    if cli.help {
        use clap::CommandFactory;
        Cli::command().print_help()?;
        return Ok(());
    }

    if cli.version {
        println!("vnStat-rs {} by Seungjin Kim (Turso {})", env!("CARGO_PKG_VERSION"), env!("TURSO_VERSION"));
        return Ok(());
    }

    if cli.live.is_some() {
        run_live(cli.iface.clone()).await?;
        return Ok(());
    }

    let file_config = load_config(&cli.config);
    
    // Determine output format
    enum OutputFormat { Table, Json, Xml, Oneline }
    let format = if cli.json.is_some() { OutputFormat::Json }
        else if cli.xml.is_some() { OutputFormat::Xml }
        else if cli.oneline.is_some() { OutputFormat::Oneline }
        else { OutputFormat::Table };

    // Try to talk to daemon first
    if let Some(ref socket_path) = file_config.daemon_socket {
        if socket_path.exists() {
            let mut requested_table = String::new();
            let req = if cli.fiveminutes.is_some() || cli.hours.is_some() || cli.days.is_some() || cli.months.is_some() || cli.years.is_some() || cli.top.is_some() {
                let (table, limit) = if let Some(l) = cli.fiveminutes { ("fiveminute", l.unwrap_or(30)) }
                    else if let Some(l) = cli.hours { ("hour", l.unwrap_or(30)) }
                    else if let Some(l) = cli.days { ("day", l.unwrap_or(30)) }
                    else if let Some(l) = cli.months { ("month", l.unwrap_or(12)) }
                    else if let Some(l) = cli.years { ("year", l.unwrap_or(10)) }
                    else { ("top", cli.top.unwrap().unwrap_or(10)) };
                
                requested_table = table.to_string();
                let begin = cli.begin.as_deref().and_then(parse_date_arg);
                let end = cli.end.as_deref().and_then(parse_date_arg);

                Some(IpcRequest::GetHistory { 
                    table: table.to_string(), 
                    interface: cli.iface.clone(), 
                    limit,
                    begin,
                    end,
                })
            } else if !cli.update && !cli.init && !cli.iflist {
                if matches!(format, OutputFormat::Table) {
                    Some(IpcRequest::GetSummary { interface: cli.iface.clone() })
                } else {
                    Some(IpcRequest::GetStats { interface: cli.iface.clone() })
                }
            } else {
                None
            };

            if let Some(req) = req {
                match request_daemon(socket_path, req).await {
                    Ok(IpcResponse::Stats(stats)) => {
                        match format {
                            OutputFormat::Json => println!("{}", serde_json::to_string(&vnstat_rs::VnStatJson::new(stats))?),
                            OutputFormat::Xml => println!("{}", vnstat_rs::VnStatJson::new(stats).to_xml()),
                            OutputFormat::Oneline => {
                                for s in stats {
                                    println!("1;{};{};{};{};{};", s.hostname, s.name, s.rx_bytes, s.tx_bytes, s.rx_bytes + s.tx_bytes);
                                }
                            }
                            OutputFormat::Table => {}
                        }
                        return Ok(());
                    }
                    Ok(IpcResponse::Summary(summaries)) => {
                        print_summary_table(summaries);
                        return Ok(());
                    }
                    Ok(IpcResponse::History(history)) => {
                        match format {
                            OutputFormat::Json => println!("{}", serde_json::to_string(&vnstat_rs::VnStatJson::from_history(history, &requested_table))?),
                            OutputFormat::Xml => println!("{}", vnstat_rs::VnStatJson::from_history(history, &requested_table).to_xml()),
                            OutputFormat::Oneline => {
                                for h in history {
                                    println!("h;{};{};{};{};{};", h.hostname, h.interface, h.date, h.rx, h.tx);
                                }
                            }
                            OutputFormat::Table => {
                                println!("{:<20} {:<15} {:<20} {:<15} {:<15}", "Host", "Interface", "Date", "RX", "TX");
                                for entry in history {
                                    let date_str = chrono::DateTime::from_timestamp(entry.date, 0)
                                        .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
                                        .unwrap_or_else(|| entry.date.to_string());

                                    println!("{:<20} {:<15} {:<20} {:<15} {:<15}", 
                                        entry.hostname, entry.interface, date_str, format_bytes(entry.rx), format_bytes(entry.tx));
                                }
                            }
                        }
                        return Ok(());
                    }
                    Ok(IpcResponse::Error(e)) => {
                        eprintln!("Daemon error: {}", e);
                    }
                    _ => {}
                }
            }
        }
    }
    
    let db_path = cli.dbdir
        .or(file_config.database)
        .unwrap_or_else(|| PathBuf::from("/var/lib/vnstat-rs/vnstat-rs.db"));
    
    let db = match Db::open(db_path, None, None).await {
        Ok(db) => db,
        Err(e) => {
            if e.to_string().contains("locked") {
                return Err(anyhow::anyhow!("Database is locked by another process (likely vnstatd-rs).\nTry starting the daemon or stopping it if you want direct access."));
            }
            return Err(e);
        }
    };

    if cli.init {
        println!("Database initialized for host: {} ({})", db.hostname, db.machine_id);
        return Ok(());
    }

    if cli.update {
        db.update_stats(cli.iface.as_deref()).await?;
        return Ok(());
    }

    if cli.iflist {
        let stats = parse_net_dev()?;
        println!("{:<15} {:<15} {:<15}", "Interface", "RX Total", "TX Total");
        for s in stats {
            println!("{:<15} {:<15} {:<15}", s.name, format_bytes(s.rx_bytes), format_bytes(s.tx_bytes));
        }
        return Ok(());
    }

    if cli.fiveminutes.is_some() || cli.hours.is_some() || cli.days.is_some() || cli.months.is_some() || cli.years.is_some() || cli.top.is_some() {
        let (table, limit) = if let Some(l) = cli.fiveminutes { ("fiveminute", l.unwrap_or(30)) }
            else if let Some(l) = cli.hours { ("hour", l.unwrap_or(30)) }
            else if let Some(l) = cli.days { ("day", l.unwrap_or(30)) }
            else if let Some(l) = cli.months { ("month", l.unwrap_or(12)) }
            else if let Some(l) = cli.years { ("year", l.unwrap_or(10)) }
            else { ("top", cli.top.unwrap().unwrap_or(10)) };

        let begin = cli.begin.as_deref().and_then(parse_date_arg);
        let end = cli.end.as_deref().and_then(parse_date_arg);

        let history = db.get_history(table, cli.iface.as_deref(), limit, begin, end).await?;
        
        match format {
            OutputFormat::Json => println!("{}", serde_json::to_string(&vnstat_rs::VnStatJson::from_history(history, table))?),
            OutputFormat::Xml => println!("{}", vnstat_rs::VnStatJson::from_history(history, table).to_xml()),
            OutputFormat::Oneline => {
                for h in history {
                    println!("h;{};{};{};{};{};", h.hostname, h.interface, h.date, h.rx, h.tx);
                }
            }
            OutputFormat::Table => {
                println!("{:<20} {:<15} {:<20} {:<15} {:<15}", "Host", "Interface", "Date", "RX", "TX");
                for entry in history {
                    let date_str = chrono::DateTime::from_timestamp(entry.date, 0)
                        .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
                        .unwrap_or_else(|| entry.date.to_string());

                    println!("{:<20} {:<15} {:<20} {:<15} {:<15}", 
                        entry.hostname, entry.interface, date_str, format_bytes(entry.rx), format_bytes(entry.tx));
                }
            }
        }
        return Ok(());
    }

    if !matches!(format, OutputFormat::Table) {
        let stats = db.get_all_interface_stats(cli.iface.as_deref()).await?;
        match format {
            OutputFormat::Json => println!("{}", serde_json::to_string(&vnstat_rs::VnStatJson::new(stats))?),
            OutputFormat::Xml => println!("{}", vnstat_rs::VnStatJson::new(stats).to_xml()),
            OutputFormat::Oneline => {
                for s in stats {
                    println!("1;{};{};{};{};{};", s.hostname, s.name, s.rx_bytes, s.tx_bytes, s.rx_bytes + s.tx_bytes);
                }
            }
            _ => unreachable!(),
        }
        return Ok(());
    }

    // Default Table view (vnstat summary)
    let summaries = db.get_summary(cli.iface.as_deref()).await?;
    print_summary_table(summaries);

    Ok(())
}

fn print_summary_table(summaries: Vec<vnstat_rs::SummaryData>) {
    if summaries.is_empty() {
        println!("No interfaces found.");
        return;
    }

    println!("\n                      rx      /      tx      /     total    /   estimated");

    let now = chrono::Utc::now();
    let now_ts = now.timestamp();
    let today_start = now.date_naive().and_hms_opt(0, 0, 0).unwrap();
    let today_ts = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(today_start, chrono::Utc).timestamp();

    let this_month_start = now.date_naive().with_day(1).unwrap().and_hms_opt(0, 0, 0).unwrap();
    let this_month_ts = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(this_month_start, chrono::Utc).timestamp();
    
    // last month
    let last_month_date = if now.month() == 1 {
        now.date_naive().with_year(now.year() - 1).unwrap().with_month(12).unwrap().with_day(1).unwrap()
    } else {
        now.date_naive().with_month(now.month() - 1).unwrap().with_day(1).unwrap()
    };
    let last_month_ts = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(last_month_date.and_hms_opt(0, 0, 0).unwrap(), chrono::Utc).timestamp();

    for summary in summaries {
        println!(" {}:", summary.name);

        // Helper to print line
        let print_line = |label: &str, rx: u64, tx: u64, est: Option<String>| {
            let total = rx + tx;
            print!("    {:<12} {:>10}  /  {:>10}  /  {:>10}", 
                label, format_bytes_short(rx), format_bytes_short(tx), format_bytes_short(total));
            if let Some(e) = est {
                println!("  /  {:>10}", e);
            } else {
                println!();
            }
        };

        // Monthly lines
        let last_month_label = chrono::DateTime::from_timestamp(last_month_ts, 0).unwrap().format("%Y-%m").to_string();
        let (lm_rx, lm_tx) = summary.last_month;
        print_line(&last_month_label, lm_rx, lm_tx, None);

        let this_month_label = chrono::DateTime::from_timestamp(this_month_ts, 0).unwrap().format("%Y-%m").to_string();
        let (tm_rx, tm_tx) = summary.this_month;
        
        // Month estimation
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
        
        println!();
    }
}

fn format_bytes_short(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.2} TiB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GiB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MiB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KiB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}
