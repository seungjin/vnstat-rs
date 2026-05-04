use anyhow::{Result};
use clap::{Parser};
use std::path::{PathBuf};
use tokio::net::UnixStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use vnstat_rs::{Db, IpcRequest, IpcResponse, print_summary_table, print_history_table, print_95th_table, print_hosts_table, format_rate};
use chrono::{Local, TimeZone};

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
        return Some(Local.from_local_datetime(&ndt.and_hms_opt(0, 0, 0).unwrap()).unwrap().timestamp());
    }
    // Try YYYY-MM
    if let Ok(ndt) = chrono::NaiveDate::parse_from_str(&format!("{}-01", date_str), "%Y-%m-%d") {
        return Some(Local.from_local_datetime(&ndt.and_hms_opt(0, 0, 0).unwrap()).unwrap().timestamp());
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
        let found = stats.iter()
            .filter(|s| s.name != "lo" && (s.rx_bytes > 0 || s.tx_bytes > 0))
            .map(|s| s.name.clone())
            .next();
            
        match found {
            Some(name) => name,
            None => {
                // Fallback to first non-lo
                stats.iter().find(|s| s.name != "lo").map(|s| s.name.clone())
                    .unwrap_or_else(|| {
                        if stats.is_empty() {
                            "eth0".to_string() // Very last resort fallback
                        } else {
                            stats[0].name.clone()
                        }
                    })
            }
        }
    };

    println!("Monitoring {}...    (press CTRL-C to stop)", selected_iface);
    println!();

    let start_stats = stats.iter().find(|s| s.name == selected_iface).unwrap().clone();
    let mut last_stats = start_stats.clone();
    let start_time = std::time::Instant::now();
    let mut first_iteration = true;
    
    let mut max_rx_bps = 0.0;
    let mut max_tx_bps = 0.0;
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
                    println!("  max                {:>15} |  {:>15}", format_rate(max_rx_bps), format_rate(max_tx_bps));
                    println!("  average            {:>15} |  {:>15}", 
                        format_rate((total_rx_bytes as f64 * 8.0) / secs),
                        format_rate((total_tx_bytes as f64 * 8.0) / secs));
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
                
                let rx_bps = rx_bytes_delta as f64 * 8.0;
                let tx_bps = tx_bytes_delta as f64 * 8.0;

                if rx_bps > max_rx_bps { max_rx_bps = rx_bps; }
                if tx_bps > max_tx_bps { max_tx_bps = tx_bps; }
                total_rx_bytes += rx_bytes_delta;
                total_tx_bytes += tx_bytes_delta;
                total_rx_packets += rx_packets_delta;
                total_tx_packets += tx_packets_delta;

                println!("   rx: {:>15} {:>5} p/s          tx: {:>15} {:>5} p/s\x1B[K", 
                    format_rate(rx_bps), rx_packets_delta, format_rate(tx_bps), tx_packets_delta);

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

    /// Show long help
    #[arg(long = "longhelp")]
    longhelp: bool,

    /// Print version
    #[arg(short = 'V', long = "version")]
    version: bool,

    /// Select interface
    #[arg(short, long, value_name = "iface")]
    iface: Option<String>,

    /// Select host
    #[arg(long, value_name = "hostname")]
    host: Option<String>,

    /// Show statistics for all hosts
    #[arg(long)]
    all_hosts: bool,

    /// List all hosts in database
    #[arg(long)]
    host_list: bool,

    /// Show 5 minutes statistics
    #[arg(short = '5', long = "fiveminutes", num_args = 0..=1)]
    fiveminutes: Option<Option<usize>>,

    /// Show hourly statistics
    #[arg(short = 'h', long, num_args = 0..=1)]
    hours: Option<Option<usize>>,

    /// Show hours graph
    #[arg(long = "hg", alias = "hoursgraph")]
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
    #[arg(short = 't', long = "top", num_args = 0..=1)]
    top: Option<Option<usize>>,

    /// Set list begin date
    #[arg(short, long, value_name = "date")]
    begin: Option<String>,

    /// Set list end date
    #[arg(short, long, value_name = "date")]
    end: Option<String>,

    /// Show 95th percentile
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
    #[arg(long = "tr", alias = "traffic", num_args = 0..=1)]
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

    /// Set output entry limit
    #[arg(short = 'L', long, value_name = "limit")]
    limit: Option<usize>,

    /// Path to the database directory/file
    #[arg(short = 'D', long, value_name = "FILE")]
    dbdir: Option<PathBuf>,

    /// Path to config file
    #[arg(short = 'n', long, value_name = "FILE")]
    config: Option<PathBuf>,

    /// Add interface to database
    #[arg(long, value_name = "iface")]
    add: Option<String>,

    /// Remove interface from database
    #[arg(long, value_name = "iface")]
    remove: Option<String>,

    /// Rename interface in database
    #[arg(long, value_names = ["old", "new"])]
    rename: Option<Vec<String>>,

    /// Set alias for interface
    #[arg(long, value_name = "alias")]
    setalias: Option<String>,

    /// Show daemon information
    #[arg(long)]
    info: bool,
}

fn print_help() {
    println!("vnStat-rs {} by Seungjin Kim", env!("CARGO_PKG_VERSION"));
    println!();
    println!("      -5,  --fiveminutes [limit]   show 5 minutes");
    println!("      -h,  --hours [limit]         show hours");
    println!("      -hg, --hg                    show hours graph");
    println!("      -d,  --days [limit]          show days");
    println!("      -m,  --months [limit]        show months");
    println!("      -y,  --years [limit]         show years");
    println!("      -t,  --top [limit]           show top days");
    println!();
    println!("      -b, --begin <date>           set list begin date");
    println!("      -e, --end <date>             set list end date");
    println!();
    println!("      --95th                       show 95th percentile");
    println!("      --oneline [mode]             show simple parsable format");
    println!("      --json [mode] [limit]        show database in json format");
    println!("      --xml [mode] [limit]         show database in xml format");
    println!();
    println!("      -tr, --tr [time]             calculate traffic");
    println!("      -l,  --live [mode]           show transfer rate in real time");
    println!("      -i,  --iface <interface>     select interface");
    println!();
    println!("Use \"--longhelp\" for complete list of options.");
}

fn print_longhelp() {
    println!("vnStat-rs {} by Seungjin Kim", env!("CARGO_PKG_VERSION"));
    println!();
    println!("Query:");
    println!("      -s,  --short                 use short output");
    println!("      -5,  --fiveminutes [limit]   show 5 minutes");
    println!("      -h,  --hours [limit]         show hours");
    println!("      -hg, --hg                    show hours graph");
    println!("      -d,  --days [limit]          show days");
    println!("      -m,  --months [limit]        show months");
    println!("      -y,  --years [limit]         show years");
    println!("      -t,  --top [limit]           show top days");
    println!("      -b,  --begin <date>          set list begin date");
    println!("      -e,  --end <date>            set list end date");
    println!("      --oneline [mode]             show simple parsable format");
    println!("      --json [mode] [limit]        show database in json format");
    println!("      --xml [mode] [limit]         show database in xml format");
    println!();
    println!("Modify:");
    println!("      --add <iface>                add interface to database");
    println!("      --remove <iface>             remove interface from database");
    println!("      --rename <old> <new>         rename interface in database");
    println!("      --setalias <alias>           set alias for interface");
    println!();
    println!("Misc:");
    println!("      -i,  --iface <interface>     select interface");
    println!("      -?,  --help                  show short help");
    println!("      -V,  --version               show version");
    println!("      -tr, --tr [time]             calculate traffic");
    println!("      -l,  --live [mode]           show transfer rate in real time");
    println!("      --limit <limit>              set output entry limit");
    println!("      --iflist                     show list of available interfaces");
    println!("      -D,  --dbdir <file>          select database directory");
    println!("      -n,  --config <file>         select config file");
    println!("      --longhelp                   show this help");
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    if cli.help {
        print_help();
        return Ok(());
    }

    if cli.longhelp {
        print_longhelp();
        return Ok(());
    }

    if cli.version {
        println!("vnStat-rs {} ({}) by Seungjin Kim", env!("CARGO_PKG_VERSION"), env!("GIT_HASH"));
        
        let file_config = if let Some(ref path) = cli.config {
            vnstat_rs::load_config(path).unwrap_or_else(|_| vnstat_rs::load_best_config())
        } else {
            vnstat_rs::load_best_config()
        };

        // 1. Try daemon first
        let mut daemon_connected = false;
        if let Some(ref socket_path) = file_config.daemon_socket {
            if socket_path.exists() {
                if let Ok(IpcResponse::Info { version, local_schema, remote_schema, .. }) = request_daemon(socket_path, IpcRequest::GetInfo).await {
                    println!("vnStatd-rs version: {}", version);
                    println!("Local DB Schema: v{}", local_schema);
                    if let Some(v) = remote_schema {
                        println!("Remote DB Schema: v{}", v);
                    }
                    daemon_connected = true;
                }
            }
        }

        if !daemon_connected {
            println!("vnStatd-rs: not running");
            
            // Try to open DB directly to get schema versions (no init to avoid side effects)
            let db_path = cli.dbdir.clone()
                .or(file_config.database.clone())
                .unwrap_or_else(|| PathBuf::from("/var/lib/vnstat-rs/vnstat-rs.db"));
            
            if db_path.exists() {
                if let Ok(db) = Db::open_no_init(db_path, file_config.url.clone(), file_config.token.clone()).await {
                    let local_schema = db.get_schema_version_from(&db.local_conn).await.unwrap_or(0);
                    println!("Local DB Schema: v{}", local_schema);
                    if let Some(ref remote) = db.remote_conn {
                        let remote_schema = db.get_schema_version_from(remote).await.unwrap_or(0);
                        println!("Remote DB Schema: v{}", remote_schema);
                    }
                }
            } else {
                // If the file doesn't exist, we can't show version, but let's not be silent
                // println!("Database not found at {:?}", db_path);
            }
        }
        return Ok(());
    }

    if cli.iflist {
        let mut stats = vnstat_rs::parse_net_dev()?;
        stats.retain(|s| s.rx_bytes + s.tx_bytes > 0);
        println!("{:<15} {:<15} {:<15}", "Interface", "RX Total", "TX Total");
        for s in stats {
            println!("{:<15} {:<15} {:<15}", s.name, vnstat_rs::format_bytes(s.rx_bytes), vnstat_rs::format_bytes(s.tx_bytes));
        }
        return Ok(());
    }

    if cli.live.is_some() {
        run_live(cli.iface.clone()).await?;
        return Ok(());
    }

    let is_root = unsafe { libc::getuid() == 0 };
    let etc_config = PathBuf::from("/etc/vnstat-rs.conf");
    let home = std::env::var("HOME").unwrap_or_default();
    let user_config = PathBuf::from(home).join(".config/vnstat-rs/vnstat-rs.conf");

    let file_config = if let Some(ref path) = cli.config {
        match vnstat_rs::load_config(path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Error loading config {}: {}", path.display(), e);
                std::process::exit(1);
            }
        }
    } else {
        match vnstat_rs::load_config(&etc_config) {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                match vnstat_rs::load_config(&user_config) {
                    Ok(c) => c,
                    Err(_) => vnstat_rs::get_default_config(is_root)
                }
            }
            Err(_) => {
                if !is_root {
                    match vnstat_rs::load_config(&user_config) {
                        Ok(c) => c,
                        Err(_) => vnstat_rs::get_default_config(is_root)
                    }
                } else {
                    vnstat_rs::get_default_config(is_root)
                }
            }
        }
    };
    
    // Determine output format
    enum OutputFormat { Table, Json, Xml, Oneline }
    let format = if cli.json.is_some() { OutputFormat::Json }
        else if cli.xml.is_some() { OutputFormat::Xml }
        else if cli.oneline.is_some() { OutputFormat::Oneline }
        else { OutputFormat::Table };

    // Use machine_id as the default filter for current host
    let current_machine_id = vnstat_rs::get_machine_id().ok();
    let host_filter_ipc = if cli.all_hosts { None } else { cli.host.clone().or_else(|| current_machine_id.clone()) };

    // Try to talk to daemon first
    if let Some(ref socket_path) = file_config.daemon_socket {
        if socket_path.exists() {
            let mut requested_table = String::new();
            let mut requested_limit = 0;
            let req = if cli.add.is_some() || cli.remove.is_some() || cli.rename.is_some() || cli.setalias.is_some() {
                None
            } else if cli.info {
                Some(IpcRequest::GetInfo)
            } else if cli.host_list {
                Some(IpcRequest::ListHosts { host: cli.host.clone() })
            } else if cli.hoursgraph {
                Some(IpcRequest::GetHistory { 
                    table: "hour".to_string(), 
                    interface: cli.iface.clone(), 
                    host: host_filter_ipc.clone(),
                    limit: 24,
                    begin: None,
                    end: None,
                })
            } else if cli.nintyfifth {
                Some(IpcRequest::Get95th { interface: cli.iface.clone(), host: host_filter_ipc.clone() })
            } else if cli.fiveminutes.is_some() || cli.hours.is_some() || cli.days.is_some() || cli.months.is_some() || cli.years.is_some() || cli.top.is_some() {
                let (table, default_limit) = if let Some(l) = cli.fiveminutes { ("fiveminute", l.unwrap_or(24)) }
                    else if let Some(l) = cli.hours { ("hour", l.unwrap_or(24)) }
                    else if let Some(l) = cli.days { ("day", l.unwrap_or(30)) }
                    else if let Some(l) = cli.months { ("month", l.unwrap_or(12)) }
                    else if let Some(l) = cli.years { ("year", l.unwrap_or(10)) }
                    else { ("top", cli.top.unwrap().unwrap_or(10)) };
                
                requested_table = table.to_string();
                let limit = cli.limit.unwrap_or(default_limit);
                requested_limit = limit;
                let begin = cli.begin.as_deref().and_then(parse_date_arg);
                let end = cli.end.as_deref().and_then(parse_date_arg);

                Some(IpcRequest::GetHistory { 
                    table: table.to_string(), 
                    interface: cli.iface.clone(), 
                    host: host_filter_ipc.clone(),
                    limit,
                    begin,
                    end,
                })
            } else if !cli.update && !cli.init && !cli.iflist {
                if matches!(format, OutputFormat::Table) {
                    Some(IpcRequest::GetSummary { interface: cli.iface.clone(), host: host_filter_ipc.clone() })
                } else {
                    Some(IpcRequest::GetStats { interface: cli.iface.clone(), host: host_filter_ipc.clone() })
                }
            } else {
                None
            };

            if let Some(req) = req {
                match request_daemon(socket_path, req).await {
                    Ok(IpcResponse::Stats(mut stats)) => {
                        stats.retain(|s| s.rx_bytes + s.tx_bytes > 0);
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
                        print_summary_table(summaries, current_machine_id.as_deref().unwrap_or(""));
                        return Ok(());
                    }
                    Ok(IpcResponse::History(mut history)) => {
                        history.retain(|h| h.rx + h.tx > 0);
                        match format {
                            OutputFormat::Json => println!("{}", serde_json::to_string(&vnstat_rs::VnStatJson::from_history(history, &requested_table))?),
                            OutputFormat::Xml => println!("{}", vnstat_rs::VnStatJson::from_history(history, &requested_table).to_xml()),
                            OutputFormat::Oneline => {
                                for h in history {
                                    println!("h;{};{};{};{};{};", h.hostname, h.interface, h.date, h.rx, h.tx);
                                }
                            }
                            OutputFormat::Table => {
                                if cli.hoursgraph {
                                    vnstat_rs::print_hours_graph(history);
                                } else {
                                    print_history_table(&requested_table, history, requested_limit);
                                }
                            }
                        }
                        return Ok(());
                    }
                    Ok(IpcResponse::NintyFifth(data)) => {
                        print_95th_table(data, file_config.five_minute_hours);
                        return Ok(());
                    }
                    Ok(IpcResponse::Info { hostname, machine_id, mac_address, version, local_schema, remote_schema }) => {
                        println!("vnStat-rs {} by Seungjin Kim", env!("CARGO_PKG_VERSION"));
                        println!("Daemon Host: {} ({})", hostname, machine_id);
                        println!("Daemon Version: {}", version);
                        println!("Local DB Schema: v{}", local_schema);
                        if let Some(v) = remote_schema {
                            println!("Remote DB Schema: v{}", v);
                        }
                        if let Some(mac) = mac_address {
                            println!("MAC Address: {}", mac);
                        }
                        return Ok(());
                    }
                    Ok(IpcResponse::Hosts(hosts)) => {
                        print_hosts_table(hosts);
                        return Ok(());
                    }
                    Ok(IpcResponse::Error(e)) => {
                        eprintln!("Daemon error: {}", e);
                    }
                    Err(e) => {
                        eprintln!("vnstatd is not working ({:?}): {}", socket_path, e);
                    }
                    _ => {}
                }
            }
        } else {
             // Socket doesn't exist - if not a purely local command, warn
             if !cli.update && !cli.init && !cli.iflist {
                 eprintln!("vnstatd is not working (socket {:?} not found). Falling back to direct database access.", socket_path);
             }
        }
    }
    
    let db_path = cli.dbdir
        .or(file_config.database.clone())
        .unwrap_or_else(|| PathBuf::from("/var/lib/vnstat-rs/vnstat-rs.db"));
    
    // Determine if we need a remote connection
    let (url, token) = if cli.host.is_some() || cli.all_hosts || cli.update {
        (file_config.url.clone(), file_config.token.clone())
    } else {
        (None, None)
    };

    let db = match Db::open(db_path, url, token, file_config.hostname_override.clone()).await {
        Ok(db) => db,
        Err(e) => {
            if e.to_string().contains("locked") {
                return Err(anyhow::anyhow!("Database is locked by another process (likely vnStatd-rs).\nTry starting the daemon or stopping it if you want direct access."));
            }
            return Err(e);
        }
    };

    if let Some(iface) = cli.add {
        db.create_interface(&iface, 0, 0, None).await?;
        println!("Interface \"{}\" added to database for host \"{}\".", iface, db.hostname);
        return Ok(());
    }

    if let Some(iface) = cli.remove {
        db.remove_interface(&iface).await?;
        println!("Interface \"{}\" removed from database for host \"{}\".", iface, db.hostname);
        return Ok(());
    }

    if let Some(names) = cli.rename {
        if names.len() != 2 {
            return Err(anyhow::anyhow!("Please provide both old and new names: --rename old new"));
        }
        db.rename_interface(&names[0], &names[1]).await?;
        println!("Interface \"{}\" renamed to \"{}\" for host \"{}\".", names[0], names[1], db.hostname);
        return Ok(());
    }

    if let Some(alias) = cli.setalias {
        let iface = cli.iface.ok_or_else(|| anyhow::anyhow!("Please specify interface with -i to set alias"))?;
        if let Some((id, _, _, _, _)) = db.get_interface(&iface).await? {
            db.update_interface_alias(&id, &alias).await?;
            println!("Alias for interface \"{}\" set to \"{}\".", iface, alias);
        } else {
            return Err(anyhow::anyhow!("Interface \"{}\" not found.", iface));
        }
        return Ok(());
    }

    if cli.init {
        println!("Database initialized for host: {} ({})", db.hostname, db.machine_id);
        return Ok(());
    }

    if cli.update {
        db.update_stats(cli.iface.as_deref(), &file_config).await?;
        db.prune_stats(&file_config).await?;
        return Ok(());
    }

    if cli.info {
        println!("vnStat-rs {} ({}) by Seungjin Kim", env!("CARGO_PKG_VERSION"), env!("GIT_HASH"));
        println!("Hostname: {}, Machine ID: {}", db.hostname, db.machine_id);
        let local_schema = db.get_schema_version_from(&db.local_conn).await.unwrap_or(0);
        println!("Local DB Schema: v{}", local_schema);
        if let Some(ref remote) = db.remote_conn {
            let remote_schema = db.get_schema_version_from(remote).await.unwrap_or(0);
            println!("Remote DB Schema: v{}", remote_schema);
        }
        if let Ok(Some(mac)) = db.get_info("mac_address").await {
            println!("MAC Address: {}", mac);
        }
        return Ok(());
    }

    if cli.host_list {
        let hosts = db.get_all_hosts(cli.host.as_deref()).await?;
        print_hosts_table(hosts);
        return Ok(());
    }
    let final_host_filter = if cli.all_hosts { None } else { cli.host.as_deref().or(current_machine_id.as_deref()) };

    if cli.nintyfifth {
        let data = db.get_95th_data(cli.iface.as_deref(), final_host_filter).await?;
        print_95th_table(data, file_config.five_minute_hours);
        return Ok(());
    }

    if cli.hoursgraph {
        let mut history = db.get_history("hour", cli.iface.as_deref(), final_host_filter, 24, None, None).await?;
        history.retain(|h| h.rx + h.tx > 0);
        vnstat_rs::print_hours_graph(history);
        return Ok(());
    }

    if cli.fiveminutes.is_some() || cli.hours.is_some() || cli.days.is_some() || cli.months.is_some() || cli.years.is_some() || cli.top.is_some() {
        let (table, default_limit) = if let Some(l) = cli.fiveminutes { ("fiveminute", l.unwrap_or(24)) }
            else if let Some(l) = cli.hours { ("hour", l.unwrap_or(24)) }
            else if let Some(l) = cli.days { ("day", l.unwrap_or(30)) }
            else if let Some(l) = cli.months { ("month", l.unwrap_or(12)) }
            else if let Some(l) = cli.years { ("year", l.unwrap_or(10)) }
            else { ("top", cli.top.unwrap().unwrap_or(10)) };

        let limit = cli.limit.unwrap_or(default_limit);
        let begin = cli.begin.as_deref().and_then(parse_date_arg);
        let end = cli.end.as_deref().and_then(parse_date_arg);

        let mut history = db.get_history(table, cli.iface.as_deref(), final_host_filter, limit, begin, end).await?;
        history.retain(|h| h.rx + h.tx > 0);
        
        match format {
            OutputFormat::Json => println!("{}", serde_json::to_string(&vnstat_rs::VnStatJson::from_history(history, table))?),
            OutputFormat::Xml => println!("{}", vnstat_rs::VnStatJson::from_history(history, table).to_xml()),
            OutputFormat::Oneline => {
                for h in history {
                    println!("h;{};{};{};{};{};", h.hostname, h.interface, h.date, h.rx, h.tx);
                }
            }
            OutputFormat::Table => {
                print_history_table(table, history, limit);
            }
        }
        return Ok(());
    }

    if !matches!(format, OutputFormat::Table) {
        let mut stats = db.get_all_interface_stats(cli.iface.as_deref(), final_host_filter).await?;
        stats.retain(|s| s.rx_bytes + s.tx_bytes > 0);
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
    let summaries = db.get_summary(cli.iface.as_deref(), final_host_filter).await?;
    print_summary_table(summaries, &db.machine_id);

    Ok(())
}
