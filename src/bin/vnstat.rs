use anyhow::{Result};
use clap::{Parser};
use std::path::{PathBuf};
use vnstat_rs::{Db, load_config, parse_net_dev, format_bytes};

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

    /// Show hourly statistics
    #[arg(short = 'h', long)]
    hours: bool,

    /// Show daily statistics
    #[arg(short = 'd', long)]
    days: bool,

    /// Show monthly statistics
    #[arg(short = 'm', long)]
    months: bool,

    /// Show yearly statistics
    #[arg(short = 'y', long)]
    years: bool,

    /// Show top 10 days
    #[arg(short, long)]
    top: bool,

    /// Use short output
    #[arg(short, long)]
    short: bool,

    /// Update database
    #[arg(short, long)]
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
    #[arg(long, value_name = "FILE", default_value = "/etc/vnstat.conf")]
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
        println!("vnStat-rs {} by Seungjin Kim (libSQL 0.6.0)", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    let file_config = load_config(&cli.config);
    
    let db_path = cli.dbdir
        .or(file_config.database)
        .unwrap_or_else(|| PathBuf::from("vnstat-rs.db"));
    
    let db = Db::open(db_path, None, None).await?;

    if cli.init {
        println!("Initializing database for host: {} ({})", db.hostname, db.machine_id);
        db.init_schema().await?;
        println!("Database initialized.");
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

    if cli.hours || cli.days || cli.months || cli.years || cli.top {
        let table = if cli.hours { "hour" }
            else if cli.days { "day" }
            else if cli.months { "month" }
            else if cli.years { "year" }
            else { "top" };

        let mut query = format!(
            "SELECT i.hostname, i.name, t.date, t.rx, t.tx 
             FROM interface i 
             JOIN {} t ON i.id = t.interface ", table);
        
        if let Some(ref iface) = cli.iface {
            query.push_str(&format!("WHERE i.name = '{}' ", iface));
        }
        query.push_str("ORDER BY t.date DESC LIMIT 30");

        let mut rows = db.conn.query(&query, ()).await?;
        println!("{:<20} {:<15} {:<20} {:<15} {:<15}", "Host", "Interface", "Date", "RX", "TX");
        while let Some(row) = rows.next().await? {
            let host: String = row.get(0)?;
            let name: String = row.get(1)?;
            let date: i64 = row.get(2)?;
            let rx: i64 = row.get(3)?;
            let tx: i64 = row.get(4)?;
            // Simple date format for now
            let date_str = chrono::DateTime::from_timestamp(date, 0)
                .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
                .unwrap_or_else(|| date.to_string());

            println!("{:<20} {:<15} {:<20} {:<15} {:<15}", 
                host, name, date_str, format_bytes(rx as u64), format_bytes(tx as u64));
        }
        return Ok(());
    }

    // Default: Show summary from interface table
    let mut query = "SELECT hostname, name, rxtotal, txtotal FROM interface".to_string();
    if let Some(ref iface) = cli.iface {
        query.push_str(&format!(" WHERE name = '{}'", iface));
    }
    query.push_str(" ORDER BY hostname, name");

    let mut rows = db.conn.query(&query, ()).await?;
    
    println!("{:<20} {:<15} {:<15} {:<15} {:<15}", "Host", "Interface", "Total RX", "Total TX", "Total");
    while let Some(row) = rows.next().await? {
        let host: String = row.get(0)?;
        let name: String = row.get(1)?;
        let rx: i64 = row.get(2)?;
        let tx: i64 = row.get(3)?;
        let total = rx + tx;
        println!("{:<20} {:<15} {:<15} {:<15} {:<15}", 
            host,
            name, 
            format_bytes(rx as u64), 
            format_bytes(tx as u64), 
            format_bytes(total as u64)
        );
    }

    Ok(())
}
