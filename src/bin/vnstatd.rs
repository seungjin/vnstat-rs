use anyhow::{Result};
use clap::{Parser};
use std::path::{PathBuf};
use std::time::SystemTime;
use tokio::time::sleep;
use std::time::Duration;
use vnstat_rs::{Db, load_config};

#[derive(Parser)]
#[command(
    author, 
    about = "vnStatd-rs - The vnStat-rs daemon", 
    long_about = None,
    disable_help_flag = true,
    disable_version_flag = true
)]
struct Cli {
    /// Show help
    #[arg(short = '?', long = "help")]
    help: bool,

    /// Show version
    #[arg(short = 'v', long = "version")]
    version: bool,

    /// Fork process to background
    #[arg(short = 'd', long)]
    daemon: bool,

    /// Stay in foreground
    #[arg(short = 'n', long)]
    nodaemon: bool,

    /// Write process id to file
    #[arg(short = 'p', long, value_name = "file")]
    pidfile: Option<PathBuf>,

    /// Set daemon process user
    #[arg(short = 'u', long, value_name = "user")]
    user: Option<String>,

    /// Set daemon process group
    #[arg(short = 'g', long, value_name = "group")]
    group: Option<String>,

    /// Use specific configuration file
    #[arg(long, value_name = "file", default_value = "/etc/vnstat-rs.conf")]
    config: PathBuf,

    /// Initialize database and exit
    #[arg(long)]
    initdb: bool,

    /// Synchronize internal counters
    #[arg(long)]
    sync_counters: bool,

    // libSQL specific additions
    /// Update interval in seconds
    #[arg(long)]
    interval: Option<u64>,

    /// Sync interval in seconds
    #[arg(long)]
    sync_interval: Option<u64>,
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
        println!("vnStatd-rs {} by Seungjin Kim (libSQL 0.9.30)", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    let file_config = load_config(&cli.config);
    
    let db_path = file_config.database
        .unwrap_or_else(|| PathBuf::from("/var/lib/vnstat-rs/vnstat-rs.db"));
    
    let url = file_config.url;
    let token = file_config.token;

    let db = Db::open(db_path, url, token).await?;
    db.init_schema().await?;

    if cli.initdb {
        println!("Initializing database for host: {} ({})", db.hostname, db.machine_id);
        db.init_schema().await?;
        println!("Database initialized.");
        return Ok(());
    }

    if cli.sync_counters {
        db.sync().await?;
        return Ok(());
    }

    let interval = cli.interval.unwrap_or(file_config.update_interval);
    let sync_interval = cli.sync_interval.unwrap_or(file_config.sync_interval);
    
    println!("vnStatd-rs {} starting (update: {}s, sync: {}s)...", env!("CARGO_PKG_VERSION"), interval, sync_interval);
    
    let mut last_sync = SystemTime::now();
    loop {
        if let Err(e) = db.update_stats(None).await {
            eprintln!("Error updating stats: {}", e);
        }
        
        if last_sync.elapsed()?.as_secs() >= sync_interval {
            if let Err(e) = db.sync().await {
                eprintln!("Error syncing: {}", e);
            }
            last_sync = SystemTime::now();
        }

        sleep(Duration::from_secs(interval)).await;
    }
}
