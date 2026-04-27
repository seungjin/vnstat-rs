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
    long_about = None
)]
struct Cli {
    /// Run in foreground
    #[arg(short = 'n', long)]
    nodaemon: bool,

    /// Path to config file
    #[arg(long, value_name = "FILE", default_value = "/etc/vnstat.conf")]
    config: PathBuf,

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
    let file_config = load_config(&cli.config);
    
    let db_path = file_config.database
        .unwrap_or_else(|| PathBuf::from("vnstat.db"));
    
    let url = file_config.url;
    let token = file_config.token;

    let db = Db::open(db_path, url, token).await?;

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
