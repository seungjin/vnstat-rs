use anyhow::{Result, Context};
use clap::{Parser};
use std::path::{PathBuf};
use std::time::SystemTime;
use tokio::time::sleep;
use std::time::Duration;
use std::sync::Arc;
use tokio::net::UnixListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use vnstat_rs::{Db, load_config, IpcRequest, IpcResponse};

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

    // Turso specific additions
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
        println!("vnStatd-rs {} by Seungjin Kim (Turso {})", env!("CARGO_PKG_VERSION"), env!("TURSO_VERSION"));
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

    let db = Arc::new(db);
    let interval = cli.interval.unwrap_or(file_config.update_interval);
    let sync_interval = cli.sync_interval.unwrap_or(file_config.sync_interval);
    let socket_path = file_config.daemon_socket.clone().unwrap_or_else(|| PathBuf::from("/var/run/vnstat-rs.sock"));
    
    println!("vnStatd-rs {} starting (update: {}s, sync: {}s)...", env!("CARGO_PKG_VERSION"), interval, sync_interval);
    
    // Start IPC server
    if socket_path.exists() {
        let _ = std::fs::remove_file(&socket_path);
    }
    
    let listener = UnixListener::bind(&socket_path).context("Failed to bind Unix socket")?;
    let db_for_ipc = Arc::clone(&db);
    
    tokio::spawn(async move {
        println!("IPC server listening on {}...", socket_path.display());
        loop {
            match listener.accept().await {
                Ok((mut socket, _)) => {
                    let db = Arc::clone(&db_for_ipc);
                    tokio::spawn(async move {
                        let mut buffer = [0u8; 4096];
                        match socket.read(&mut buffer).await {
                            Ok(n) if n > 0 => {
                                let req: Result<IpcRequest, _> = serde_json::from_slice(&buffer[..n]);
                                let resp = match req {
                                    Ok(IpcRequest::GetStats { interface }) => {
                                        match db.get_all_interface_stats(interface.as_deref()).await {
                                            Ok(stats) => IpcResponse::Stats(stats),
                                            Err(e) => IpcResponse::Error(e.to_string()),
                                        }
                                    }
                                    Ok(IpcRequest::GetHistory { table, interface, limit }) => {
                                        match db.get_history(&table, interface.as_deref(), limit).await {
                                            Ok(history) => IpcResponse::History(history),
                                            Err(e) => IpcResponse::Error(e.to_string()),
                                        }
                                    }
                                    Ok(IpcRequest::GetInfo) => {
                                        IpcResponse::Info {
                                            hostname: db.hostname.clone(),
                                            machine_id: db.machine_id.clone(),
                                        }
                                    }
                                    Err(e) => IpcResponse::Error(format!("Invalid request: {}", e)),
                                };
                                let _ = socket.write_all(&serde_json::to_vec(&resp).unwrap()).await;
                            }
                            _ => {}
                        }
                    });
                }
                Err(e) => eprintln!("IPC accept error: {}", e),
            }
        }
    });

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
