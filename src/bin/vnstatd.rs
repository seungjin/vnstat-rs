use anyhow::{Result};
use clap::{Parser};
use std::path::{PathBuf};
use std::time::SystemTime;
use tokio::time::sleep;
use std::time::Duration;
use std::sync::Arc;
use tokio::net::UnixListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use vnstat_rs::{Db, IpcRequest, IpcResponse};

#[derive(Parser)]
#[command(
    author, 
    version, 
    about = "A Rust port of vnStat daemon", 
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

    /// Start as daemon
    #[arg(short, long)]
    daemon: bool,

    /// Pid file
    #[arg(short, long, value_name = "file")]
    pidfile: Option<PathBuf>,

    /// Run in foreground
    #[arg(short, long)]
    nodaemon: bool,

    /// User to run as
    #[arg(short, long, value_name = "user")]
    user: Option<String>,

    /// Group to run as
    #[arg(short, long, value_name = "group")]
    group: Option<String>,

    /// Path to config file
    #[arg(short = 'c', long, value_name = "file")]
    config: Option<PathBuf>,

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
                println!("Permission denied reading {}, trying {}...", etc_config.display(), user_config.display());
                match vnstat_rs::load_config(&user_config) {
                    Ok(c) => c,
                    Err(ue) => {
                        eprintln!("Could not read {} (permission denied or not exist) and {} ({}).", etc_config.display(), user_config.display(), ue);
                        vnstat_rs::get_default_config(is_root)
                    }
                }
            }
            Err(_) => {
                // Not found or other error, try user config silently if not root
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
    
    let db_path = file_config.database
        .clone()
        .unwrap_or_else(|| {
            if is_root {
                PathBuf::from("/var/lib/vnstat-rs/vnstat-rs.db")
            } else {
                let home = std::env::var("HOME").unwrap_or_default();
                PathBuf::from(home).join(".local/share/vnstat-rs/vnstat-rs.db")
            }
        });
    
    let url = file_config.url;
    let token = file_config.token;

    let db = Db::open(db_path, url, token).await?;

    if cli.initdb {
        println!("Database initialized for host: {} ({})", db.hostname, db.machine_id);
        return Ok(());
    }

    if cli.sync_counters {
        db.sync().await?;
        return Ok(());
    }

    if cli.daemon {
        let mut daemonize = daemonize::Daemonize::new()
            .user(cli.user.as_deref().unwrap_or("nobody"))
            .group(cli.group.as_deref().unwrap_or("nogroup"))
            .umask(0o027);

        if let Some(ref pid) = cli.pidfile {
            daemonize = daemonize.pid_file(pid);
        }

        match daemonize.start() {
            Ok(_) => println!("Success, daemonized"),
            Err(e) => {
                eprintln!("Error daemonizing: {}", e);
                std::process::exit(1);
            }
        }
    }

    let db = Arc::new(db);
    let interval = cli.interval.unwrap_or(file_config.update_interval);
    let sync_interval = cli.sync_interval.unwrap_or(file_config.sync_interval);
    let socket_path = file_config.daemon_socket.clone().unwrap_or_else(|| {
        if is_root {
            PathBuf::from("/var/run/vnstat-rs.sock")
        } else {
            let home = std::env::var("HOME").unwrap_or_default();
            PathBuf::from(home).join(".local/share/vnstat-rs/vnstat-rs.sock")
        }
    });
    
    // Start IPC server
    let db_ipc = Arc::clone(&db);
    let socket_path_ipc = socket_path.clone();
    tokio::spawn(async move {
        if socket_path_ipc.exists() {
            let _ = std::fs::remove_file(&socket_path_ipc);
        }
        
        // Ensure parent directory exists for the socket
        if let Some(parent) = socket_path_ipc.parent() {
            if !parent.exists() {
                let _ = std::fs::create_dir_all(parent);
            }
        }

        let listener = UnixListener::bind(&socket_path_ipc).expect("Failed to bind socket");
        loop {
            match listener.accept().await {
                Ok((mut stream, _)) => {
                    let db = Arc::clone(&db_ipc);
                    tokio::spawn(async move {
                        let mut buffer = [0; 1024];
                        match stream.read(&mut buffer).await {
                            Ok(n) if n > 0 => {
                                let req: Result<IpcRequest, _> = serde_json::from_slice(&buffer[..n]);
                                let resp = match req {
                                    Ok(IpcRequest::GetStats { interface }) => {
                                        match db.get_all_interface_stats(interface.as_deref()).await {
                                            Ok(stats) => IpcResponse::Stats(stats),
                                            Err(e) => IpcResponse::Error(e.to_string()),
                                        }
                                    }
                                    Ok(IpcRequest::GetHistory { table, interface, limit, begin, end }) => {
                                        match db.get_history(&table, interface.as_deref(), limit, begin, end).await {
                                            Ok(history) => IpcResponse::History(history),
                                            Err(e) => IpcResponse::Error(e.to_string()),
                                        }
                                    }
                                    Ok(IpcRequest::GetSummary { interface }) => {
                                        match db.get_summary(interface.as_deref()).await {
                                            Ok(summary) => IpcResponse::Summary(summary),
                                            Err(e) => IpcResponse::Error(e.to_string()),
                                        }
                                    }
                                    Ok(IpcRequest::GetInfo) => {
                                        IpcResponse::Info {
                                            hostname: db.hostname.clone(),
                                            machine_id: db.machine_id.clone(),
                                        }
                                    }
                                    Err(e) => IpcResponse::Error(e.to_string()),
                                };
                                let resp_json = serde_json::to_vec(&resp).unwrap();
                                let _ = stream.write_all(&resp_json).await;
                            }
                            _ => {}
                        }
                    });
                }
                Err(e) => eprintln!("Error accepting connection: {}", e),
            }
        }
    });

    println!("vnStatd-rs started. Update interval: {}s, Sync interval: {}s", interval, sync_interval);
    println!("Socket path: {}", socket_path.display());

    let mut last_sync = SystemTime::now();

    loop {
        if let Err(e) = db.update_stats(None).await {
            eprintln!("Error updating stats: {}", e);
        }

        if last_sync.elapsed().unwrap().as_secs() >= sync_interval {
            if let Err(e) = db.sync().await {
                eprintln!("Error syncing: {}", e);
            }
            last_sync = SystemTime::now();
        }

        sleep(Duration::from_secs(interval)).await;
    }
}
