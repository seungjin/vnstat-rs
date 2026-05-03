use anyhow::{Result};
use clap::{Parser};
use std::path::{PathBuf};
use std::time::{SystemTime, Duration};
use tokio::time::sleep;
use std::sync::Arc;
use tokio::net::UnixListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::fs;
use vnstat_rs::{Db, IpcRequest, IpcResponse};

#[derive(Parser)]
#[command(author, version = concat!(env!("CARGO_PKG_VERSION"), " (", env!("GIT_HASH"), ")"), about = "A Rust port of vnStat daemon", long_about = None)]
struct Cli {
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
    #[arg(short, long, value_name = "file")]
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

    let file_config = if let Some(ref path) = cli.config {
        match vnstat_rs::load_config(path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Error loading config {}: {}", path.display(), e);
                std::process::exit(1);
            }
        }
    } else {
        vnstat_rs::load_best_config()
    };

    let is_root = unsafe { libc::getuid() == 0 };

    let db_path = file_config.database.clone().unwrap_or_else(|| PathBuf::from("/var/lib/vnstat-rs/vnstat-rs.db"));
    let url = file_config.url.clone();
    let token = file_config.token.clone();

    let db = Db::open(db_path, url, token, file_config.hostname_override.clone()).await? ;
    println!("Reporting version {} to database...", concat!(env!("CARGO_PKG_VERSION"), " (", env!("GIT_HASH"), ")"));

    if cli.initdb {
        println!("Database initialized for host: {} ({})", db.hostname, db.machine_id);
        return Ok(());
    }

    if cli.sync_counters {
        db.sync().await?;
        return Ok(());
    }

    if cli.daemon && !cli.nodaemon {
        let daemonize = daemonize::Daemonize::new()
            .pid_file(cli.pidfile.clone().unwrap_or_else(|| PathBuf::from("/var/run/vnstatd-rs.pid")))
            .working_directory("/tmp");

        match daemonize.start() {
            Ok(_) => println!("Daemonized successfully"),
            Err(e) => eprintln!("Error daemonizing: {}", e),
        }
    }

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

    // Ensure socket directory exists
    if let Some(parent) = socket_path.parent() {
        if !parent.exists() {
            fs::create_dir_all(parent)?;
        }
    }

    if socket_path.exists() {
        fs::remove_file(&socket_path)?;
    }

    let listener = UnixListener::bind(&socket_path)?;
    let db_ipc = Arc::new(db);
    let db_loop = Arc::clone(&db_ipc);

    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((mut stream, _)) => {
                    let db = Arc::clone(&db_ipc);
                    tokio::spawn(async move {
                        let mut buffer = [0; 65536];
                        match stream.read(&mut buffer).await {
                            Ok(n) if n > 0 => {
                                let req: Result<IpcRequest, _> = serde_json::from_slice(&buffer[..n]);
                                let resp = match req {
                                    Ok(IpcRequest::GetStats { interface, host }) => {
                                        match db.get_all_interface_stats(interface.as_deref(), host.as_deref()).await {
                                            Ok(stats) => IpcResponse::Stats(stats),
                                            Err(e) => IpcResponse::Error(e.to_string()),
                                        }
                                    }
                                    Ok(IpcRequest::GetHistory { table, interface, host, limit, begin, end }) => {
                                        match db.get_history(&table, interface.as_deref(), host.as_deref(), limit, begin, end).await {
                                            Ok(history) => IpcResponse::History(history),
                                            Err(e) => IpcResponse::Error(e.to_string()),
                                        }
                                    }
                                    Ok(IpcRequest::GetSummary { interface, host }) => {
                                        match db.get_summary(interface.as_deref(), host.as_deref()).await {
                                            Ok(summary) => IpcResponse::Summary(summary),
                                            Err(e) => IpcResponse::Error(e.to_string()),
                                        }
                                    }
                                    Ok(IpcRequest::GetInfo) => {
                                        let mac = db.get_info("mac_address").await.unwrap_or(None);
                                        let local_schema = db.get_schema_version_from(&db.local_conn).await.unwrap_or(0);
                                        let mut remote_schema = None;
                                        if let Some(ref remote) = db.remote_conn {
                                            remote_schema = Some(db.get_schema_version_from(remote).await.unwrap_or(0));
                                        }

                                        IpcResponse::Info {
                                            hostname: db.hostname.clone(),
                                            machine_id: db.machine_id.clone(),
                                            mac_address: mac,
                                            version: format!("{} ({})", env!("CARGO_PKG_VERSION"), env!("GIT_HASH")),
                                            local_schema,
                                            remote_schema,
                                        }
                                    }
                                    Ok(IpcRequest::GetConfig { name }) => {
                                        match db.get_info(&name).await {
                                            Ok(val) => IpcResponse::Config(val),
                                            Err(e) => IpcResponse::Error(e.to_string()),
                                        }
                                    }
                                    Ok(IpcRequest::SetConfig { name, value }) => {
                                        match db.set_info(&name, &value).await {
                                            Ok(_) => IpcResponse::Ok,
                                            Err(e) => IpcResponse::Error(e.to_string()),
                                        }
                                    }
                                    Ok(IpcRequest::ListHosts) => {
                                        match db.get_all_hosts().await {
                                            Ok(hosts) => IpcResponse::Hosts(hosts),
                                            Err(e) => IpcResponse::Error(e.to_string()),
                                        }
                                    }
                                    Ok(IpcRequest::Get95th { interface, host }) => {
                                        match db.get_95th_data(interface.as_deref(), host.as_deref()).await {
                                            Ok(data) => IpcResponse::NintyFifth(data),
                                            Err(e) => IpcResponse::Error(e.to_string()),
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
        if let Err(e) = db_loop.update_stats(None, &file_config).await {
            eprintln!("Error updating stats: {}", e);
        }

        // Apply data retention pruning
        if let Err(e) = db_loop.prune_stats(&file_config).await {
            eprintln!("Error pruning stats: {}", e);
        }

        if sync_interval > 0 && SystemTime::now().duration_since(last_sync)?.as_secs() >= sync_interval {
            if let Err(e) = db_loop.sync().await {
                eprintln!("Error syncing: {}", e);
            }
            last_sync = SystemTime::now();
        }

        sleep(Duration::from_secs(interval)).await;
    }
}
