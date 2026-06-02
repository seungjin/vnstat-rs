# vnstat-rs

A modern Rust network monitoring tool inspired by [vnStat](https://github.com/vergoh/vnstat), featuring [libsql](https://github.com/tursodatabase/libsql) for robust local storage and native remote synchronization with Turso.

Following the original vnStat architecture, this project provides two binaries:
- `vnstat-rs`: The CLI client for querying statistics.
- `vnstatd-rs`: The background daemon for collecting traffic data.

## Key Improvements over Original vnStat

- **Hybrid Persistence**: Maintains a local SQLite/libsql database for the local host's statistics while optionally aggregating data from multiple hosts via a remote Libsql/Turso server.
- **Distributed Identification**: Uses `machine-id` (from `/etc/machine-id`) and MAC addresses to uniquely identify hosts and interfaces, allowing for unified monitoring of multiple servers from a single CLI.
- **Reboot Detection**: Automatically detects system reboots by monitoring `/proc/uptime`. This prevents traffic over-counting by correctly identifying counter resets instead of treating them as rollovers.
- **Race-Free Updates**: The CLI delegates update requests (`-u`) to the running daemon via Unix Domain Sockets, preventing data corruption and double-counting that can occur if two processes access the database simultaneously.
- **Smart Filtering**: Prioritizes physical interfaces (Ethernet, WiFi, Mobile) in default summary views to provide the most relevant data while still allowing monitoring of virtual interfaces (VPNs, Bridges, Docker).
- **Modern Storage**: Built on `libsql` for better performance and native cloud synchronization capabilities.

## Features

- **Traffic Monitoring**: Reads network traffic statistics from `/proc/net/dev`.
- **Delta Calculation**: Stores only the differences between updates, handling counter resets and 32/64-bit rollovers.
- **Automated Failover**: The CLI automatically detects if `vnstatd-rs` is not running and falls back to direct database access.
- **Unique Identification**: Uses both `machine-id` and MAC addresses for robust identification in distributed environments.
- **Hardware Tracking**: Automatically discovers and stores MAC addresses for all monitored interfaces.
- **Flexible Persistence**: Automatically switches to user-local paths (`~/.config` and `~/.local`) if system paths are not accessible.
- **Multi-host Support**: Aggregate views of all reporting hosts using the `--all-hosts` flag.
- **Human-readable Output**: Displays statistics in KiB, MiB, GiB, etc., with official vnStat-compatible tabular formatting.
- **CLI Compatibility**: Command-line arguments designed to match the original `vnstat` and `vnstatd`.

## Installation

### From Source

The easiest way to install is using `cargo install`:

```bash
git clone https://github.com/seungjin/vnstat-rs
cd vnstat-rs
cargo install --path .
```

This installs `vnstat-rs` and `vnstatd-rs` to `~/.cargo/bin`.

### Manual / System-wide

```bash
cargo build --release
sudo cp target/release/vnstat-rs /usr/local/bin/
sudo cp target/release/vnstatd-rs /usr/local/bin/
```

## Usage

### vnstat-rs (Client)

```bash
# Show summary (physical interfaces of current host)
vnstat-rs

# Show help (use -? or --help)
vnstat-rs -?

# Show hourly statistics (matches original vnStat behavior)
vnstat-rs -h

# Show daily statistics
vnstat-rs -d

# Show monthly statistics
vnstat-rs -m

# Show all interfaces (including virtual/docker/lo)
vnstat-rs -a

# Show statistics for all hosts in the remote database
vnstat-rs --all-hosts

# List all known hosts and their machine IDs
vnstat-rs --host-list

# Show daemon/host information
vnstat-rs --info

# Select a specific interface
vnstat-rs -i eth0

# Update the database (delegates to daemon if running)
vnstat-rs -u
```

## Comparison with Original vnStat

While `vnstat-rs` is designed as a drop-in replacement, there are intentional differences in its architecture and behavior.

### Command-line Interface

- **The `-h` Flag**: Following the legacy of the original C version, **`-h` is used for "Hours"**, not "Help". To see the help message, use `-?` or `--help`.
- **IEC Units**: `vnstat-rs` strictly follows IEC standard units (KiB, MiB, GiB, TiB) to avoid the ambiguity of "KB" (which can be 1000 or 1024 bytes depending on the tool).
- **Timezones**: Hourly and summary outputs include timezone suffixes (e.g., `(UTC)`) to ensure clarity in distributed monitoring environments.

### Architecture

| Feature | Original vnStat (C) | vnstat-rs (Rust) |
| :--- | :--- | :--- |
| **Database** | Custom binary format | SQLite / Libsql |
| **Identification** | Interface Name | MAC Address + Machine ID |
| **Concurrency** | File locking | IPC Delegation + SQLite ACID |
| **Remote Sync** | Not native (requires script) | Native (Libsql/Turso) |
| **Virtual Ifaces** | Manual configuration | Auto-detection & Smart filtering |

### Accuracy and Performance

`vnstat-rs` utilizes the same kernel source (`/proc/net/dev`) as the original, ensuring data parity. The use of Rust and Libsql provides:
1. **Safety**: Memory safety and overflow protection for high-speed (Tbit/s) counters.
2. **Reliability**: Atomic database transactions prevent data corruption during power failures.
3. **Efficiency**: Multi-threaded daemon for simultaneous local collection and remote synchronization.

### vnstatd-rs (Daemon)

```bash
# Start the daemon in the foreground
vnstatd-rs -n

# Initialize the database and exit
vnstatd-rs --initdb

# Use a specific configuration file
vnstatd-rs -c ~/.vnstat-rs.conf
```

### User Space

If you installed via `cargo install`, you can set up a user-space daemon:

```bash
just setup-user-service
systemctl --user enable --now vnstatd-rs
```

This will run the daemon as your local user and store data in `~/.local/share/vnstat-rs/`.

## Configuration

By default, the application looks for a configuration file at:
- Root: `/etc/vnstat-rs/vnstat-rs.conf`
- User: `~/.config/vnstat-rs/vnstat-rs.conf`

### Remote Synchronization

To centralize data from multiple hosts, use the `LibsqlUrl` and `LibsqlToken` settings in your config file:

```conf
# Remote Libsql/Turso configuration
LibsqlUrl "libsql://your-db-name.turso.io"
LibsqlToken "your-auth-token"

# Intervals in seconds
UpdateInterval 30
SyncInterval 300
```

## License

This project is licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Author

Seungjin Kim
