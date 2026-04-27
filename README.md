# vnstat-rs

A Rust port of [vnStat](https://github.com/vergoh/vnstat) using [libSQL](https://github.com/tursodatabase/libsql) for SQLite storage and remote synchronization.

Following the original vnStat architecture, this project provides two binaries:
- `vnstat-rs`: The CLI client for querying statistics.
- `vnstatd-rs`: The background daemon for collecting traffic data.

## Features

- **Traffic Monitoring**: Reads network traffic statistics from `/proc/net/dev`.
- **Delta Calculation**: Stores only the differences between updates, handling counter resets (e.g., after reboots).
- **libSQL Integration**: Uses libSQL for robust local storage.
- **Multi-host Support**: Identifies hosts using a unique `machine-id` (from `/etc/machine-id`) and hostname, allowing multiple instances to report to a centralized server.
- **Remote Sync**: Supports syncing local statistics with a remote libSQL/Turso database (handled by the daemon).
- **Human-readable Output**: Displays statistics in KiB, MiB, GiB, etc.

## Installation

```bash
cargo build --release
sudo cp target/release/vnstat-rs /usr/local/bin/
sudo cp target/release/vnstatd-rs /usr/local/bin/
```

## Usage

### Initialize the database
```bash
vnstat-rs --init
```

### Update statistics once (one-shot)
```bash
vnstat-rs -u
```

### Show statistics
```bash
vnstat-rs
vnstat-rs -i eth0
```

### List interfaces
```bash
vnstat-rs --iflist
```

### Run the daemon
```bash
vnstatd-rs
```

## Configuration

By default, the application looks for a configuration file at `/etc/vnstat.conf`. You can specify a different path using the `--config` flag.

Example `/etc/vnstat.conf`:

```conf
# location of the database directory
DatabaseDir "/var/lib/vnstat-rs"

# database file name
Database "vnstat.db"

# Remote libSQL/Turso configuration
LibsqlUrl "libsql://your-db-name.turso.io"
LibsqlToken "your-auth-token"

# Intervals in seconds
UpdateInterval 30
SyncInterval 300
```

## Systemd Service

A systemd service file is provided in `vnstatd-rs.service`. To install it:

1. Build the release binaries: `cargo build --release`
2. Install the binaries: `sudo cp target/release/vnstat* /usr/local/bin/`
3. Create a dedicated user: `sudo useradd -r -s /sbin/nologin vnstat`
4. Create the data directory: `sudo mkdir /var/lib/vnstat-rs && sudo chown vnstat:vnstat /var/lib/vnstat-rs`
5. Install the service file: `sudo cp vnstatd-rs.service /etc/systemd/system/`
6. Enable and start the service:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable --now vnstatd-rs
   ```

## License

This project is licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Author

Seungjin Kim
