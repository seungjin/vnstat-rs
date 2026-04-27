# vnstat-rs

A Rust port of [vnStat](https://github.com/vergoh/vnstat) using [libSQL](https://github.com/tursodatabase/libsql) for SQLite storage and remote synchronization.

## Features

- **Traffic Monitoring**: Reads network traffic statistics from `/proc/net/dev`.
- **Delta Calculation**: Stores only the differences between updates, handling counter resets (e.g., after reboots).
- **libSQL Integration**: Uses libSQL for robust local storage.
- **Multi-host Support**: Identifies hosts using a unique `machine-id` (from `/etc/machine-id`) and hostname, allowing multiple instances to report to a centralized server.
- **Remote Sync**: Supports syncing local statistics with a remote libSQL/Turso database.
- **Daemon Mode**: Background process for periodic updates and synchronization.
- **Human-readable Output**: Displays statistics in KiB, MiB, GiB, etc.

## Installation

```bash
cargo build --release
```

## Usage

### Initialize the database
```bash
vnstat-rs init
```

### Update statistics once
```bash
vnstat-rs update
```

### Show statistics
```bash
vnstat-rs show
```

### Sync with remote database
```bash
vnstat-rs --url <URL> --token <TOKEN> sync
```

### Run as a daemon
```bash
vnstat-rs daemon --interval 30 --sync-interval 300
```

## Systemd Service

A systemd service file is provided in `vnstat-rs.service`. To install it:

1. Build the release binary: `cargo build --release`
2. Install the binary: `sudo cp target/release/vnstat-rs /usr/local/bin/`
3. Create a dedicated user: `sudo useradd -r -s /sbin/nologin vnstat`
4. Create the data directory: `sudo mkdir /var/lib/vnstat-rs && sudo chown vnstat:vnstat /var/lib/vnstat-rs`
5. Install the service file: `sudo cp vnstat-rs.service /etc/systemd/system/`
6. Enable and start the service:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable --now vnstat-rs
   ```

## Configuration

Environment variables can be used for remote synchronization:
- `LIBSQL_URL`: The URL of your libSQL/Turso database.
- `LIBSQL_TOKEN`: The authentication token for the database.

## License

This project is licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Author

Seungjin Kim
