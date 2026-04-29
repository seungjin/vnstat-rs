# vnstat-rs

A Rust port of [vnStat](https://github.com/vergoh/vnstat) using [Turso](https://github.com/tursodatabase/turso) for SQLite storage and remote synchronization.

Following the original vnStat architecture, this project provides two binaries:
- `vnstat-rs`: The CLI client for querying statistics.
- `vnstatd-rs`: The background daemon for collecting traffic data.

## Features

- **Traffic Monitoring**: Reads network traffic statistics from `/proc/net/dev`.
- **Delta Calculation**: Stores only the differences between updates, handling counter resets (e.g., after reboots).
- **Turso Integration**: Uses Turso for robust local storage.
- **Root/sudo is not necessary**: Automatically switches to user-local paths (`~/.config` and `~/.local`) if system paths are not accessible.
- **Multi-host Support**: Identifies hosts using a unique `machine-id` (from `/etc/machine-id`) and hostname, allowing multiple instances to report to a centralized server.
- **Remote Sync**: Supports syncing local statistics with a remote Turso database (handled by the daemon).
- **Human-readable Output**: Displays statistics in KiB, MiB, GiB, etc.
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
# Show daily statistics
vnstat-rs -d

# Show hourly statistics
vnstat-rs -h

# Select a specific interface
vnstat-rs -i eth0

# Use a specific configuration file
vnstat-rs -c ~/.vnstat-rs.conf

# Update the database (one-shot update)
vnstat-rs -u

# Initialize the database
vnstat-rs --init

# List available interfaces
vnstat-rs --iflist

# Show help
vnstat-rs -?
```

### vnstatd-rs (Daemon)

```bash
# Start the daemon in the foreground
vnstatd-rs -n

# Initialize the database and exit
vnstatd-rs --initdb

# Use a specific configuration file
vnstatd-rs -c ~/.vnstat-rs.conf

# Synchronize internal counters (useful after reboot)
vnstatd-rs --sync-counters
```

## Configuration

By default, the application looks for a configuration file at:
- Root: `/etc/vnstat-rs.conf`
- User: `~/.config/vnstat-rs/vnstat-rs.conf`

And a database at:
- Root: `/var/lib/vnstat-rs/vnstat-rs.db`
- User: `~/.local/share/vnstat-rs/vnstat-rs.db`

The daemon socket is located at:
- Root: `/var/run/vnstat-rs.sock`
- User: `~/.local/share/vnstat-rs/vnstat-rs.sock`

Example config content:

```conf
# location of the database directory
# DatabaseDir "/var/lib/vnstat-rs"

# database file name
# Database "vnstat-rs.db"

# Remote Turso configuration (vnstat-rs specific)
TursoUrl "libsql://your-db-name.turso.io"
TursoToken "your-auth-token"

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

## Running without sudo

By default, the application now automatically switches to user-local paths if not run as root or if it lacks permission to read `/etc/vnstat-rs.conf`:
- Config: `~/.config/vnstat-rs/vnstat-rs.conf`
- Data/Socket: `~/.local/share/vnstat-rs/`

This allows a normal user to run the daemon and client without any special permissions or `sudo`. You can still override these using the `-c` (config) or `-D` (database) flags.

## License

This project is licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Author

Seungjin Kim
