# vnstat-rs Design Document

This document outlines the architectural and programming design of `vnstat-rs`.

## Architecture Overview

`vnstat-rs` follows a classic daemon-client architecture but extends it for modern, distributed environments.

### Core Components

1.  **`vnstatd-rs` (The Daemon)**:
    *   **Collection**: Periodically reads `/proc/net/dev` to gather raw interface statistics.
    *   **Processing**: Calculates deltas (incremental usage) by comparing current counters with the last known state in the database.
    *   **Persistence**: Saves processed data to a local Libsql (SQLite) database.
    *   **Synchronization**: Optionally pushes local data to a remote Turso database.
    *   **IPC Server**: Listens on a Unix Domain Socket to serve requests from the CLI client.

2.  **`vnstat-rs` (The CLI Client)**:
    *   **Querying**: Requests statistics from the daemon via IPC. It searches multiple standard locations (e.g., `/run/`, `/var/run/`, `/var/lib/vnstat-rs/`) to find the active daemon.
    *   **Failover**: If the daemon is not running, it falls back to reading the local database directly. It uses a dedicated **read-only mode** that bypasses schema initialization, allowing non-root users to query the system-wide database if they have read permissions (e.g., via the `vnstat` group).
    *   **Formatting**: Renders statistics in human-readable tables, JSON, or XML.
    *   **Compatibility**: Maintains strict command-line argument compatibility with the original vnStat (e.g., using `-h` for hourly statistics instead of help) to ensure a seamless transition for existing users and scripts.

## Data Model & Persistence

### Hybrid Storage (Libsql + Turso)
The project uses `libsql` as its primary storage engine. This provides:
*   **Local Reliability**: Full SQLite compatibility for local edge storage.
*   **Cloud Sync**: Native ability to synchronize with Turso for centralized monitoring of multiple nodes.

### Unique Identification
Unlike the original vnStat which often relies on interface names, `vnstat-rs` uses a multi-layered identification strategy to support distributed environments:
*   **Host**: Identified by the system's `machine-id` (`/etc/machine-id`). This ensures statistics follow the machine even if the hostname changes.
*   **Interface**: Identified by MAC address where possible, falling back to name.

### Database Schema
The database uses a resolution-tiered approach:
*   `fiveminute`: 5-minute raw resolution.
*   `hour`: Hourly aggregates.
*   `day`: Daily aggregates.
*   `month`: Monthly aggregates.
*   `year`: Yearly aggregates.
*   `top`: Historical high-usage days.

## Key Algorithms

### Average Rate Calculation
For historical periods (e.g., yesterday, last month), the average rate is calculated by dividing the total traffic by the full duration of the period.

For **active periods** (e.g., today, this month, current hour), `vnstat-rs` matches the behavior of the original vnStat:
*   The average rate is calculated using the **elapsed time** since the period started.
*   This provides a real-time average that reflects actual usage so far, rather than an average spread over the entire future duration of the period.

### Delta Calculation & Reboot Detection
To ensure accuracy, the daemon must distinguish between a simple counter reset (due to reboot) and a counter rollover (due to 32-bit or 64-bit limits).

1.  **Normal Flow**: `delta = current - last` (if `current >= last`).
2.  **Decrease Detection**: If `current < last`:
    *   Check system uptime via `/proc/uptime`.
    *   Calculate the actual system boot time.
    *   If the boot time has shifted significantly since the last update, the counter decrease is treated as a **reset** (`delta = current`).
    *   Otherwise, it is treated as a **rollover** (either 32-bit or 64-bit) based on whether the resulting delta exceeds the configured `MaxBandwidth`.

### Delegated Updates
To prevent race conditions where multiple processes (e.g., the daemon and a manual `vnstat-rs -u` call) attempt to write to the database simultaneously:
*   The CLI client first checks for a running daemon.
*   If found, it sends an `Update` IPC request.
*   The daemon performs the update and responds, ensuring a single owner for write operations.

## Networking & IPC

### Unix Domain Sockets
Communication between the client and daemon uses JSON-serialized `IpcRequest` and `IpcResponse` enums over a Unix Domain Socket. This provides a type-safe and performant local API.
*   **Permissions**: The daemon creates the socket with `0666` permissions, allowing non-root users to query the daemon without requiring `sudo` or being part of a specific group (though group-based access to the underlying database is still recommended for fallback scenarios).
*   **Discovery**: The CLI client performs a multi-path search for the socket, ensuring connectivity even in varied system configurations.

### Configuration Priority
The application follows a hierarchical configuration loading strategy:
1.  **Command Line**: `-c` or `--config` flag.
2.  **Local Environment**: `vnstat-rs.conf` in the current working directory.
3.  **User Override**: `~/.config/vnstat-rs/vnstat-rs.conf`.
4.  **System Default**: `/etc/vnstat-rs/vnstat-rs.conf`.
5.  **Built-in Defaults**: Hardcoded sane defaults if no config files are found.

This hierarchy allows users to override system-wide settings (like `DatabaseDir` or `DaemonSocket`) for their specific environment.

### Interface Filtering
The system categorizes interfaces into **Physical** (Ethernet, WiFi, Mobile) and **Virtual** (VPN, Bridge, Docker).
*   **Logic**: Uses `/sys/class/net/<iface>/device` presence to identify physical hardware.
*   **Defaults**: Summary views default to physical interfaces to avoid "double-counting" traffic that passes through both a physical wire and a virtual tunnel or bridge. The `-a` or `--all-interfaces` flag can be used to show all data.

## Distributed Design (Turso)
When remote synchronization is enabled:
*   Local updates are performed first.
*   The daemon periodically synchronizes local changes to the remote Turso instance.
*   The remote schema mirrors the local one but aggregates data from all reporting `machine-id`s.
*   The client can use `--all-hosts` to query the remote database for a unified view of the entire infrastructure.
