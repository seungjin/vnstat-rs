# justfile for vnstat-rs

# Build the project
build:
    cargo build

# Build the project in release mode
release:
    cargo build --release

# Run the project with arguments
run *args:
    cargo run -- {{ args }}

# Initialize the database
init:
    cargo run --bin vnstat-rs -- --init

# Update statistics once
update:
    cargo run --bin vnstat-rs -- -u

# Show statistics
show:
    cargo run --bin vnstat-rs --

# Run the daemon
daemon:
    cargo run --bin vnstatd-rs

# Install the binaries to /usr/local/bin
install: release
    sudo cp target/release/vnstat-rs /usr/local/bin/
    sudo cp target/release/vnstatd-rs /usr/local/bin/

# Install the binaries to ~/.cargo/bin
install-user:
    cargo install --path .

# Setup systemd user service
setup-user-service:
    mkdir -p ~/.config/systemd/user/
    cp vnstatd-rs.user.service ~/.config/systemd/user/vnstatd-rs.service
    systemctl --user daemon-reload
    @echo "User service installed. Run 'systemctl --user enable --now vnstatd-rs' to start."

# Setup systemd service and data directory
setup-service:
    sudo useradd -r -s /sbin/nologin vnstat || true
    sudo mkdir -p /var/lib/vnstat-rs
    sudo chown vnstat:vnstat /var/lib/vnstat-rs
    sudo cp vnstat.conf-sample /etc/vnstat-rs.conf
    sudo cp vnstatd-rs.service /etc/systemd/system/vnstatd-rs.service
    sudo systemctl daemon-reload
    sudo systemctl enable vnstatd-rs
    @echo "Service and config installed. Edit /etc/vnstat-rs.conf then run 'sudo systemctl start vnstatd-rs'."

# Clean build artifacts
clean:
    cargo clean

# Run tests
test:
    cargo test
