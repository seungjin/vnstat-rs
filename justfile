# justfile for vnstat-rs

# Build the project
build:
    cargo build

# Build the project in release mode
release:
    cargo build --release

# Run the project with arguments
run *args:
    cargo run -- {{args}}

# Initialize the database
init:
    cargo run -- init

# Update statistics once
update:
    cargo run -- update

# Show statistics
show:
    cargo run -- show

# Run as a daemon
daemon:
    cargo run -- daemon

# Install the binary to /usr/local/bin
install: release
    sudo cp target/release/vnstat-rs /usr/local/bin/

# Setup systemd service and data directory
setup-service:
    sudo useradd -r -s /sbin/nologin vnstat || true
    sudo mkdir -p /var/lib/vnstat-rs
    sudo chown vnstat:vnstat /var/lib/vnstat-rs
    sudo cp vnstat-rs.service /etc/systemd/system/
    sudo systemctl daemon-reload
    sudo systemctl enable vnstat-rs
    @echo "Service installed. Use 'sudo systemctl start vnstat-rs' to start."

# Clean build artifacts
clean:
    cargo clean

# Run tests
test:
    cargo test
