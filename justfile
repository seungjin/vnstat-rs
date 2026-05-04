# justfile for vnstat-rs

# Increment patch version
bump:
    @perl -i -pe 's/^version = "(\d+)\.(\d+)\.(\d+)"/"version = \"$1.$2." . ($3 + 1) . "\""/e' Cargo.toml
    @echo "Bumped version to $(awk -F'\"' '/^version =/ {print $2; exit}' Cargo.toml)"

# Build the project
build:
    cargo build

# Build all release binaries
release: bump build-x86_64 build-aarch64

# Inner build recipes (don't call these directly if you want a version bump)
build-x86_64:
    cargo build --release

build-aarch64:
    cargo zigbuild --release --target aarch64-unknown-linux-gnu

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

# Install the binaries to ~/.cargo/bin
install:
    install -m 755 target/release/vnstat-rs ~/.cargo/bin/vnstat-rs
    install -m 755 target/release/vnstatd-rs ~/.cargo/bin/vnstatd-rs

# Install the binaries via cargo
install-user:
    cargo install --path .

# Setup systemd user service
setup-user-service:
    mkdir -p ~/.config/systemd/user/
    cp vnstatd-rs.user.service ~/.config/systemd/user/vnstatd-rs.service
    systemctl --user daemon-reload
    @echo "User service installed. Run 'systemctl --user enable --now vnstatd-rs' to start."

# Rebuild and restart the user service
restart: release
    systemctl --user restart vnstatd-rs
    @echo "Daemon restarted."

# Show daily stats for all hosts
all-hosts:
    cargo run --bin vnstat-rs -- -d --all-hosts

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

myservers:
    rsync -avhz target/release/{vnstatd-rs,vnstat-rs} 0.z:~/.local/bin/
    ssh 0.z "sudo mv ~/.local/bin/{vnstatd-rs,vnstat-rs} /usr/local/bin && sudo chown root:root /usr/local/bin/{vnstatd-rs,vnstat-rs}"
    ssh 0.z "sudo systemctl stop vnstatd-rs.service && sudo systemctl start vnstatd-rs.service"

    rsync -avhz target/release/{vnstatd-rs,vnstat-rs} 1.c:~/.local/bin/
    ssh 1.c "systemctl --user stop vnstatd-rs.service && systemctl --user start vnstatd-rs.service"

    rsync -avhz -e "ssh -q" target/release/{vnstatd-rs,vnstat-rs} freeshell.de:~/.local/bin/
    ssh freeshell.de "systemctl --user stop vnstatd-rs.service && systemctl --user start vnstatd-rs.service"

    rsync -avhz target/release/{vnstatd-rs,vnstat-rs} 3.o:~/.local/bin/
    ssh 3.0 "systemctl --user stop vnstatd-rs.service && systemctl --user start vnstatd-rs.service"

    rsync -avhz target/release/{vnstatd-rs,vnstat-rs} 2.o:~/.local/bin/
    ssh 2.0 "systemctl --user stop vnstatd-rs.service && systemctl --user start vnstatd-rs.service"

    rsync -avhz target/aarch64-unknown-linux-gnu/release/{vnstatd-rs,vnstat-rs} 1.o:~/.local/bin/

    rsync -avhz target/aarch64-unknown-linux-gnu/release/{vnstatd-rs,vnstat-rs} 0.o:~/.local/bin/
