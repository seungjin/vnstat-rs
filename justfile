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

# Clean build artifacts
clean:
    cargo clean

# Run tests
test:
    cargo test
