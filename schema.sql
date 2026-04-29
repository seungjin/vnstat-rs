-- vnStat-rs Database Schema

-- Info table for metadata
CREATE TABLE IF NOT EXISTS info (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    value TEXT NOT NULL
);

-- Host table
CREATE TABLE IF NOT EXISTS host (
    id           TEXT PRIMARY KEY,
    machine_id   TEXT UNIQUE NOT NULL,
    hostname     TEXT NOT NULL
);

-- Interface table (normalized with host_id)
CREATE TABLE IF NOT EXISTS interface (
    id           TEXT PRIMARY KEY,
    host_id      TEXT NOT NULL REFERENCES host(id) ON DELETE CASCADE,
    name         TEXT NOT NULL,
    alias        TEXT,
    active       INTEGER NOT NULL DEFAULT 1,
    created      INTEGER NOT NULL,
    updated      INTEGER NOT NULL,
    rxcounter    INTEGER NOT NULL DEFAULT 0,
    txcounter    INTEGER NOT NULL DEFAULT 0,
    rxtotal      INTEGER NOT NULL DEFAULT 0,
    txtotal      INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT u_host_name UNIQUE(host_id, name)
);

-- Resolution tables
CREATE TABLE IF NOT EXISTS fiveminute (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    interface    TEXT REFERENCES interface(id) ON DELETE CASCADE,
    date         INTEGER NOT NULL,
    rx           INTEGER NOT NULL,
    tx           INTEGER NOT NULL,
    CONSTRAINT u UNIQUE (interface, date)
);

CREATE TABLE IF NOT EXISTS hour (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    interface    TEXT REFERENCES interface(id) ON DELETE CASCADE,
    date         INTEGER NOT NULL,
    rx           INTEGER NOT NULL,
    tx           INTEGER NOT NULL,
    CONSTRAINT u UNIQUE (interface, date)
);

CREATE TABLE IF NOT EXISTS day (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    interface    TEXT REFERENCES interface(id) ON DELETE CASCADE,
    date         INTEGER NOT NULL,
    rx           INTEGER NOT NULL,
    tx           INTEGER NOT NULL,
    CONSTRAINT u UNIQUE (interface, date)
);

CREATE TABLE IF NOT EXISTS month (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    interface    TEXT REFERENCES interface(id) ON DELETE CASCADE,
    date         INTEGER NOT NULL,
    rx           INTEGER NOT NULL,
    tx           INTEGER NOT NULL,
    CONSTRAINT u UNIQUE (interface, date)
);

CREATE TABLE IF NOT EXISTS year (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    interface    TEXT REFERENCES interface(id) ON DELETE CASCADE,
    date         INTEGER NOT NULL,
    rx           INTEGER NOT NULL,
    tx           INTEGER NOT NULL,
    CONSTRAINT u UNIQUE (interface, date)
);

CREATE TABLE IF NOT EXISTS top (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    interface    TEXT REFERENCES interface(id) ON DELETE CASCADE,
    date         INTEGER NOT NULL,
    rx           INTEGER NOT NULL,
    tx           INTEGER NOT NULL,
    CONSTRAINT u UNIQUE (interface, date)
);
