-- vnStat-rs Database Schema

-- Info table for metadata
CREATE TABLE IF NOT EXISTS info (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    value TEXT NOT NULL
);

-- Initial version
INSERT OR IGNORE INTO info (name, value) VALUES ('version', '3');

-- Host table
CREATE TABLE IF NOT EXISTS host (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    machine_id   TEXT UNIQUE NOT NULL,
    hostname     TEXT NOT NULL
);

-- Interface table (normalized with host_id)
CREATE TABLE IF NOT EXISTS interface (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id      INTEGER NOT NULL REFERENCES host(id) ON DELETE CASCADE,
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
    interface    INTEGER REFERENCES interface(id) ON DELETE CASCADE,
    date         INTEGER NOT NULL,
    rx           INTEGER NOT NULL,
    tx           INTEGER NOT NULL,
    CONSTRAINT u UNIQUE (interface, date)
);

CREATE TABLE IF NOT EXISTS hour (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    interface    INTEGER REFERENCES interface(id) ON DELETE CASCADE,
    date         INTEGER NOT NULL,
    rx           INTEGER NOT NULL,
    tx           INTEGER NOT NULL,
    CONSTRAINT u UNIQUE (interface, date)
);

CREATE TABLE IF NOT EXISTS day (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    interface    INTEGER REFERENCES interface(id) ON DELETE CASCADE,
    date         INTEGER NOT NULL,
    rx           INTEGER NOT NULL,
    tx           INTEGER NOT NULL,
    CONSTRAINT u UNIQUE (interface, date)
);

CREATE TABLE IF NOT EXISTS month (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    interface    INTEGER REFERENCES interface(id) ON DELETE CASCADE,
    date         INTEGER NOT NULL,
    rx           INTEGER NOT NULL,
    tx           INTEGER NOT NULL,
    CONSTRAINT u UNIQUE (interface, date)
);

CREATE TABLE IF NOT EXISTS year (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    interface    INTEGER REFERENCES interface(id) ON DELETE CASCADE,
    date         INTEGER NOT NULL,
    rx           INTEGER NOT NULL,
    tx           INTEGER NOT NULL,
    CONSTRAINT u UNIQUE (interface, date)
);

CREATE TABLE IF NOT EXISTS top (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    interface    INTEGER REFERENCES interface(id) ON DELETE CASCADE,
    date         INTEGER NOT NULL,
    rx           INTEGER NOT NULL,
    tx           INTEGER NOT NULL,
    CONSTRAINT u UNIQUE (interface, date)
);

-- Migration SQL example (from v2 to v3)
-- ALTER TABLE interface RENAME TO interface_old;
-- CREATE TABLE host (id INTEGER PRIMARY KEY AUTOINCREMENT, machine_id TEXT UNIQUE NOT NULL, hostname TEXT NOT NULL);
-- CREATE TABLE interface (
--     id INTEGER PRIMARY KEY AUTOINCREMENT,
--     host_id INTEGER NOT NULL REFERENCES host(id) ON DELETE CASCADE,
--     name TEXT NOT NULL,
--     alias TEXT,
--     active INTEGER NOT NULL DEFAULT 1,
--     created INTEGER NOT NULL,
--     updated INTEGER NOT NULL,
--     rxcounter INTEGER NOT NULL DEFAULT 0,
--     txcounter INTEGER NOT NULL DEFAULT 0,
--     rxtotal INTEGER NOT NULL DEFAULT 0,
--     txtotal INTEGER NOT NULL DEFAULT 0,
--     UNIQUE(host_id, name)
-- );
-- INSERT INTO host (machine_id, hostname) VALUES ('your-machine-id', 'your-hostname');
-- INSERT INTO interface (id, host_id, name, alias, active, created, updated, rxcounter, txcounter, rxtotal, txtotal)
-- SELECT id, 1, name, alias, active, created, updated, rxcounter, txcounter, rxtotal, txtotal FROM interface_old;
-- DROP TABLE interface_old;
-- UPDATE info SET value = '3' WHERE name = 'version';
