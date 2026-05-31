CREATE TABLE IF NOT EXISTS accounts (
    account_id     INTEGER PRIMARY KEY,
    username       TEXT NOT NULL UNIQUE,
    balance_cents  INTEGER NOT NULL DEFAULT 0 CHECK (balance_cents >= 0),
    status         TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'frozen'))
);

CREATE TABLE IF NOT EXISTS operations_log (
    log_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    log_index           INTEGER NOT NULL UNIQUE,
    term                INTEGER NOT NULL,
    operation           TEXT NOT NULL CHECK (operation IN ('check_balance', 'deposit', 'withdraw', 'transfer', 'bonus', 'interest', 'open', 'close', 'freeze', 'unfreeze', 'charge_service')),
    actor_account_id    INTEGER,
    actor_username      TEXT,
    target_account_id   INTEGER,
    target_username     TEXT,
    amount_cents        INTEGER,
    percentage_bps      INTEGER
);

CREATE TABLE IF NOT EXISTS raft_metadata (
    id                  INTEGER PRIMARY KEY CHECK (id = 1),
    term                INTEGER NOT NULL,
    last_committed_idx  INTEGER NOT NULL,
    last_applied_idx    INTEGER NOT NULL
);
