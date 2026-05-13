PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS accounts (
    account_id     INTEGER PRIMARY KEY,
    username       TEXT NOT NULL UNIQUE,
    balance_cents  INTEGER NOT NULL DEFAULT 0 CHECK (balance_cents >= 0),
    status         TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'frozen'))
);

CREATE TABLE IF NOT EXISTS operations_log (
    log_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    operation      TEXT NOT NULL CHECK (operation IN ('check_balance', 'deposit', 'withdraw', 'transfer', 'bonus', 'interest', 'open', 'close', 'freeze', 'unfreeze', 'charge_service')),
    actor_account_id    INTEGER,
    target_account_id   INTEGER,
    amount_cents        INTEGER,
    percentage_bps      INTEGER
    -- TODO: decide if we want failures stored in the log
    -- result              TEXT NOT NULL DEFAULT 'pending' CHECK (result IN ('pending', 'success', 'failure'))
);
