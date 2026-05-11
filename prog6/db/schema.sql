PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS accounts (
    account_id     INTEGER PRIMARY KEY,
    username       TEXT NOT NULL UNIQUE,
    balance_cents  INTEGER NOT NULL DEFAULT 0 CHECK (balance_cents >= 0),
    status         TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'frozen', 'closed'))
);

CREATE TABLE IF NOT EXISTS operations_log (
    log_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_type      TEXT NOT NULL CHECK (operation_type IN ('check_balance', 'deposit', 'withdraw', 'transfer', 'bonus', 'interest', 'open', 'close', 'freeze', 'unfreeze', 'charge_service')),
    from_account_id     INTEGER,
    to_account_id       INTEGER,
    amount_cents        INTEGER,
    percentage_bps      INTEGER, -- bps: percent stores as integer, -2.50% = -250
    result              TEXT NOT NULL DEFAULT 'pending' CHECK (result IN ('pending', 'success', 'failure')),
    FOREIGN KEY (from_account_id) REFERENCES accounts(account_id),
    FOREIGN KEY (to_account_id) REFERENCES accounts(account_id)
);
