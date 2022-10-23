-- name: create_tables#
CREATE TABLE IF NOT EXISTS transactions (
    id TEXT PRIMARY KEY,
    Date DATE NOT NULL,
    Type TEXT NOT NULL,
    Item TEXT NOT NULL,
    Currency TEXT NOT NULL,
    Units DOUBLE NOT NULL,
    PPU DOUBLE NOT NULL,
    Fees DOUBLE NOT NULL,
    Taxes DOUBLE NOT NULL,
    StockSplitRatio DOUBLE NOT NULL,
    Remarks TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS deposits_and_withdrawals (
    id TEXT PRIMARY KEY,
    Date DATE NOT NULL,
    Type TEXT NOT NULL,
    Currency TEXT NOT NULL,
    Amount DOUBLE NOT NULL,
    Remarks TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS forex (
    id TEXT PRIMARY KEY,
    Date DATE NOT NULL,
    CurrencySold TEXT NOT NULL,
    CurrencyBought TEXT NOT NULL,
    CurrencyPairCode TEXT NOT NULL,
    CurrencySoldUnits DOUBLE NOT NULL,
    PPU DOUBLE NOT NULL,
    Fees DOUBLE NOT NULL
);
