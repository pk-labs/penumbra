-- Block details table to store comprehensive information about each block
CREATE TABLE IF NOT EXISTS explorer_block_details (
                                                      height BIGINT PRIMARY KEY,
                                                      root BYTEA NOT NULL,
                                                      timestamp TIMESTAMPTZ NOT NULL,
                                                      num_transactions INT NOT NULL DEFAULT 0,
                                                      total_fees NUMERIC(39, 0) DEFAULT 0,
    validator_identity_key TEXT,
    previous_block_hash BYTEA,
    block_hash BYTEA
    );

-- Indices for efficient querying of the blocks table
CREATE INDEX IF NOT EXISTS idx_explorer_block_details_timestamp ON explorer_block_details(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_explorer_block_details_validator ON explorer_block_details(validator_identity_key);

-- Simplified transactions table - focusing on just the essential data
CREATE TABLE IF NOT EXISTS explorer_transactions (
                                                     id SERIAL PRIMARY KEY,
                                                     tx_hash BYTEA NOT NULL UNIQUE,
                                                     block_height BIGINT NOT NULL,
                                                     timestamp TIMESTAMPTZ NOT NULL,
                                                     raw_data BYTEA,
                                                     FOREIGN KEY (block_height) REFERENCES explorer_block_details(height)
    );

-- Indices for efficient querying of the transactions table
CREATE INDEX IF NOT EXISTS idx_explorer_transactions_tx_hash ON explorer_transactions(tx_hash);
CREATE INDEX IF NOT EXISTS idx_explorer_transactions_block_height ON explorer_transactions(block_height);
CREATE INDEX IF NOT EXISTS idx_explorer_transactions_timestamp ON explorer_transactions(timestamp DESC);

-- Views for common queries

-- Recent blocks with transaction counts
CREATE OR REPLACE VIEW explorer_recent_blocks AS
SELECT
    height,
    timestamp,
    num_transactions,
    total_fees,
    validator_identity_key
FROM
    explorer_block_details
ORDER BY
    height DESC;

-- Transaction summary view
CREATE OR REPLACE VIEW explorer_transaction_summary AS
SELECT
    t.tx_hash,
    t.block_height,
    t.timestamp,
    t.fee,
    t.transaction_type,
    t.status,
    COUNT(a.id) AS action_count
FROM
    explorer_transactions t
        LEFT JOIN
    explorer_transaction_actions a ON t.tx_hash = a.tx_hash
GROUP BY
    t.tx_hash, t.block_height, t.timestamp, t.fee, t.transaction_type, t.status;