use anyhow::Result;
use cometindex::{
    async_trait, index::EventBatch, sqlx, AppView, PgTransaction,
};
use penumbra_sdk_proto::{core::component::sct::v1 as pb, event::ProtoEvent};
use sqlx::types::chrono::DateTime;

#[derive(Debug)]
pub struct BlockDetails {}

#[derive(Debug)]
pub struct Transactions {}

#[async_trait]
impl AppView for BlockDetails {
    fn name(&self) -> String {
        "explorer/block_details".to_string()
    }

    async fn init_chain(
        &self,
        dbtx: &mut PgTransaction,
        _: &serde_json::Value,
    ) -> Result<(), anyhow::Error> {
        sqlx::query(
            "
            CREATE TABLE IF NOT EXISTS explorer_block_details (
                height BIGINT PRIMARY KEY,
                root BYTEA NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                num_transactions INT NOT NULL DEFAULT 0,
                total_fees NUMERIC(39, 0) DEFAULT 0,
                validator_identity_key TEXT,
                previous_block_hash BYTEA,
                block_hash BaYTEA
            );
            "
        )
            .execute(dbtx.as_mut())
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_explorer_block_details_timestamp ON explorer_block_details(timestamp DESC);")
            .execute(dbtx.as_mut())
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_explorer_block_details_validator ON explorer_block_details(validator_identity_key);")
            .execute(dbtx.as_mut())
            .await?;

        Ok(())
    }

    async fn index_batch(
        &self,
        dbtx: &mut PgTransaction,
        batch: EventBatch,
    ) -> Result<(), anyhow::Error> {
        for block in batch.events_by_block() {
            let mut block_root = None;
            let mut timestamp = None;
            let tx_count = block.transactions().count();

            println!("Processing block height {} with {} transactions", block.height(), tx_count);

            for event in block.events() {
                if let Ok(pe) = pb::EventBlockRoot::from_event(&event.event) {
                    let timestamp_proto = pe.timestamp.unwrap_or_default();
                    timestamp = DateTime::from_timestamp(
                        timestamp_proto.seconds,
                        u32::try_from(timestamp_proto.nanos)?,
                    );
                    block_root = Some(pe.root.unwrap().inner);
                    break;
                }
            }

            if let (Some(root), Some(ts)) = (block_root, timestamp) {
                let height = block.height();
                let validator_key = None::<String>;
                let previous_hash = None::<Vec<u8>>;
                let block_hash = None::<Vec<u8>>;

                sqlx::query(
                    "
                INSERT INTO explorer_block_details
                (height, root, timestamp, num_transactions, validator_identity_key, previous_block_hash, block_hash)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (height) DO UPDATE SET
                root = EXCLUDED.root,
                timestamp = EXCLUDED.timestamp,
                num_transactions = EXCLUDED.num_transactions,
                validator_identity_key = EXCLUDED.validator_identity_key,
                previous_block_hash = EXCLUDED.previous_block_hash,
                block_hash = EXCLUDED.block_hash
                "
                )
                    .bind(i64::try_from(height)?)
                    .bind(root)
                    .bind(ts)
                    .bind(i32::try_from(tx_count)?)
                    .bind(validator_key)
                    .bind(previous_hash)
                    .bind(block_hash)
                    .execute(dbtx.as_mut())
                    .await?;

                if tx_count > 0 {
                    sqlx::query(
                        "
                    CREATE TABLE IF NOT EXISTS explorer_transactions (
                        id SERIAL PRIMARY KEY,
                        tx_hash BYTEA NOT NULL UNIQUE,
                        block_height BIGINT NOT NULL,
                        timestamp TIMESTAMPTZ NOT NULL,
                        raw_data BYTEA,
                        FOREIGN KEY (block_height) REFERENCES explorer_block_details(height)
                    );
                    "
                    )
                        .execute(dbtx.as_mut())
                        .await?;

                    for (tx_hash, tx_bytes) in block.transactions() {
                        println!("Inserting transaction with hash {:?} from block {}", tx_hash, height);

                        let insert_result = sqlx::query(
                            "
                        INSERT INTO explorer_transactions
                        (tx_hash, block_height, timestamp, raw_data)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (tx_hash) DO NOTHING
                        "
                        )
                            .bind(tx_hash.as_ref())
                            .bind(i64::try_from(height)?)
                            .bind(ts)
                            .bind(tx_bytes)
                            .execute(dbtx.as_mut())
                            .await;

                        match insert_result {
                            Ok(result) => println!("Successfully inserted transaction, rows affected: {}", result.rows_affected()),
                            Err(e) => println!("Error inserting transaction: {:?}", e)
                        }
                    }
                }
            }
        }

        Ok(())
    }
    }

#[async_trait]
impl AppView for Transactions {
    fn name(&self) -> String {
        "explorer/transactions".to_string()
    }

    async fn init_chain(
        &self,
        dbtx: &mut PgTransaction,
        _: &serde_json::Value,
    ) -> Result<(), anyhow::Error> {
        sqlx::query(
            "
            CREATE TABLE IF NOT EXISTS explorer_transactions (
                id SERIAL PRIMARY KEY,
                tx_hash BYTEA NOT NULL UNIQUE,
                block_height BIGINT NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                raw_data BYTEA,
                FOREIGN KEY (block_height) REFERENCES explorer_block_details(height)
            );
            "
        )
            .execute(dbtx.as_mut())
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_explorer_transactions_tx_hash ON explorer_transactions(tx_hash);")
            .execute(dbtx.as_mut())
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_explorer_transactions_block_height ON explorer_transactions(block_height);")
            .execute(dbtx.as_mut())
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_explorer_transactions_timestamp ON explorer_transactions(timestamp DESC);")
            .execute(dbtx.as_mut())
            .await?;

        Ok(())
    }

    async fn index_batch(
        &self,
        dbtx: &mut PgTransaction,
        batch: EventBatch,
    ) -> Result<(), anyhow::Error> {
        for block in batch.events_by_block() {
            let height = block.height();
            let tx_count = block.transactions().count();

            println!("Transactions AppView: Processing block height {} with {} transactions", height, tx_count);

            let timestamp = self.get_block_timestamp(dbtx, height).await?;

            if timestamp.is_none() {
                println!("Transactions AppView: No timestamp found for block height {}, skipping transactions", height);
                continue;
            }

            let block_time = timestamp.unwrap();

            for (tx_hash, tx_bytes) in block.transactions() {
                println!("Transactions AppView: Inserting transaction with hash {:?} from block {}", tx_hash, height);

                let result = sqlx::query(
                    "
                    INSERT INTO explorer_transactions
                    (tx_hash, block_height, timestamp, raw_data)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (tx_hash) DO NOTHING
                    "
                )
                    .bind(tx_hash.as_ref())
                    .bind(i64::try_from(height)?)
                    .bind(block_time)
                    .bind(tx_bytes)
                    .execute(dbtx.as_mut())
                    .await;

                match result {
                    Ok(_) => println!("Transactions AppView: Successfully inserted transaction"),
                    Err(e) => println!("Transactions AppView: Error inserting transaction: {:?}", e),
                }
            }
        }

        Ok(())
    }
}

impl BlockDetails {
    async fn get_block_timestamp(&self, dbtx: &mut PgTransaction<'_>, height: u64) -> Result<Option<DateTime<sqlx::types::chrono::Utc>>, anyhow::Error> {
        let timestamp: Option<DateTime<sqlx::types::chrono::Utc>> = sqlx::query_scalar(
            "SELECT timestamp FROM explorer_block_details WHERE height = $1"
        )
            .bind(i64::try_from(height)?)
            .fetch_optional(dbtx.as_mut())
            .await?;

        Ok(timestamp)
    }
}

impl Transactions {
    async fn get_block_timestamp(&self, dbtx: &mut PgTransaction<'_>, height: u64) -> Result<Option<DateTime<sqlx::types::chrono::Utc>>, anyhow::Error> {
        let timestamp: Option<DateTime<sqlx::types::chrono::Utc>> = sqlx::query_scalar(
            "SELECT timestamp FROM explorer_block_details WHERE height = $1"
        )
            .bind(i64::try_from(height)?)
            .fetch_optional(dbtx.as_mut())
            .await?;

        Ok(timestamp)
    }
}

pub fn app_views() -> Vec<Box<dyn AppView>> {
    vec![
        Box::new(BlockDetails {}),
        Box::new(Transactions {}),
    ]
}