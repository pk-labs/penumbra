use anyhow::Result;
use cometindex::{
    async_trait, index::EventBatch, sqlx, AppView, ContextualizedEvent, PgTransaction,
};
use penumbra_sdk_proto::{
    core::{
        component::sct::v1 as pb,
        transaction::v1::Transaction,
    },
    event::ProtoEvent,
};
use prost::Message;
use sqlx::types::chrono::DateTime;
use serde_json::{json, Value};
use std::fmt::Write;

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
                block_hash BYTEA,
                raw_json JSONB
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
            let height = block.height();

            println!("Processing block height {} with {} transactions", height, tx_count);

            let mut block_events = Vec::new();
            let mut tx_events = Vec::new();

            for event in block.events() {
                if let Ok(pe) = pb::EventBlockRoot::from_event(&event.event) {
                    let timestamp_proto = pe.timestamp.unwrap_or_default();
                    timestamp = DateTime::from_timestamp(
                        timestamp_proto.seconds,
                        u32::try_from(timestamp_proto.nanos)?,
                    );
                    block_root = Some(pe.root.unwrap().inner);
                }

                if let Some(tx_hash) = event.tx_hash() {
                    tx_events.push(event_to_json(event, Some(tx_hash))?);
                } else {
                    block_events.push(event_to_json(event, None)?);
                }
            }

            let transactions: Vec<Value> = block.transactions()
                .enumerate()
                .map(|(index, (tx_hash, _))| {
                    json!({
                        "block_id": height,
                        "index": index,
                        "created_at": timestamp,
                        "tx_hash": encode_to_hex(tx_hash)
                    })
                })
                .collect();

            let mut all_events = Vec::new();
            all_events.extend(block_events);
            all_events.extend(tx_events);

            let raw_json = json!({
                "block": {
                    "height": height,
                    "chain_id": "penumbra-1",
                    "created_at": timestamp,
                    "transactions": transactions,
                    "events": all_events
                }
            });

            if let (Some(root), Some(ts)) = (block_root, timestamp) {
                let validator_key = None::<String>;
                let previous_hash = None::<Vec<u8>>;
                let block_hash = None::<Vec<u8>>;

                sqlx::query(
                    "
                INSERT INTO explorer_block_details
                (height, root, timestamp, num_transactions, validator_identity_key, previous_block_hash, block_hash, raw_json)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (height) DO UPDATE SET
                root = EXCLUDED.root,
                timestamp = EXCLUDED.timestamp,
                num_transactions = EXCLUDED.num_transactions,
                validator_identity_key = EXCLUDED.validator_identity_key,
                previous_block_hash = EXCLUDED.previous_block_hash,
                block_hash = EXCLUDED.block_hash,
                raw_json = EXCLUDED.raw_json
                "
                )
                    .bind(i64::try_from(height)?)
                    .bind(root)
                    .bind(ts)
                    .bind(i32::try_from(tx_count)?)
                    .bind(validator_key)
                    .bind(previous_hash)
                    .bind(block_hash)
                    .bind(raw_json)
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
                        raw_json JSONB,
                        FOREIGN KEY (block_height) REFERENCES explorer_block_details(height)
                    );
                    "
                    )
                        .execute(dbtx.as_mut())
                        .await?;

                    for (tx_hash, tx_bytes) in block.transactions() {
                        println!("Inserting transaction with hash {:?} from block {}", tx_hash, height);

                        let decoded_tx_json = create_transaction_json(tx_hash, tx_bytes, height, ts);

                        let insert_result = sqlx::query(
                            "
                        INSERT INTO explorer_transactions
                        (tx_hash, block_height, timestamp, raw_data, raw_json)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (tx_hash) DO NOTHING
                        "
                        )
                            .bind(tx_hash.as_ref())
                            .bind(i64::try_from(height)?)
                            .bind(ts)
                            .bind(tx_bytes)
                            .bind(decoded_tx_json)
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
        _dbtx: &mut PgTransaction,
        _: &serde_json::Value,
    ) -> Result<(), anyhow::Error> {
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

                let decoded_tx_json = create_transaction_json(tx_hash, tx_bytes, height, block_time);

                let result = sqlx::query(
                    "
                    INSERT INTO explorer_transactions
                    (tx_hash, block_height, timestamp, raw_data, raw_json)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (tx_hash) DO UPDATE SET
                    block_height = EXCLUDED.block_height,
                    timestamp = EXCLUDED.timestamp,
                    raw_data = EXCLUDED.raw_data,
                    raw_json = EXCLUDED.raw_json
                    "
                )
                    .bind(tx_hash.as_ref())
                    .bind(i64::try_from(height)?)
                    .bind(block_time)
                    .bind(tx_bytes)
                    .bind(decoded_tx_json)
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

fn create_transaction_json(
    tx_hash: [u8; 32],
    tx_bytes: &[u8],
    height: u64,
    timestamp: DateTime<sqlx::types::chrono::Utc>
) -> Value {
    let tx_raw_hex = encode_to_hex(tx_bytes);

    let decoded_transaction = match Transaction::decode(tx_bytes) {
        Ok(tx) => {
            let tx_json = serde_json::to_value(&tx)
                .unwrap_or_else(|_| json!({"decode_error": "Failed to convert transaction to JSON"}));

            json!({
                "transaction": {
                    "hash": encode_to_hex(tx_hash),
                    "block_height": height,
                    "timestamp": timestamp,
                    "raw_data_hex": tx_raw_hex,
                    "decoded_tx": tx_json
                }
            })
        },
        Err(_) => {
            json!({
                "transaction": {
                    "hash": encode_to_hex(tx_hash),
                    "block_height": height,
                    "timestamp": timestamp,
                    "raw_data_hex": tx_raw_hex
                }
            })
        }
    };

    decoded_transaction
}

fn event_to_json(event: ContextualizedEvent<'_>, tx_hash: Option<[u8; 32]>) -> Result<Value, anyhow::Error> {
    let mut attributes = Vec::new();

    for attr in &event.event.attributes {
        let attr_str = format!("{:?}", attr);

        attributes.push(json!({
            "key": attr_str.clone(),
            "composite_key": format!("{}.{}", event.event.kind, attr_str),
            "value": "Unknown"
        }));
    }

    let json_event = json!({
        "block_id": event.block_height,
        "tx_id": tx_hash.map(encode_to_hex),
        "type": event.event.kind,
        "attributes": attributes
    });

    Ok(json_event)
}

fn encode_to_hex<T: AsRef<[u8]>>(data: T) -> String {
    let bytes = data.as_ref();
    let mut hex_string = String::with_capacity(bytes.len() * 2);

    for &byte in bytes {
        let _ = write!(&mut hex_string, "{:02X}", byte);
    }

    hex_string
}

pub fn app_views() -> Vec<Box<dyn AppView>> {
    vec![
        Box::new(BlockDetails {}),
        Box::new(Transactions {}),
    ]
}
