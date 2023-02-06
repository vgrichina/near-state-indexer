use clap::Parser;

use bigdecimal::FromPrimitive;
use borsh::BorshSerialize;
use futures::StreamExt;
use scylla::{QueryResult, Session};

use tracing_subscriber::EnvFilter;

use crate::configs::Opts;
use near_indexer_primitives::views::StateChangeValueView;
use near_indexer_primitives::CryptoHash;
use near_primitives_core::account::Account;

mod configs;

// Categories for logging
const INDEXER: &str = "state_indexer";

async fn handle_streamer_message(
    streamer_message: near_indexer_primitives::StreamerMessage,
    scylladb_session: &Session,
    indexer_id: &str,
) -> anyhow::Result<()> {
    let block_height = streamer_message.block.header.height;
    let block_hash = streamer_message.block.header.hash;
    tracing::info!(target: INDEXER, "Block height {}", block_height,);

    let _ = handle_block(&streamer_message.block, scylladb_session).await;

    let futures = streamer_message.shards.into_iter().flat_map(|shard| {
        shard.state_changes.into_iter().map(|state_change_with_cause| {
            handle_state_change(state_change_with_cause, scylladb_session, block_height, block_hash)
        })
    });

    futures::future::join_all(futures).await;

    scylladb_session
        .query(
            "INSERT INTO meta
            (indexer_id, last_processed_block_height)
            VALUES (?, ?)",
            (indexer_id, bigdecimal::BigDecimal::from_u64(block_height).unwrap()),
        )
        .await?;
    Ok(())
}

async fn handle_block(
    block: &near_indexer_primitives::views::BlockView,
    scylladb_session: &Session,
) -> anyhow::Result<QueryResult> {
    Ok(scylladb_session
        .query(
            "INSERT INTO blocks
            (block_height, block_hash, chunks)
            VALUES (?, ?, ?)",
            (
                bigdecimal::BigDecimal::from_u64(block.header.height).unwrap(),
                block.header.hash.to_string(),
                block
                    .chunks
                    .iter()
                    .map(|chunk_header_view| chunk_header_view.chunk_hash.to_string())
                    .collect::<Vec<String>>(),
            ),
        )
        .await?)
}

async fn handle_state_change(
    state_change: near_indexer_primitives::views::StateChangeWithCauseView,
    scylladb_session: &Session,
    block_height: u64,
    block_hash: CryptoHash,
) -> anyhow::Result<()> {
    match state_change.value {
        StateChangeValueView::DataUpdate { account_id, key, value } => {
            let key: &[u8] = key.as_ref();
            let value: &[u8] = value.as_ref();
            scylladb_session
                .query(
                    "INSERT INTO state_changes
                    (account_id, block_height, block_hash, change_scope, data_key, data_value)
                    VALUES(?, ?, ?, ?, ?, ?)",
                    (
                        account_id.to_string(),
                        bigdecimal::BigDecimal::from_u64(block_height).unwrap(),
                        block_hash.to_string(),
                        "Data".to_string(),
                        key.to_vec(),
                        value.to_vec(),
                    ),
                )
                .await?;
            scylladb_session
                .query(
                    "INSERT INTO account_state
                    (account_id, data_key)
                    VALUES(?, ?)",
                    (
                        account_id.to_string(),
                        key.to_vec(),
                    ),
                )
                .await?;
            tracing::debug!(target: INDEXER, "DataUpdate {}", account_id,);
        }
        StateChangeValueView::DataDeletion { account_id, key } => {
            let key: &[u8] = key.as_ref();
            scylladb_session
                .query(
                    "INSERT INTO state_changes
                    (account_id, block_height, block_hash, change_scope, data_key, data_value)
                    VALUES(?, ?, ?, ?, ?, NULL)",
                    (
                        account_id.to_string(),
                        bigdecimal::BigDecimal::from_u64(block_height).unwrap(),
                        block_hash.to_string(),
                        "Data".to_string(),
                        Some(key.to_vec()),
                    ),
                )
                .await?;
            tracing::debug!(target: INDEXER, "DataUpdate {}", account_id,);
        }
        StateChangeValueView::AccessKeyUpdate {
            account_id,
            public_key,
            access_key,
        } => {
            let data_key = public_key
                .try_to_vec()
                .expect("Failed to borsh-serialize the PublicKey");
            let data_value = access_key
                .try_to_vec()
                .expect("Failed to borsh-serialize the AccessKey");
            scylladb_session
                .query(
                    "INSERT INTO state_changes
                    (account_id, block_height, block_hash, change_scope, data_key, data_value)
                    VALUES(?, ?, ?, ?, ?, ?)",
                    (
                        account_id.to_string(),
                        bigdecimal::BigDecimal::from_u64(block_height).unwrap(),
                        block_hash.to_string(),
                        "AccessKey".to_string(),
                        Some(data_key),
                        Some(data_value),
                    ),
                )
                .await?;
            tracing::debug!(target: INDEXER, "AccessKeyUpdate {}", account_id,);
        }
        StateChangeValueView::AccessKeyDeletion { account_id, public_key } => {
            let data_key = public_key
                .try_to_vec()
                .expect("Failed to borsh-serialize the PublicKey");
            scylladb_session
                .query(
                    "INSERT INTO state_changes
                    (account_id, block_height, block_hash, change_scope, data_key, data_value)
                    VALUES(?, ?, ?, ?, ?, NULL)",
                    (
                        account_id.to_string(),
                        bigdecimal::BigDecimal::from_u64(block_height).unwrap(),
                        block_hash.to_string(),
                        "AccessKey".to_string(),
                        Some(data_key),
                    ),
                )
                .await?;
            tracing::debug!(target: INDEXER, "AccessKeyUpdate {}", account_id,);
        }
        StateChangeValueView::ContractCodeUpdate { account_id, code } => {
            let code: &[u8] = code.as_ref();
            scylladb_session
                .query(
                    "INSERT INTO state_changes
                    (account_id, block_height, block_hash, change_scope, data_key, data_value)
                    VALUES(?, ?, ?, ?, NULL, ?)",
                    (
                        account_id.to_string(),
                        bigdecimal::BigDecimal::from_u64(block_height).unwrap(),
                        block_hash.to_string(),
                        "Contract".to_string(),
                        Some(code.to_vec()),
                    ),
                )
                .await?;
            tracing::debug!(target: INDEXER, "ContractCodeUpdate {}", account_id,);
        }
        StateChangeValueView::ContractCodeDeletion { account_id } => {
            scylladb_session
                .query(
                    "INSERT INTO state_changes
                    (account_id, block_height, block_hash, change_scope, data_key, data_value)
                    VALUES(?, ?, ?, ?, NULL, NULL)",
                    (
                        account_id.to_string(),
                        bigdecimal::BigDecimal::from_u64(block_height).unwrap(),
                        block_hash.to_string(),
                        "Contract".to_string(),
                    ),
                )
                .await?;
            tracing::debug!(target: INDEXER, "ContractCodeUpdate {}", account_id,);
        }
        StateChangeValueView::AccountUpdate { account_id, account } => {
            let value = Account::from(account)
                .try_to_vec()
                .expect("Failed to borsh-serialize the Account");
            scylladb_session
                .query(
                    "INSERT INTO state_changes
                    (account_id, block_height, block_hash, change_scope, data_key, data_value)
                    VALUES(?, ?, ?, ?, NULL, ?)",
                    (
                        account_id.to_string(),
                        bigdecimal::BigDecimal::from_u64(block_height).unwrap(),
                        block_hash.to_string(),
                        "Account".to_string(),
                        Some(value),
                    ),
                )
                .await?;
            tracing::debug!(target: INDEXER, "AccountUpdate {}", account_id,);
        }
        StateChangeValueView::AccountDeletion { account_id } => {
            scylladb_session
                .query(
                    "INSERT INTO state_changes
                    (account_id, block_height, block_hash, change_scope, data_key, data_value)
                    VALUES(?, ?, ?, ?, NULL, NULL)",
                    (
                        account_id.to_string(),
                        bigdecimal::BigDecimal::from_u64(block_height).unwrap(),
                        block_hash.to_string(),
                        "Account".to_string(),
                    ),
                )
                .await?;
            tracing::debug!(target: INDEXER, "AccountUpdate {}", account_id,);
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // We use it to automatically search the for root certificates to perform HTTPS calls
    // (sending telemetry and downloading genesis)
    openssl_probe::init_ssl_cert_env_vars();

    let mut env_filter = EnvFilter::new("state_indexer=info");

    if let Ok(rust_log) = std::env::var("RUST_LOG") {
        if !rust_log.is_empty() {
            for directive in rust_log.split(',').filter_map(|s| match s.parse() {
                Ok(directive) => Some(directive),
                Err(err) => {
                    eprintln!("Ignoring directive `{}`: {}", s, err);
                    None
                }
            }) {
                env_filter = env_filter.add_directive(directive);
            }
        }
    }

    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .init();

    dotenv::dotenv().ok();

    let opts: Opts = Opts::parse();

    let config: near_lake_framework::LakeConfig = opts.to_lake_config().await?;
    let (sender, stream) = near_lake_framework::streamer(config);

    // let redis_connection = get_redis_connection().await?;
    let scylladb_session = configs::get_scylladb_session(&opts.scylla_url, &opts.scylla_keyspace).await?;

    let mut handlers = tokio_stream::wrappers::ReceiverStream::new(stream)
        .map(|streamer_message| handle_streamer_message(streamer_message, &scylladb_session, &opts.indexer_id))
        .buffer_unordered(1usize);

    while let Some(_handle_message) = handlers.next().await {}
    drop(handlers); // close the channel so the sender will stop

    // propagate errors from the sender
    match sender.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(e) => Err(anyhow::Error::from(e)), // JoinError
    }
}
