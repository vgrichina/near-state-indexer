use clap::Parser;

use std::env;

use borsh::BorshSerialize;
use futures::StreamExt;
use redis::aio::Connection;
use redis::AsyncCommands;

use tracing_subscriber::EnvFilter;

use crate::configs::Opts;
use near_indexer_primitives::types::AccountId;
use near_indexer_primitives::views::StateChangeValueView;
use near_indexer_primitives::CryptoHash;
use near_primitives_core::account::Account;

mod configs;

// Categories for logging
const INDEXER_FOR_EXPLORER: &str = "indexer_for_explorer";

async fn get_redis_connection() -> anyhow::Result<Connection> {
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
    let redis_client = redis::Client::open(redis_url)?;
    Ok(redis_client.get_async_connection().await?)
}

async fn handle_update(
    redis_connection: &mut redis::aio::Connection, block_hash: CryptoHash, block_height: u64, scope: &[u8],
    account_id: &AccountId, data_key: Option<&[u8]>, data_value: Option<&[u8]>,
) -> anyhow::Result<()> {
    let redis_key = if let Some(data_key) = data_key {
        [account_id.as_ref().as_bytes(), b":", data_key].concat()
    } else {
        account_id.as_ref().as_bytes().to_vec()
    };

    redis_connection
        .zadd([b"h:", scope, b":", redis_key.as_slice()].concat(), block_hash.as_ref(), block_height)
        .await?;

    if let Some(data_value) = data_value {
        redis_connection
            .set([b"d", scope, b":", redis_key.as_slice(), b":", block_hash.as_ref()].concat(), data_value)
            .await?;
    }

    if let Some(data_key) = data_key {
        // NOTE: using hset in indexer to overwrite latest data
        redis_connection
            .hset(
                [b"k:", scope, b":", account_id.as_ref().as_bytes()].concat(),
                data_key,
                block_height,
            )
            .await?;
    }

    Ok(())
}

async fn handle_message(streamer_message: near_indexer_primitives::StreamerMessage) -> anyhow::Result<()> {
    let mut redis_connection = get_redis_connection().await?;

    let block_height = streamer_message.block.header.height;
    let block_timestamp = streamer_message.block.header.timestamp;
    let block_hash = streamer_message.block.header.hash;

    for shard in streamer_message.shards {
        for state_change in shard.state_changes {
            match state_change.value {
                StateChangeValueView::DataUpdate { account_id, key, value } => {
                    handle_update(
                        &mut redis_connection,
                        block_hash,
                        block_height,
                        b"d",
                        &account_id,
                        Some(key.as_ref()),
                        Some(value.as_ref()),
                    )
                    .await?;
                    println!("DataUpdate {}", account_id);
                }
                StateChangeValueView::DataDeletion { account_id, key } => {
                    handle_update(
                        &mut redis_connection,
                        block_hash,
                        block_height,
                        b"d",
                        &account_id,
                        Some(key.as_ref()),
                        None,
                    )
                    .await?;
                    println!("DataDeletion {}", account_id);
                }
                StateChangeValueView::AccessKeyUpdate {
                    account_id,
                    public_key,
                    access_key,
                } => {
                    let data_key = public_key.try_to_vec().unwrap();
                    let value = access_key.try_to_vec().unwrap();
                    handle_update(
                        &mut redis_connection,
                        block_hash,
                        block_height,
                        b"k",
                        &account_id,
                        Some(&data_key),
                        Some(&value),
                    )
                    .await?;
                    println!("AccessKeyUpdate {}", account_id);
                }
                StateChangeValueView::AccessKeyDeletion { account_id, public_key } => {
                    let data_key = public_key.try_to_vec().unwrap();
                    handle_update(
                        &mut redis_connection,
                        block_hash,
                        block_height,
                        b"k",
                        &account_id,
                        Some(&data_key),
                        None,
                    )
                    .await?;
                    println!("AccessKeyDeletion {}", account_id);
                }
                StateChangeValueView::ContractCodeUpdate { account_id, code } => {
                    handle_update(
                        &mut redis_connection,
                        block_hash,
                        block_height,
                        b"c",
                        &account_id,
                        None,
                        Some(code.as_ref()),
                    )
                    .await?;
                    println!("ContractCodeUpdate {}", account_id);
                }
                StateChangeValueView::ContractCodeDeletion { account_id } => {
                    handle_update(&mut redis_connection, block_hash, block_height, b"c", &account_id, None, None)
                        .await?;
                    println!("ContractCodeDeletion {}", account_id);
                }
                StateChangeValueView::AccountUpdate { account_id, account } => {
                    let value = Account::from(account).try_to_vec().unwrap();
                    handle_update(
                        &mut redis_connection,
                        block_hash,
                        block_height,
                        b"a",
                        &account_id,
                        None,
                        Some(value.as_ref()),
                    )
                    .await?;
                    println!("AccountUpdate {}", account_id);
                }
                StateChangeValueView::AccountDeletion { account_id } => {
                    handle_update(&mut redis_connection, block_hash, block_height, b"a", &account_id, None, None)
                        .await?;
                    println!("AccountDeletion {}", account_id);
                    let redis_key = account_id.as_ref().as_bytes();
                    redis_connection
                        .zadd([b"h:a:", redis_key].concat(), block_hash.as_ref(), block_height)
                        .await?;
                }
            }
        }
    }

    redis_connection
        .set([b"t:", block_height.to_string().as_bytes()].concat(), block_timestamp.to_string())
        .await?;

    let disable_block_height_update = env::var("DISABLE_BLOCK_INDEX_UPDATE").unwrap_or_else(|_| "false".to_string());
    if !(disable_block_height_update == "true" || disable_block_height_update == "yes") {
        println!("latest_block_height {}", block_height);
        redis_connection.set(b"latest_block_height", block_height).await?;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // We use it to automatically search the for root certificates to perform HTTPS calls
    // (sending telemetry and downloading genesis)
    openssl_probe::init_ssl_cert_env_vars();

    let mut env_filter = EnvFilter::new(
        "tokio_reactor=info,near=info,near=error,stats=info,telemetry=info,indexer=info,indexer_for_explorer=info,aggregated=info",
    );

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

    let config: near_lake_framework::LakeConfig = opts.to_lake_config().await;
    let (sender, stream) = near_lake_framework::streamer(config);

    let mut handlers = tokio_stream::wrappers::ReceiverStream::new(stream)
        .map(handle_message)
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
