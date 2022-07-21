use clap::Parser;
use std::convert::TryFrom;
use std::env;

use borsh::BorshSerialize;
use futures::StreamExt;
use redis::aio::Connection;
use redis::AsyncCommands;
use tokio::sync::mpsc;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use crate::configs::{Opts, SubCommand};
use near_indexer::near_primitives;
use near_primitives::account::Account;
use near_primitives::views::StateChangeValueView;
use near_primitives::hash::CryptoHash;
use near_indexer_primitives::types::AccountId;

mod configs;
#[macro_use]
mod retriable;

// Categories for logging
const INDEXER_FOR_EXPLORER: &str = "indexer_for_explorer";

async fn get_redis_connection() -> anyhow::Result<Connection> {
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
    let redis_client = redis::Client::open(redis_url)?;
    Ok(redis_client.get_async_connection().await?)
}

async fn handle_update(redis_connection: &mut redis::aio::Connection, block_hash: CryptoHash, block_height: u64, scope: &[u8], account_id: &AccountId, data_key: Option<&[u8]>, data_value: Option<&[u8]>) -> anyhow::Result<()> {
    let redis_key = if let Some(data_key) = data_key {
        [account_id.as_ref().as_bytes(), b":", data_key.as_ref()].concat()
    } else {
        account_id.as_ref().as_bytes().to_vec()
    };

    redis_connection.zadd(
        [b"h:", scope, b":", redis_key.as_slice()].concat(),
        block_hash.as_ref(),
        block_height,
    ).await?;

    if let Some(data_value) = data_value {
        redis_connection.set(
            [b"d", scope, b":", redis_key.as_slice(), b":", block_hash.as_ref()].concat(),
            data_value,
        ).await?;
    }

    if let Some(data_key) = data_key {
        // NOTE: using hset in indexer to overwrite latest data
        redis_connection.hset(
            [b"k:", scope, b":", account_id.as_ref().as_bytes()].concat(),
            data_key.as_ref() as &[u8],
            block_height).await?;
    }

    Ok(())
}

async fn handle_message(streamer_message: near_indexer::StreamerMessage, _strict_mode: bool) -> anyhow::Result<()> {
    let mut redis_connection = get_redis_connection().await?;

    let block_height = streamer_message.block.header.height;
    let block_timestamp = streamer_message.block.header.timestamp;
    let block_hash = streamer_message.block.header.hash;

    for shard in streamer_message.shards {
        for state_change in shard.state_changes {
            match state_change.value {
                StateChangeValueView::DataUpdate { account_id, key, value } => {
                    handle_update(&mut redis_connection, block_hash, block_height, b"d", &account_id, Some(key.as_ref()), Some(value.as_ref())).await?;
                    println!("DataUpdate {}", account_id);
                }
                StateChangeValueView::DataDeletion { account_id, key } => {
                    handle_update(&mut redis_connection, block_hash, block_height, b"d", &account_id, Some(key.as_ref()), None).await?;
                    println!("DataDeletion {}", account_id);
                }
                StateChangeValueView::AccessKeyUpdate { account_id, public_key, access_key } => {
                    let data_key = public_key.try_to_vec().unwrap();
                    let value = access_key.try_to_vec().unwrap();
                    handle_update(&mut redis_connection, block_hash, block_height, b"k", &account_id, Some(&data_key), Some(&value)).await?;
                    println!("AccessKeyUpdate {}", account_id);
                }
                StateChangeValueView::AccessKeyDeletion { account_id, public_key } => {
                    let data_key = public_key.try_to_vec().unwrap();
                    handle_update(&mut redis_connection, block_hash, block_height, b"k", &account_id, Some(&data_key), None).await?;
                    println!("AccessKeyDeletion {}", account_id);
                }
                StateChangeValueView::ContractCodeUpdate { account_id, code } => {
                    handle_update(&mut redis_connection, block_hash, block_height, b"c", &account_id, None, Some(code.as_ref())).await?;
                    println!("ContractCodeUpdate {}", account_id);
                }
                StateChangeValueView::ContractCodeDeletion { account_id } => {
                    handle_update(&mut redis_connection, block_hash, block_height, b"c", &account_id, None, None).await?;
                    println!("ContractCodeDeletion {}", account_id);
                }
                StateChangeValueView::AccountUpdate { account_id, account } => {
                    let value = Account::from(account).try_to_vec().unwrap();
                    handle_update(&mut redis_connection, block_hash, block_height, b"a", &account_id, None, Some(value.as_ref())).await?;
                    println!("AccountUpdate {}", account_id);
                }
                StateChangeValueView::AccountDeletion { account_id } => {
                    handle_update(&mut redis_connection, block_hash, block_height, b"a", &account_id, None, None).await?;
                    println!("AccountDeletion {}", account_id);
                    let redis_key = account_id.as_ref().as_bytes();
                    redis_connection
                        .zadd([b"h:a:", redis_key].concat(), block_hash.as_ref(), block_height)
                        .await?;
                }
            }
        }
    }

    redis_connection.set([b"t:", block_height.to_string().as_bytes()].concat(), block_timestamp.to_string()).await?;

    let disable_block_height_update = env::var("DISABLE_BLOCK_INDEX_UPDATE").unwrap_or_else(|_| "false".to_string());
    if !(disable_block_height_update == "true" || disable_block_height_update == "yes") {
        println!("latest_block_height {}", block_height);
        redis_connection.set(b"latest_block_height", block_height).await?;
    }

    Ok(())
}

async fn listen_blocks(
    stream: mpsc::Receiver<near_indexer::StreamerMessage>, concurrency: std::num::NonZeroU16, strict_mode: bool,
    stop_after_number_of_blocks: Option<std::num::NonZeroUsize>,
) {
    if let Some(stop_after_n_blocks) = stop_after_number_of_blocks {
        warn!(target: crate::INDEXER_FOR_EXPLORER, "Indexer will stop after indexing {} blocks", stop_after_n_blocks,);
    }
    if !strict_mode {
        warn!(target: crate::INDEXER_FOR_EXPLORER, "Indexer is starting in NON-STRICT mode",);
    }
    info!(target: crate::INDEXER_FOR_EXPLORER, "Stream has started");
    let handle_messages = tokio_stream::wrappers::ReceiverStream::new(stream).map(|streamer_message| async {
        info!(target: crate::INDEXER_FOR_EXPLORER, "Block height {}", &streamer_message.block.header.height);
        handle_message(streamer_message, strict_mode)
            .await
            .map_err(|e| println!("error {}", e))
    });
    let mut handle_messages = if let Some(stop_after_n_blocks) = stop_after_number_of_blocks {
        handle_messages.take(stop_after_n_blocks.get()).boxed_local()
    } else {
        handle_messages.boxed_local()
    }
    .buffer_unordered(usize::from(concurrency.get()));

    while let Some(_handled_message) = handle_messages.next().await {}
    // Graceful shutdown
    info!(target: crate::INDEXER_FOR_EXPLORER, "Indexer will be shutdown gracefully in 7 seconds...",);
    drop(handle_messages);
    tokio::time::sleep(std::time::Duration::from_secs(7)).await;
}

/// Takes `home_dir` and `RunArgs` to build proper IndexerConfig and returns it
async fn construct_near_indexer_config(
    home_dir: std::path::PathBuf, args: configs::RunArgs,
) -> near_indexer::IndexerConfig {
    // Extract await mode to avoid duplication
    info!(target: crate::INDEXER_FOR_EXPLORER, "construct_near_indexer_config");
    let sync_mode: near_indexer::SyncModeEnum = match args.sync_mode {
        configs::SyncModeSubCommand::SyncFromInterruption(interruption_args) if interruption_args.delta == 1 => {
            info!(target: crate::INDEXER_FOR_EXPLORER, "got from interruption");
            // If delta is 0 we just return IndexerConfig with sync_mode FromInterruption
            // without any changes
            near_indexer::SyncModeEnum::FromInterruption
        }
        configs::SyncModeSubCommand::SyncFromInterruption(interruption_args) => {
            info!(target: crate::INDEXER_FOR_EXPLORER, "got from interruption");
            info!(target: crate::INDEXER_FOR_EXPLORER, "delta is non zero, calculating...");

            let mut redis_connection = get_redis_connection().await.expect("error connecting to Redis");
            redis_connection
                .get(b"latest_block_height")
                .await
                .ok()
                .map(|latest_block_height: Option<u64>| {
                    if let Some(height) = latest_block_height {
                        near_indexer::SyncModeEnum::BlockHeight(height.saturating_sub(interruption_args.delta))
                    } else {
                        near_indexer::SyncModeEnum::FromInterruption
                    }
                })
                .unwrap_or_else(|| near_indexer::SyncModeEnum::FromInterruption)
        }
        configs::SyncModeSubCommand::SyncFromBlock(block_args) => {
            near_indexer::SyncModeEnum::BlockHeight(block_args.height)
        }
        configs::SyncModeSubCommand::SyncFromLatest => near_indexer::SyncModeEnum::LatestSynced,
    };

    near_indexer::IndexerConfig {
        home_dir,
        sync_mode,
        await_for_node_synced: if args.stream_while_syncing {
            near_indexer::AwaitForNodeSyncedEnum::StreamWhileSyncing
        } else {
            near_indexer::AwaitForNodeSyncedEnum::WaitForFullSync
        },
    }
}

fn main() {
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

    let opts: Opts = Opts::parse();

    let home_dir = opts.home_dir.unwrap_or_else(near_indexer::get_default_home);

    match opts.subcmd {
        SubCommand::Run(args) => {
            tracing::info!(
                target: crate::INDEXER_FOR_EXPLORER,
                "NEAR Indexer for Explorer v{} starting...",
                env!("CARGO_PKG_VERSION")
            );

            let system = actix::System::new();
            system.block_on(async move {
                let indexer_config = construct_near_indexer_config(home_dir, args.clone()).await;
                let indexer = near_indexer::Indexer::new(indexer_config).unwrap();

                // Regular indexer process starts here
                let stream = indexer.streamer();

                listen_blocks(stream, args.concurrency, !args.non_strict_mode, args.stop_after_number_of_blocks).await;

                actix::System::current().stop();
            });
            system.run().unwrap();
        }
        SubCommand::Init(config) => near_indexer::init_configs(
            &home_dir,
            config.chain_id.as_ref().map(AsRef::as_ref),
            config.account_id.map(|account_id_string| {
                near_indexer::near_primitives::types::AccountId::try_from(account_id_string)
                    .expect("Received accound_id is not valid")
            }),
            config.test_seed.as_ref().map(AsRef::as_ref),
            config.num_shards,
            config.fast,
            config.genesis.as_ref().map(AsRef::as_ref),
            config.download_genesis,
            config.download_genesis_url.as_ref().map(AsRef::as_ref),
            config.download_config,
            config.download_config_url.as_ref().map(AsRef::as_ref),
            config.boot_nodes.as_ref().map(AsRef::as_ref),
            config.max_gas_burnt_view,
        ).expect("Failed to initiate config"),
    }
}
