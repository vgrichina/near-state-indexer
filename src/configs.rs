pub use clap::{Parser, Subcommand};

use near_jsonrpc_client::{methods, JsonRpcClient};
use near_lake_framework::near_indexer_primitives::types::{BlockReference, Finality};

/// NEAR Indexer for Explorer
/// Watches for stream of blocks from the chain
#[derive(Parser, Debug)]
#[clap(
    version,
    author,
    about,
    setting(clap::AppSettings::DisableHelpSubcommand),
    setting(clap::AppSettings::PropagateVersion),
    setting(clap::AppSettings::NextLineHelp)
)]
pub(crate) struct Opts {
    #[clap(long, default_value = "redis://127.0.0.1", env)]
    pub redis_url: String,
    /// AWS Access Key with the rights to read from AWS S3
    #[clap(long, env)]
    pub lake_aws_access_key: String,
    #[clap(long, env)]
    /// AWS Secret Access Key with the rights to read from AWS S3
    pub lake_aws_secret_access_key: String,
    /// Chain ID: testnet or mainnet
    #[clap(subcommand)]
    pub chain_id: ChainId,
}

#[derive(Subcommand, Debug, Clone)]
pub enum ChainId {
    #[clap(subcommand)]
    Mainnet(StartOptions),
    #[clap(subcommand)]
    Testnet(StartOptions),
}

#[derive(Subcommand, Debug, Clone)]
pub enum StartOptions {
    FromBlock { height: u64 },
    FromInterruption,
    FromLatest,
}

impl Opts {
    // pub fn chain_id(&self) -> crate::types::primitives::ChainId {
    //     match self.chain_id {
    //         ChainId::Mainnet(_) => crate::types::primitives::ChainId::Mainnet,
    //         ChainId::Testnet(_) => crate::types::primitives::ChainId::Testnet,
    //     }
    // }

    /// Returns [StartOptions] for current [Opts]
    pub fn start_options(&self) -> &StartOptions {
        match &self.chain_id {
            ChainId::Mainnet(start_options) | ChainId::Testnet(start_options) => start_options,
        }
    }

    // Creates AWS Credentials for NEAR Lake
    fn lake_credentials(&self) -> aws_types::credentials::SharedCredentialsProvider {
        let provider = aws_types::Credentials::new(
            self.lake_aws_access_key.clone(),
            self.lake_aws_secret_access_key.clone(),
            None,
            None,
            "alertexer_lake",
        );
        aws_types::credentials::SharedCredentialsProvider::new(provider)
    }

    /// Creates AWS Shared Config for NEAR Lake
    pub fn lake_aws_sdk_config(&self) -> aws_types::sdk_config::SdkConfig {
        aws_types::sdk_config::SdkConfig::builder()
            .credentials_provider(self.lake_credentials())
            .region(aws_types::region::Region::new("eu-central-1"))
            .build()
    }

    pub fn rpc_url(&self) -> &str {
        match self.chain_id {
            ChainId::Mainnet(_) => "https://rpc.mainnet.near.org",
            ChainId::Testnet(_) => "https://rpc.testnet.near.org",
        }
    }

    pub async fn redis_client(&self) -> anyhow::Result<redis::aio::ConnectionManager> {
        Ok(redis::Client::open(self.redis_url.clone())?
            .get_tokio_connection_manager()
            .await?)
    }
}

impl Opts {
    pub async fn to_lake_config(&self) -> near_lake_framework::LakeConfig {
        let s3_config = aws_sdk_s3::config::Builder::from(&self.lake_aws_sdk_config()).build();

        let config_builder = near_lake_framework::LakeConfigBuilder::default().s3_config(s3_config);

        match &self.chain_id {
            ChainId::Mainnet(_) => config_builder
                .mainnet()
                .start_block_height(get_start_block_height(self).await),
            ChainId::Testnet(_) => config_builder
                .testnet()
                .start_block_height(get_start_block_height(self).await),
        }
        .build()
        .expect("Failed to build LakeConfig")
    }
}

// TODO: refactor to read from Redis once `storage` is extracted to a separate crate
async fn get_start_block_height(opts: &Opts) -> u64 {
    match opts.start_options() {
        StartOptions::FromBlock { height } => *height,
        StartOptions::FromInterruption => {
            let redis_connection_manager = match opts.redis_client().await {
                Ok(connection_manager) => connection_manager,
                Err(err) => {
                    tracing::warn!(
                        target: "alertexer",
                        "Failed to connect to Redis to get last synced block, failing to the latest...\n{:#?}",
                        err,
                    );
                    return final_block_height(opts).await;
                }
            };
            match redis::cmd("GET")
                .arg("last_indexed_block")
                .query_async(&mut redis_connection_manager.clone())
                .await
            {
                Ok(last_indexed_block) => last_indexed_block,
                Err(err) => {
                    tracing::warn!(
                        target: crate::INDEXER_FOR_EXPLORER,
                        "Failed to get last indexer block from Redis. Failing to the latest one...\n{:#?}",
                        err
                    );
                    final_block_height(opts).await
                }
            }
        }
        StartOptions::FromLatest => final_block_height(opts).await,
    }
}

async fn final_block_height(opts: &Opts) -> u64 {
    let client = JsonRpcClient::connect(opts.rpc_url());
    let request = methods::block::RpcBlockRequest {
        block_reference: BlockReference::Finality(Finality::Final),
    };

    let latest_block = client.call(request).await.unwrap();

    latest_block.header.height
}
