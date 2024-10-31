use alloy_primitives::{Address, U256};
use alloy_provider::{
    network::{BlockResponse, HeaderResponse},
    Network, Provider,
};
use alloy_rpc_types::BlockNumberOrTag;
use alloy_transport::Transport;
use eyre::WrapErr;
use foundry_common::NON_ARCHIVE_NODE_WARNING;

use foundry_config::NamedChain;
use revm::primitives::{BlockEnv, CfgEnv, Env, TxEnv};

/// Initializes a REVM block environment based on a forked
/// ethereum provider.
pub async fn bitfinity_environment<N: Network, T: Transport + Clone, P: Provider<T, N>>(
    provider: &P,
    memory_limit: u64,
    gas_price: Option<u128>,
    override_chain_id: Option<u64>,
    pin_block: Option<u64>,
    origin: Address,
    disable_block_gas_limit: bool,
) -> eyre::Result<(Env, N::BlockResponse)> {
    let block_number = if let Some(pin_block) = pin_block {
        pin_block
    } else {
        provider.get_block_number().await.wrap_err("Failed to get latest block number")?
    };
    let (fork_gas_price, rpc_chain_id) =
        tokio::try_join!(provider.get_gas_price(), provider.get_chain_id())?;

    // Let us try 3 trials with a backoff of 1s.
    const MAX_TRIALS: u32 = 3;
    const BACKOFF_DURATION: std::time::Duration = std::time::Duration::from_secs(1);

    let mut trials = 0;
    let mut block = None;

    while block.is_none() && trials < MAX_TRIALS {
        warn!("Fetching block: {}", block_number);
        tokio::time::sleep(BACKOFF_DURATION).await;
        match provider.get_block_by_number(BlockNumberOrTag::Number(block_number), false).await {
            Ok(b) => block = b,
            Err(e) => {
                eprintln!("Error fetching block: {e:?}");
            }
        }
        trials += 1;
    }

    let block = if let Some(block) = block {
        block
    } else {
        if let Ok(latest_block) = provider.get_block_number().await {
            // If the `eth_getBlockByNumber` call succeeds, but returns null instead of
            // the block, and the block number is less than equal the latest block, then
            // the user is forking from a non-archive node with an older block number.
            if block_number <= latest_block {
                error!("{NON_ARCHIVE_NODE_WARNING}");
            }
            eyre::bail!(
                "Failed to get block for block number: {}\nlatest block number: {}",
                block_number,
                latest_block
            );
        }
        eyre::bail!("Failed to get block for block number: {}", block_number)
    };

    let mut cfg = CfgEnv::default();
    cfg.chain_id = override_chain_id.unwrap_or(rpc_chain_id);
    cfg.memory_limit = memory_limit;
    cfg.limit_contract_code_size = Some(usize::MAX);
    // EIP-3607 rejects transactions from senders with deployed code.
    // If EIP-3607 is enabled it can cause issues during fuzz/invariant tests if the caller
    // is a contract. So we disable the check by default.
    cfg.disable_eip3607 = true;
    cfg.disable_block_gas_limit = disable_block_gas_limit;

    let mut env = Env {
        cfg,
        block: BlockEnv {
            number: U256::from(block.header().number()),
            timestamp: U256::from(block.header().timestamp()),
            coinbase: block.header().coinbase(),
            difficulty: block.header().difficulty(),
            prevrandao: block.header().mix_hash(),
            basefee: U256::from(block.header().base_fee_per_gas().unwrap_or_default()),
            gas_limit: U256::from(block.header().gas_limit()),
            ..Default::default()
        },
        tx: TxEnv {
            caller: origin,
            gas_price: U256::from(gas_price.unwrap_or(fork_gas_price)),
            chain_id: Some(override_chain_id.unwrap_or(rpc_chain_id)),
            gas_limit: block.header().gas_limit() as u64,
            ..Default::default()
        },
    };

    apply_chain_and_block_specific_env_changes::<N>(&mut env, &block);

    Ok((env, block))
}

/// Depending on the configured chain id and block number this should apply any specific changes
///
/// - checks for prevrandao mixhash after merge
/// - applies chain specifics: on Arbitrum `block.number` is the L1 block
///
/// Should be called with proper chain id (retrieved from provider if not provided).
pub fn apply_chain_and_block_specific_env_changes<N: Network>(
    env: &mut revm::primitives::Env,
    block: &N::BlockResponse,
) {
    if let Ok(chain) = NamedChain::try_from(env.cfg.chain_id) {
        let block_number = block.header().number();

        match chain {
            NamedChain::Mainnet => {
                // after merge difficulty is supplanted with prevrandao EIP-4399
                if block_number >= 15_537_351u64 {
                    env.block.difficulty = env.block.prevrandao.unwrap_or_default().into();
                }

                return;
            }
            NamedChain::Arbitrum |
            NamedChain::ArbitrumGoerli |
            NamedChain::ArbitrumNova |
            NamedChain::ArbitrumTestnet => {
                // on arbitrum `block.number` is the L1 block which is included in the
                // `l1BlockNumber` field
                if let Some(l1_block_number) = block
                    .other_fields()
                    .and_then(|other| other.get("l1BlockNumber").cloned())
                    .and_then(|l1_block_number| {
                        serde_json::from_value::<U256>(l1_block_number).ok()
                    })
                {
                    env.block.number = l1_block_number;
                }
            }
            _ => {}
        }
    }

    // if difficulty is `0` we assume it's past merge
    if block.header().difficulty().is_zero() {
        env.block.difficulty = env.block.prevrandao.unwrap_or_default().into();
    }
}
