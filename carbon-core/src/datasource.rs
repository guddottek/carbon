use async_trait::async_trait;
use solana_account::Account;
use solana_address::Address;
use solana_hash::Hash;
use solana_signature::Signature;
use solana_transaction::versioned::VersionedTransaction;
use solana_transaction_status_client_types::{Rewards, TransactionStatusMeta};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::error::CarbonResult;

#[async_trait]
pub trait Datasource: Send + Sync {
    async fn consume(
        &self,
        id: DatasourceId,
        sender: tokio::sync::mpsc::Sender<(Update, DatasourceId)>,
        cancellation_token: CancellationToken,
    ) -> CarbonResult<()>;

    fn update_types(&self) -> Vec<UpdateType>;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatasourceId(String);

impl DatasourceId {
    pub fn new_unique() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn new_named(name: &str) -> Self {
        Self(name.to_string())
    }
}

#[derive(Debug, Clone)]
pub enum Update {
    Account(AccountUpdate),
    Transaction(Box<TransactionUpdate>),
    AccountDeletion(AccountDeletion),
    BlockDetails(BlockDetails),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateType {
    AccountUpdate,
    Transaction,
    AccountDeletion,
}

#[derive(Debug, Clone)]
pub struct AccountUpdate {
    pub pubkey: Address,
    pub account: Account,
    pub slot: u64,
    pub transaction_signature: Option<Signature>,
}

#[derive(Debug, Clone)]
pub struct BlockDetails {
    pub slot: u64,
    pub block_hash: Option<Hash>,
    pub previous_block_hash: Option<Hash>,
    pub rewards: Option<Rewards>,
    pub num_reward_partitions: Option<u64>,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct AccountDeletion {
    pub pubkey: Address,
    pub slot: u64,
    pub transaction_signature: Option<Signature>,
}

#[derive(Debug, Clone)]
pub struct TransactionUpdate {
    pub signature: Signature,
    pub transaction: VersionedTransaction,
    pub meta: TransactionStatusMeta,
    pub is_vote: bool,
    pub slot: u64,
    pub index: Option<u64>,
    pub block_time: Option<i64>,
    pub block_hash: Option<Hash>,
}
