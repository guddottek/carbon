use async_trait::async_trait;
use solana_account::Account;
use solana_address::Address;
use solana_signature::Signature;

use crate::{error::CarbonResult, filter::Filter, processor::Processor};

#[derive(Debug, Clone)]
pub struct AccountMetadata {
    pub slot: u64,
    pub pubkey: Address,
    pub transaction_signature: Option<Signature>,
}

#[derive(Debug, Clone)]
pub struct DecodedAccount<T> {
    pub lamports: u64,
    pub data: T,
    pub owner: Address,
    pub executable: bool,
    pub rent_epoch: u64,
}

pub trait AccountDecoder<'a> {
    type AccountType;

    fn decode_account(&self, account: &'a Account) -> Option<DecodedAccount<Self::AccountType>>;
}

pub type AccountProcessorInputType<T> = (AccountMetadata, DecodedAccount<T>, Account);

pub struct AccountPipe<T: Send> {
    pub decoder: Box<dyn for<'a> AccountDecoder<'a, AccountType = T> + Send + Sync + 'static>,
    pub processor: Box<dyn Processor<InputType = AccountProcessorInputType<T>> + Send + Sync>,
    pub filters: Vec<Box<dyn Filter + Send + Sync + 'static>>,
}

#[async_trait]
pub trait AccountPipes: Send + Sync {
    async fn run(&mut self, account_with_metadata: (AccountMetadata, Account)) -> CarbonResult<()>;

    fn filters(&self) -> &Vec<Box<dyn Filter + Send + Sync + 'static>>;
}

#[async_trait]
impl<T: Send> AccountPipes for AccountPipe<T> {
    async fn run(&mut self, account_with_metadata: (AccountMetadata, Account)) -> CarbonResult<()> {
        if let Some(decoded_account) = self.decoder.decode_account(&account_with_metadata.1) {
            self.processor
                .process((
                    account_with_metadata.0.clone(),
                    decoded_account,
                    account_with_metadata.1,
                ))
                .await?;
        }
        Ok(())
    }

    fn filters(&self) -> &Vec<Box<dyn Filter + Send + Sync + 'static>> {
        &self.filters
    }
}
