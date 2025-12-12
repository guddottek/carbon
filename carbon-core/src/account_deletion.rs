use async_trait::async_trait;

use crate::{
    datasource::AccountDeletion, error::CarbonResult, filter::Filter, processor::Processor,
};

pub struct AccountDeletionPipe {
    pub processor: Box<dyn Processor<InputType = AccountDeletion> + Send + Sync>,
    pub filters: Vec<Box<dyn Filter + Send + Sync + 'static>>,
}

#[async_trait]
pub trait AccountDeletionPipes: Send + Sync {
    async fn run(&mut self, account_deletion: AccountDeletion) -> CarbonResult<()>;

    fn filters(&self) -> &Vec<Box<dyn Filter + Send + Sync + 'static>>;
}

#[async_trait]
impl AccountDeletionPipes for AccountDeletionPipe {
    async fn run(&mut self, account_deletion: AccountDeletion) -> CarbonResult<()> {
        self.processor.process(account_deletion).await?;

        Ok(())
    }

    fn filters(&self) -> &Vec<Box<dyn Filter + Send + Sync + 'static>> {
        &self.filters
    }
}
