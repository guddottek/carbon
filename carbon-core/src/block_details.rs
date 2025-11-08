use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    datasource::BlockDetails, error::CarbonResult, filter::Filter, metrics::MetricsCollection,
    processor::Processor,
};

pub struct BlockDetailsPipe {
    pub processor: Box<dyn Processor<InputType = BlockDetails> + Send + Sync>,
    pub filters: Vec<Box<dyn Filter + Send + Sync + 'static>>,
}

#[async_trait]
pub trait BlockDetailsPipes: Send + Sync {
    async fn run(
        &mut self,
        block_details: BlockDetails,
        metrics: Arc<MetricsCollection>,
    ) -> CarbonResult<()>;

    fn filters(&self) -> &Vec<Box<dyn Filter + Send + Sync + 'static>>;
}

#[async_trait]
impl BlockDetailsPipes for BlockDetailsPipe {
    async fn run(
        &mut self,
        block_details: BlockDetails,
        metrics: Arc<MetricsCollection>,
    ) -> CarbonResult<()> {
        log::trace!(
            "Block details::run(block_details: {:?}, metrics)",
            block_details,
        );

        self.processor.process(block_details, metrics).await?;

        Ok(())
    }

    fn filters(&self) -> &Vec<Box<dyn Filter + Send + Sync + 'static>> {
        &self.filters
    }
}
