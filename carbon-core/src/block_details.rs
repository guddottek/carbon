use async_trait::async_trait;

use crate::{datasource::BlockDetails, error::CarbonResult, filter::Filter, processor::Processor};

pub struct BlockDetailsPipe {
    pub processor: Box<dyn Processor<InputType = BlockDetails> + Send + Sync>,
    pub filters: Vec<Box<dyn Filter + Send + Sync + 'static>>,
}

#[async_trait]
pub trait BlockDetailsPipes: Send + Sync {
    async fn run(&mut self, block_details: BlockDetails) -> CarbonResult<()>;

    fn filters(&self) -> &Vec<Box<dyn Filter + Send + Sync + 'static>>;
}

#[async_trait]
impl BlockDetailsPipes for BlockDetailsPipe {
    async fn run(&mut self, block_details: BlockDetails) -> CarbonResult<()> {
        self.processor.process(block_details).await?;

        Ok(())
    }

    fn filters(&self) -> &Vec<Box<dyn Filter + Send + Sync + 'static>> {
        &self.filters
    }
}
