use std::sync::Arc;

use async_trait::async_trait;

use crate::{error::CarbonResult, metrics::MetricsCollection};

#[async_trait]
pub trait Processor {
    type InputType;

    async fn process(
        &mut self,
        data: Self::InputType,
        metrics: Arc<MetricsCollection>,
    ) -> CarbonResult<()>;
}
