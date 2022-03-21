use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::Result;

use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::SyncSource;
use crate::pipelines::new::processors::SyncSourcer;
use crate::sessions::QueryContext;

pub struct EmptySource {
    finish: bool,
    schema: DataSchemaRef,
}

impl EmptySource {
    pub fn create(
        ctx: Arc<QueryContext>,
        output: Arc<OutputPort>,
        schema: DataSchemaRef,
    ) -> Result<ProcessorPtr> {
        SyncSourcer::create(ctx, output, EmptySource {
            finish: false,
            schema,
        })
    }
}

impl SyncSource for EmptySource {
    const NAME: &'static str = "EmptySource";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finish {
            return Ok(None);
        }

        self.finish = true;
        Ok(Some(DataBlock::empty_with_schema(self.schema.clone())))
    }
}
