use std::sync::Arc;
use common_base::base::{Progress, ProgressValues};
use common_catalog::table_context::TableContext;
use common_datablocks::{BlockMetaInfo, DataBlock};
use common_exception::ErrorCode;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_transforms::processors::transforms::{Transform, Transformer};
use crate::io::BlockReader;
use crate::operations::read::parquet_data_source::DataSourceMeta;
use common_exception::Result;
use common_pipeline_core::processors::port::{InputPort, OutputPort};

pub struct DeserializeDataTransform {
    scan_progress: Arc<Progress>,
    block_reader: Arc<BlockReader>,
}

impl DeserializeDataTransform {
    pub fn create(ctx: Arc<dyn TableContext>, block_reader: Arc<BlockReader>, input: Arc<InputPort>, output: Arc<OutputPort>) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        Ok(Transformer::create(input, output, DeserializeDataTransform { scan_progress, block_reader }))
    }
}

impl Transform for DeserializeDataTransform {
    const NAME: &'static str = "DeserializeDataTransform";

    fn transform(&mut self, mut data: DataBlock) -> Result<DataBlock> {
        if let Some(mut source_meta) = data.take_meta() {
            if let Some(source_meta) = source_meta.as_mut_any().downcast_mut::<DataSourceMeta>() {
                let data_block = self.block_reader.deserialize(
                    source_meta.part.clone(),
                    source_meta.data.take().unwrap(),
                )?;

                let progress_values = ProgressValues {
                    rows: data_block.num_rows(),
                    bytes: data_block.memory_size(),
                };
                self.scan_progress.incr(&progress_values);

                return Ok(data_block);
            }
        }

        Err(ErrorCode::Internal("Block meta state is error"))
    }
}
