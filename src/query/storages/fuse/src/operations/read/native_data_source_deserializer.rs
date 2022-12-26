use std::any::Any;
use std::sync::Arc;

use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_catalog::plan::PartInfoPtr;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_exception::Result;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;

use crate::io::BlockReader;
use crate::operations::read::native_data_source::DataChunks;
use crate::operations::read::native_data_source::NativeDataSourceMeta;

pub struct NativeDeserializeDataTransform {
    scan_progress: Arc<Progress>,
    block_reader: Arc<BlockReader>,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,
    parts: Vec<PartInfoPtr>,
    chunks: Vec<DataChunks>,
}

impl NativeDeserializeDataTransform {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        block_reader: Arc<BlockReader>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        Ok(ProcessorPtr::create(Box::new(
            NativeDeserializeDataTransform {
                scan_progress,
                block_reader,
                input,
                output,
                output_data: None,
                parts: vec![],
                chunks: vec![],
            },
        )))
    }
}

#[async_trait::async_trait]
impl Processor for NativeDeserializeDataTransform {
    fn name(&self) -> String {
        String::from("NativeDeserializeDataTransform")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data.take() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if !self.chunks.is_empty() {
            if !self.input.has_data() {
                self.input.set_need_data();
            }

            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;
            if let Some(mut source_meta) = data_block.take_meta() {
                if let Some(source_meta) = source_meta
                    .as_mut_any()
                    .downcast_mut::<NativeDataSourceMeta>()
                {
                    self.parts = source_meta.part.clone();
                    self.chunks = std::mem::take(&mut source_meta.chunks);
                    return Ok(Event::Sync);
                }
            }

            unreachable!();
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(mut chunks) = self.chunks.pop() {
            let mut arrays = Vec::with_capacity(chunks.len());

            for (index, chunk) in chunks.iter_mut() {
                if !chunk.has_next() {
                    // No data anymore
                    return Ok(());
                }

                arrays.push((*index, chunk.next_array()?));
            }

            let data_block = self.block_reader.build_block(arrays)?;

            let progress_values = ProgressValues {
                rows: data_block.num_rows(),
                bytes: data_block.memory_size(),
            };
            self.scan_progress.incr(&progress_values);

            self.output_data = Some(data_block);
        }

        Ok(())
    }
}
