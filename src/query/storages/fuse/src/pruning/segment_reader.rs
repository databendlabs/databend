use std::any::Any;
use std::sync::Arc;

use common_exception::Result;
use common_expression::DataBlock;
use common_expression::TableSchemaRef;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::Processor;
use storages_common_cache::LoadParams;
use storages_common_table_meta::meta::CompactSegmentInfo;

use crate::io::MetaReaders;
use crate::pruning::segment_info_meta::CompactSegmentInfoMeta;
use crate::pruning::PruningContext;
use crate::pruning::SegmentLocation;

pub struct CompactSegmentSource {
    finished: bool,
    // input: Arc<InputPort>,
    output: Arc<OutputPort>,
    locations: std::vec::IntoIter<SegmentLocation>,

    table_schema: TableSchemaRef,
    pruning_ctx: Arc<PruningContext>,
    output_data: Option<(SegmentLocation, Arc<CompactSegmentInfo>)>,
}

#[async_trait::async_trait]
impl Processor for CompactSegmentSource {
    fn name(&self) -> String {
        String::from("CompactSegmentSource")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.finished {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some((segment_location, segment_info)) = self.output_data.take() {
            let meta = CompactSegmentInfoMeta::create(segment_location, segment_info);
            self.output.push_data(Ok(DataBlock::empty_with_meta(meta)))
        }

        Ok(Event::Async)
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if let Some(segment_location) = self.locations.next() {
            let dal = self.pruning_ctx.dal.clone();
            let table_schema = self.table_schema.clone();

            // Keep in mind that segment_info_read must need a schema
            let segment_reader = MetaReaders::segment_info_reader(dal, table_schema);
            let (location, ver) = segment_location.location.clone();
            let load_params = LoadParams {
                location,
                len_hint: None,
                ver,
                put_cache: true,
            };
            self.output_data = Some((segment_location, segment_reader.read(&load_params).await?));
            return Ok(());
        }

        self.finished = true;
        Ok(())
    }
}
