use std::any::Any;
use std::sync::Arc;

use common_base::base::GlobalUniqName;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::arrow::serialize_column;
use common_expression::BlockEntry;
use common_expression::BlockMetaInfoDowncast;
use common_expression::BlockMetaInfoPtr;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::Processor;
use common_storage::DataOperator;
use opendal::Operator;

use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::serde::transform_group_by_serializer::serialize_group_by;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;

pub struct TransformGroupBySpillWriter<Method: HashMethodBounds> {
    method: Method,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    operator: Operator,
    spilled_meta: Option<BlockMetaInfoPtr>,
    spilling_meta: Option<AggregateMeta<Method, ()>>,
    writing_data_block: Option<(isize, usize, Vec<Vec<u8>>)>,
}

impl<Method: HashMethodBounds> TransformGroupBySpillWriter<Method> {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        method: Method,
        operator: Operator,
    ) -> Box<dyn Processor> {
        Box::new(TransformGroupBySpillWriter::<Method> {
            method,
            input,
            output,
            operator,
            spilled_meta: None,
            spilling_meta: None,
            writing_data_block: None,
        })
    }
}

#[async_trait::async_trait]
impl<Method: HashMethodBounds> Processor for TransformGroupBySpillWriter<Method> {
    fn name(&self) -> String {
        String::from("TransformGroupBySpillWriter")
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

        if let Some(spilled_meta) = self.spilled_meta.take() {
            self.output
                .push_data(Ok(DataBlock::empty_with_meta(spilled_meta)));
            return Ok(Event::NeedConsume);
        }

        if self.writing_data_block.is_some() {
            self.input.set_not_need_data();
            return Ok(Event::Async);
        }

        if self.spilling_meta.is_some() {
            self.input.set_not_need_data();
            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;

            if let Some(block_meta) = data_block
                .get_meta()
                .and_then(AggregateMeta::<Method, ()>::downcast_ref_from)
            {
                if matches!(block_meta, AggregateMeta::Spilling(_)) {
                    self.input.set_not_need_data();
                    let block_meta = data_block.take_meta().unwrap();
                    self.spilling_meta = AggregateMeta::<Method, ()>::downcast_from(block_meta);
                    return Ok(Event::Sync);
                }
            }

            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(spilling_meta) = self.spilling_meta.take() {
            if let AggregateMeta::Spilling(payload) = spilling_meta {
                let bucket = payload.bucket;
                let data_block = serialize_group_by(&self.method, payload)?;
                let columns = get_columns(data_block);

                let mut total_size = 0;
                let mut columns_data = Vec::with_capacity(columns.len());
                for column in columns.into_iter() {
                    let column = column.value.as_column().unwrap();
                    let column_data = serialize_column(column);
                    total_size += column_data.len();
                    columns_data.push(column_data);
                }

                self.writing_data_block = Some((bucket, total_size, columns_data));
                return Ok(());
            }

            return Err(ErrorCode::Internal(""));
        }

        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        if let Some((bucket, total_size, data)) = self.writing_data_block.take() {
            let unique_name = GlobalUniqName::unique();
            let location = unique_name;
            let object = self.operator.object(&location);

            // temp code: waiting https://github.com/datafuselabs/opendal/pull/1431
            let mut write_data = Vec::with_capacity(total_size);
            let mut columns_layout = Vec::with_capacity(data.len());

            for data in data.into_iter() {
                columns_layout.push(data.len());
                write_data.extend(data);
            }

            object.write(write_data).await?;
            self.spilled_meta = Some(AggregateMeta::<Method, ()>::create_spilled(
                bucket,
                location,
                columns_layout,
            ));
        }

        Ok(())
    }
}

fn get_columns(data_block: DataBlock) -> Vec<BlockEntry> {
    data_block.columns().to_vec()
}
