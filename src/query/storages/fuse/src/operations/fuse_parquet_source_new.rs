use std::any::Any;
use std::sync::Arc;
use serde::{Deserializer, Serializer};
use tracing::info;
use common_catalog::plan::PartInfoPtr;
use common_catalog::table_context::TableContext;
use common_datablocks::{BlockMetaInfo, BlockMetaInfoPtr, DataBlock};
use common_pipeline_core::processors::Processor;
use common_pipeline_core::processors::processor::{Event, ProcessorPtr};
use crate::io::BlockReader;
use common_exception::{ErrorCode, Result};
use common_pipeline_core::Pipeline;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_transforms::processors::transforms::{Transform, Transformer};

#[derive(Debug, PartialEq)]
struct DataSourceMeta {
    part: PartInfoPtr,
    data: Option<Vec<(usize, Vec<u8>)>>,
}

impl DataSourceMeta {
    pub fn create(part: PartInfoPtr, data: Vec<(usize, Vec<u8>)>) -> BlockMetaInfoPtr {
        Box::new(DataSourceMeta { part, data: Some(data) })
    }
}

impl serde::Serialize for DataSourceMeta {
    fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error> where S: Serializer {
        unimplemented!("")
    }
}

impl<'de> serde::Deserialize<'de> for DataSourceMeta {
    fn deserialize<D>(_: D) -> Result<Self, D::Error> where D: Deserializer<'de> {
        unimplemented!("")
    }
}

#[typetag::serde(name = "fuse_data_source")]
impl BlockMetaInfo for DataSourceMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("")
    }

    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        match info.as_any().downcast_ref::<DataSourceMeta>() {
            None => false,
            Some(other) => self == other,
        }
    }
}

struct ReadParquetDataSource {
    finished: bool,
    ctx: Arc<dyn TableContext>,
    block_reader: Arc<BlockReader>,

    output: Arc<OutputPort>,
    output_data: Option<(PartInfoPtr, Vec<(usize, Vec<u8>)>)>,
}

#[async_trait::async_trait]
impl Processor for ReadParquetDataSource {
    fn name(&self) -> String {
        String::from("ReadParquetDataSource")
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

        if let Some((part, data)) = self.output_data.take() {
            let output = DataBlock::empty_with_meta(DataSourceMeta::create(part, data));
            self.output.push_data(Ok(output));
            return Ok(Event::NeedConsume);
        }

        Ok(Event::Async)
    }

    async fn async_process(&mut self) -> Result<()> {
        if let Some(part) = self.ctx.try_get_part() {
            self.output_data = Some((part.clone(), self.block_reader.read_columns_data(part).await?));
            return Ok(());
        }

        self.finished = true;
        Ok(())
    }
}

struct DeserializeDataTransform {
    block_reader: Arc<BlockReader>,
}

impl Transform for DeserializeDataTransform {
    const NAME: &'static str = "DeserializeDataTransform";

    fn transform(&mut self, mut data: DataBlock) -> Result<DataBlock> {
        if let Some(mut source_meta) = data.take_meta() {
            if let Some(source_meta) = source_meta.as_mut_any().downcast_mut::<DataSourceMeta>() {
                return self.block_reader.deserialize(
                    source_meta.part.clone(),
                    source_meta.data.take().unwrap(),
                );
            }
        }

        Err(ErrorCode::Internal("Block meta state is error"))
    }
}


pub fn build_fuse_parquet_source_pipeline(
    ctx: Arc<dyn TableContext>,
    pipeline: &mut Pipeline,
    block_reader: Arc<BlockReader>,
    max_threads: usize,
    max_io_requests: usize,
) -> Result<()> {
    info!("read block data adjust max io requests:{}", max_io_requests);

    pipeline.add_source(|output| Ok(ProcessorPtr::create(Box::new(
        ReadParquetDataSource {
            output,
            ctx: ctx.clone(),
            finished: false,
            output_data: None,
            block_reader: block_reader.clone(),
        }
    ))), max_io_requests)?;

    pipeline.resize(std::cmp::min(max_threads, max_io_requests))?;

    info!(
        "read block pipeline resize from:{} to:{}",
        max_io_requests, pipeline.output_len()
    );

    pipeline.add_transform(|input, output| {
        Ok(Transformer::create(input, output, DeserializeDataTransform {
            block_reader: block_reader.clone(),
        }))
    })
}
