//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::any::Any;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use common_base::base::tokio;
use common_catalog::plan::PartInfoPtr;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::BlockMetaInfo;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_pipeline_sources::SyncSource;
use common_pipeline_sources::SyncSourcer;
use serde::Deserializer;
use serde::Serializer;

use crate::parquet_reader::IndexedReaders;
use crate::parquet_reader::ParquetReader;

pub struct ParquetSourceMeta {
    pub parts: Vec<PartInfoPtr>,
    /// The readers' order is the same of column nodes of the source schema.
    pub readers: Vec<IndexedReaders>,
}

impl Debug for ParquetSourceMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetSourceMeta")
            .field("part", &self.parts)
            .finish()
    }
}

impl serde::Serialize for ParquetSourceMeta {
    fn serialize<S>(&self, _: S) -> common_exception::Result<S::Ok, S::Error>
    where S: Serializer {
        unimplemented!("Unimplemented serialize ParquetSourceMeta")
    }
}

impl<'de> serde::Deserialize<'de> for ParquetSourceMeta {
    fn deserialize<D>(_: D) -> common_exception::Result<Self, D::Error>
    where D: Deserializer<'de> {
        unimplemented!("Unimplemented deserialize ParquetSourceMeta")
    }
}

#[typetag::serde(name = "parquet_source")]
impl BlockMetaInfo for ParquetSourceMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone ParquetSourceMeta")
    }

    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals ParquetSourceMeta")
    }
}

pub struct SyncParquetSource {
    ctx: Arc<dyn TableContext>,
    block_reader: Arc<ParquetReader>,
}

impl SyncParquetSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        block_reader: Arc<ParquetReader>,
    ) -> Result<ProcessorPtr> {
        SyncSourcer::create(ctx.clone(), output, SyncParquetSource { ctx, block_reader })
    }
}

impl SyncSource for SyncParquetSource {
    const NAME: &'static str = "SyncParquetSource";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        match self.ctx.get_partition() {
            None => Ok(None),
            Some(part) => Ok(Some(DataBlock::empty_with_meta(Box::new(
                ParquetSourceMeta {
                    parts: vec![part.clone()],
                    readers: vec![self.block_reader.readers_from_blocking_io(part)?],
                },
            )))),
        }
    }
}

pub struct AsyncParquetSource {
    finished: bool,
    batch_size: usize,
    ctx: Arc<dyn TableContext>,
    block_reader: Arc<ParquetReader>,

    output: Arc<OutputPort>,
    output_data: Option<(Vec<PartInfoPtr>, Vec<IndexedReaders>)>,
}

impl AsyncParquetSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        block_reader: Arc<ParquetReader>,
    ) -> Result<ProcessorPtr> {
        let batch_size = ctx.get_settings().get_storage_fetch_part_num()? as usize;
        Ok(ProcessorPtr::create(Box::new(AsyncParquetSource {
            ctx,
            output,
            batch_size,
            block_reader,
            finished: false,
            output_data: None,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for AsyncParquetSource {
    fn name(&self) -> String {
        String::from("AsyncParquetSource")
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

        if let Some((parts, readers)) = self.output_data.take() {
            let output = DataBlock::empty_with_meta(Box::new(ParquetSourceMeta { parts, readers }));
            self.output.push_data(Ok(output));
        }

        Ok(Event::Async)
    }

    async fn async_process(&mut self) -> Result<()> {
        let parts = self.ctx.get_partitions(self.batch_size);

        if !parts.is_empty() {
            let mut readers = Vec::with_capacity(parts.len());
            for part in &parts {
                let part = part.clone();
                let block_reader = self.block_reader.clone();

                readers.push(async move {
                    let handler = tokio::spawn(async move {
                        block_reader.readers_from_non_blocking_io(part).await
                    });
                    handler.await.unwrap()
                });
            }

            self.output_data = Some((parts, futures::future::try_join_all(readers).await?));
            return Ok(());
        }

        self.finished = true;
        Ok(())
    }
}
