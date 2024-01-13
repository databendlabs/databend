// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::DataBlock;
use databend_common_metrics::storage::*;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sources::SyncSource;
use databend_common_pipeline_sources::SyncSourcer;
use serde::Deserializer;
use serde::Serializer;

use crate::parquet2::parquet_reader::Parquet2PartData;
use crate::parquet2::parquet_reader::Parquet2Reader;
use crate::ParquetPart;

pub struct Parquet2SourceMeta {
    pub parts: Vec<(PartInfoPtr, Parquet2PartData)>,
}

impl Debug for Parquet2SourceMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetSourceMeta")
            .field(
                "part",
                &self.parts.iter().map(|(p, _)| p).collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl serde::Serialize for Parquet2SourceMeta {
    fn serialize<S>(&self, _: S) -> databend_common_exception::Result<S::Ok, S::Error>
    where S: Serializer {
        unimplemented!("Unimplemented serialize ParquetSourceMeta")
    }
}

impl<'de> serde::Deserialize<'de> for Parquet2SourceMeta {
    fn deserialize<D>(_: D) -> databend_common_exception::Result<Self, D::Error>
    where D: Deserializer<'de> {
        unimplemented!("Unimplemented deserialize ParquetSourceMeta")
    }
}

#[typetag::serde(name = "parquet_source")]
impl BlockMetaInfo for Parquet2SourceMeta {
    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals ParquetSourceMeta")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone ParquetSourceMeta")
    }
}

pub struct SyncParquet2Source {
    ctx: Arc<dyn TableContext>,
    block_reader: Arc<Parquet2Reader>,
}

impl SyncParquet2Source {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        block_reader: Arc<Parquet2Reader>,
    ) -> Result<ProcessorPtr> {
        SyncSourcer::create(ctx.clone(), output, SyncParquet2Source {
            ctx,
            block_reader,
        })
    }
}

impl SyncSource for SyncParquet2Source {
    const NAME: &'static str = "SyncParquetSource";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        match self.ctx.get_partition() {
            None => Ok(None),
            Some(part) => {
                let part_clone = part.clone();
                let data = self
                    .block_reader
                    .readers_from_blocking_io(self.ctx.clone(), part)?;
                metrics_inc_copy_read_part_counter();
                Ok(Some(DataBlock::empty_with_meta(Box::new(
                    Parquet2SourceMeta {
                        parts: vec![(part_clone, data)],
                    },
                ))))
            }
        }
    }
}

pub struct AsyncParquet2Source {
    finished: bool,
    ctx: Arc<dyn TableContext>,
    block_reader: Arc<Parquet2Reader>,

    output: Arc<OutputPort>,
    output_data: Option<Vec<(PartInfoPtr, Parquet2PartData)>>,
}

impl AsyncParquet2Source {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        block_reader: Arc<Parquet2Reader>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(AsyncParquet2Source {
            ctx,
            output,
            block_reader,
            finished: false,
            output_data: None,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for AsyncParquet2Source {
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

        if let Some(parts) = self.output_data.take() {
            let output = DataBlock::empty_with_meta(Box::new(Parquet2SourceMeta { parts }));
            self.output.push_data(Ok(output));
        }

        Ok(Event::Async)
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        let parts = self.ctx.get_partitions(1);

        if !parts.is_empty() {
            let part = parts[0].clone();
            let parquet_part = ParquetPart::from_part(&part)?;
            let block_reader = self.block_reader.clone();
            let data = block_reader
                .readers_from_non_blocking_io(self.ctx.clone(), parquet_part)
                .await?;
            metrics_inc_copy_read_part_counter();
            self.output_data = Some(vec![(part, data)]);
        } else {
            self.finished = true;
            self.output_data = None;
        }
        Ok(())
    }
}
