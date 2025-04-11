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
use std::collections::VecDeque;
use std::mem;
use std::sync::Arc;

use arrow_schema::Schema;
use async_trait::async_trait;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchema;
use databend_common_meta_app::principal::StageFileCompression;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_storages_common_index::BloomIndex;
use opendal::Operator;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::basic::ZstdLevel;
use parquet::file::properties::EnabledStatistics;
use parquet::file::properties::WriterProperties;
use parquet::file::properties::WriterVersion;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use crate::io::create_inverted_index_builders;
use crate::io::StreamBlockBuilder;
use crate::io::StreamBlockProperties;
use crate::FuseTable;

pub struct TransformBlockWriter {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    properties: Arc<StreamBlockProperties>,
    builder: Option<StreamBlockBuilder>,
    dal: Operator,
    table_id: Option<u64>, // Only used in multi table insert
    kind: MutationKind,
}

impl TransformBlockWriter {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        table: &FuseTable,
        kind: MutationKind,
        table_meta_timestamps: TableMetaTimestamps,
        with_tid: bool,
    ) -> Result<ProcessorPtr> {
        let do_append = matches!(kind, MutationKind::Insert | MutationKind::Replace);
        let properties = StreamBlockProperties::try_create(ctx, table, table_meta_timestamps, do_append)?;
        Ok(ProcessorPtr::create(Box::new(
            TransformBlockWriter {
                input,
                output,
                properties,
                builder: None,
                dal: table.get_operator(),
                table_id: if with_tid { Some(table.get_id()) } else { None },
                kind,
            }
        )))
    }
    pub fn reinit_writer(&mut self) -> Result<()> {
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        todo!()
    }
}

#[async_trait]
impl Processor for TransformBlockWriter {
    fn name(&self) -> String {
        "TransformBlockWriter".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        todo!()
    }

    fn process(&mut self) -> Result<()> {
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        todo!()
    }
}
