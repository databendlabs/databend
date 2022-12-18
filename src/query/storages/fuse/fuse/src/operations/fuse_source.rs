//  Copyright 2021 Datafuse Labs.
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
use std::sync::Arc;

use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_catalog::plan::PartInfoPtr;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::ColumnRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::FunctionContext;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_sql::evaluator::EvalNode;

use super::fuse_native_source::FuseNativeSource;
use super::fuse_parquet_source::FuseParquetSource;
use crate::fuse_table::FuseStorageFormat;
use crate::io::BlockReader;

pub struct FuseTableSource;

impl FuseTableSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        output_reader: Arc<BlockReader>,
        prewhere_reader: Arc<BlockReader>,
        prewhere_filter: Arc<Option<EvalNode>>,
        remain_reader: Arc<Option<BlockReader>>,
        storage_format: FuseStorageFormat,
    ) -> Result<ProcessorPtr> {
        match storage_format {
            FuseStorageFormat::Parquet => FuseParquetSource::create(
                ctx,
                output,
                output_reader,
                prewhere_reader,
                prewhere_filter,
                remain_reader,
            ),
            FuseStorageFormat::Native => FuseNativeSource::create(
                ctx,
                output,
                output_reader,
                prewhere_reader,
                prewhere_filter,
                remain_reader,
            ),
        }
    }
}
