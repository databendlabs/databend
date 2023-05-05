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

use std::sync::Arc;

use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::TopK;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_pipeline_core::Pipeline;

use crate::fuse_table::FuseStorageFormat;
use crate::io::BlockReader;
use crate::operations::read::build_fuse_parquet_source_pipeline;
use crate::operations::read::fuse_source::build_fuse_native_source_pipeline;

pub fn build_fuse_source_pipeline(
    ctx: Arc<dyn TableContext>,
    pipeline: &mut Pipeline,
    storage_format: FuseStorageFormat,
    block_reader: Arc<BlockReader>,
    plan: &DataSourcePlan,
    top_k: Option<TopK>,
    max_io_requests: usize,
) -> Result<()> {
    let max_threads = ctx.get_settings().get_max_threads()? as usize;

    match storage_format {
        FuseStorageFormat::Native => build_fuse_native_source_pipeline(
            ctx,
            pipeline,
            block_reader,
            max_threads,
            plan,
            top_k,
            max_io_requests,
        ),
        FuseStorageFormat::Parquet => build_fuse_parquet_source_pipeline(
            ctx,
            pipeline,
            block_reader,
            plan,
            max_threads,
            max_io_requests,
        ),
    }
}
