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
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::Pipeline;
use common_pipeline_core::SourcePipeBuilder;

use crate::io::BlockReader;
use crate::operations::read::parquet_knn_reader::ReadParquetKnnSource;
use crate::FuseStorageFormat;
pub fn build_fuse_knn_pipeline(
    ctx: Arc<dyn TableContext>,
    pipeline: &mut Pipeline,
    storage_format: FuseStorageFormat,
    block_reader: Arc<BlockReader>,
    plan: &DataSourcePlan,
) -> Result<()> {
    match storage_format {
        FuseStorageFormat::Parquet => {
            build_fuse_knn_pipeline_parquet(ctx, pipeline, block_reader, plan)?;
        }
        FuseStorageFormat::Native => todo!(),
    }
    Ok(())
}

fn build_fuse_knn_pipeline_parquet(
    ctx: Arc<dyn TableContext>,
    pipeline: &mut Pipeline,
    block_reader: Arc<BlockReader>,
    plan: &DataSourcePlan,
) -> Result<()> {
    let mut source_builder = SourcePipeBuilder::create();
    let output = OutputPort::create();
    let table = ctx.build_table_from_source_plan(plan)?;
    let pushdowns = plan.push_downs.as_ref().unwrap();
    let limit = pushdowns.limit.unwrap();
    let target = pushdowns
        .similarity
        .as_ref()
        .unwrap()
        .target
        .as_ref()
        .as_array()
        .unwrap()
        .as_decimal()
        .unwrap()
        .clone()
        .into();
    let metric = pushdowns.similarity.as_ref().unwrap().metric.clone();
    let vector_index = pushdowns.similarity.as_ref().unwrap().vector_index.clone();
    source_builder.add_source(
        output.clone(),
        ReadParquetKnnSource::create(
            ctx,
            table,
            output,
            block_reader,
            limit,
            target,
            metric,
            vector_index,
        ),
    );
    pipeline.add_pipe(source_builder.finalize());
    Ok(())
}
