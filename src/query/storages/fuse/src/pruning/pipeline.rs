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

use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::Pipeline;
use databend_storages_common_pruner::InternalColumnPruner;
use opendal::Operator;

use super::processors::segment_source::SegmentSource;

pub fn build_pruning_pipelines(
    ctx: Arc<dyn TableContext>,
    push_down: &PushDownInfo,
    dal: Operator,
) -> Result<Vec<Pipeline>> {
    let mut pipelines = vec![];
    let max_io_requests = ctx.get_settings().get_max_storage_io_requests()?;
    let func_ctx = ctx.get_function_context()?;
    let filter_expr = push_down
        .filters
        .as_ref()
        .map(|f| f.filter.as_expr(&BUILTIN_FUNCTIONS));
    let internal_column_pruner = InternalColumnPruner::try_create(func_ctx, filter_expr.as_ref());

    pipelines.push(build_range_index_pruning_pipeline(
        ctx,
        max_io_requests as usize,
        dal,
        internal_column_pruner,
    )?);

    Ok(pipelines)
}

fn build_range_index_pruning_pipeline(
    ctx: Arc<dyn TableContext>,
    read_segment_concurrency: usize,
    dal: Operator,
    internal_column_pruner: Option<Arc<InternalColumnPruner>>,
) -> Result<Pipeline> {
    let mut range_index_pruning_pipeline = Pipeline::create();
    range_index_pruning_pipeline.set_max_threads(ctx.get_settings().get_max_threads()? as usize);

    range_index_pruning_pipeline.add_source(
        |output| SegmentSource::create(ctx, dal, internal_column_pruner, output),
        read_segment_concurrency,
    );
    Ok(range_index_pruning_pipeline)
}
