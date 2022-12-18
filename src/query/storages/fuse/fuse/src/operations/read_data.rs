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

use std::sync::Arc;

use common_base::runtime::Runtime;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::Projection;
use common_catalog::plan::PushDownInfo;
use common_catalog::table_context::TableContext;
use common_config::GlobalConfig;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_pipeline_core::Pipeline;
use common_sql::evaluator::EvalNode;
use common_sql::evaluator::Evaluator;
use tracing::info;

use crate::fuse_lazy_part::FuseLazyPartInfo;
use crate::io::BlockReader;
use crate::operations::FuseTableSource;
use crate::FuseTable;

/// Read data kind to avoid OOM.
pub enum ReadDataKind {
    // Compact/Recluster data, need less io requests.
    // io_requests = max_threads()
    OptimizeDataLessIORequests,
    // Read column block data, need adjust io requests.
    // io requests = memory-size/avg(blocks-size)
    BlockDataAdjustIORequests,
}

impl FuseTable {
    pub fn create_block_reader(&self, projection: Projection) -> Result<Arc<BlockReader>> {
        let table_schema = self.table_info.schema();
        BlockReader::create(self.operator.clone(), table_schema, projection)
    }

    // Build the block reader.
    fn build_block_reader(&self, plan: &DataSourcePlan) -> Result<Arc<BlockReader>> {
        match PushDownInfo::prewhere_of_push_downs(&plan.push_downs) {
            None => {
                let projection = PushDownInfo::projection_of_push_downs(
                    &self.table_info.schema(),
                    &plan.push_downs,
                );
                self.create_block_reader(projection)
            }
            Some(v) => self.create_block_reader(v.output_columns),
        }
    }

    // Build the prewhere reader.
    fn build_prewhere_reader(&self, plan: &DataSourcePlan) -> Result<Arc<BlockReader>> {
        match PushDownInfo::prewhere_of_push_downs(&plan.push_downs) {
            None => {
                let projection = PushDownInfo::projection_of_push_downs(
                    &self.table_info.schema(),
                    &plan.push_downs,
                );
                self.create_block_reader(projection)
            }
            Some(v) => self.create_block_reader(v.prewhere_columns),
        }
    }

    // Build the prewhere filter executor.
    fn build_prewhere_filter_executor(
        &self,
        _ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        schema: DataSchemaRef,
    ) -> Result<Arc<Option<EvalNode>>> {
        Ok(
            match PushDownInfo::prewhere_of_push_downs(&plan.push_downs) {
                None => Arc::new(None),
                Some(v) => {
                    let executor = Evaluator::eval_expression(&v.filter, schema.as_ref())?;
                    Arc::new(Some(executor))
                }
            },
        )
    }

    // Build the remain reader.
    fn build_remain_reader(&self, plan: &DataSourcePlan) -> Result<Arc<Option<BlockReader>>> {
        Ok(
            match PushDownInfo::prewhere_of_push_downs(&plan.push_downs) {
                None => Arc::new(None),
                Some(v) => {
                    if v.remain_columns.is_empty() {
                        Arc::new(None)
                    } else {
                        Arc::new(Some((*self.create_block_reader(v.remain_columns)?).clone()))
                    }
                }
            },
        )
    }

    fn adjust_io_request(
        &self,
        ctx: &Arc<dyn TableContext>,
        projection: &Projection,
        kind: ReadDataKind,
    ) -> Result<usize> {
        Ok(match kind {
            ReadDataKind::OptimizeDataLessIORequests => {
                ctx.get_settings().get_max_threads()? as usize
            }
            ReadDataKind::BlockDataAdjustIORequests => {
                let conf = GlobalConfig::instance();
                let mut max_memory_usage = ctx.get_settings().get_max_memory_usage()? as usize;
                if conf.query.table_cache_enabled {
                    // Removing bloom index memory size.
                    max_memory_usage -= conf.query.table_cache_bloom_index_data_bytes as usize;
                }

                // Assume 300MB one block file after decompressed.
                let block_file_size = 300 * 1024 * 1024_usize;
                let table_column_len = self.table_info.schema().fields().len();
                let per_column_bytes = block_file_size / table_column_len;
                let scan_column_bytes = per_column_bytes * projection.len();
                let estimate_io_requests = max_memory_usage / scan_column_bytes;

                let setting_io_requests = std::cmp::max(
                    1,
                    ctx.get_settings().get_max_storage_io_requests()? as usize,
                );
                let adjust_io_requests = std::cmp::max(1, estimate_io_requests);
                std::cmp::min(adjust_io_requests, setting_io_requests)
            }
        })
    }

    #[inline]
    pub fn do_read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        read_kind: ReadDataKind,
    ) -> Result<()> {
        let mut lazy_init_segments = Vec::with_capacity(plan.parts.len());

        for part in &plan.parts.partitions {
            if let Some(lazy_part_info) = part.as_any().downcast_ref::<FuseLazyPartInfo>() {
                lazy_init_segments.push(lazy_part_info.segment_location.clone());
            }
        }

        if !lazy_init_segments.is_empty() {
            let table = self.clone();
            let table_info = self.table_info.clone();
            let push_downs = plan.push_downs.clone();
            let query_ctx = ctx.clone();
            let dal = self.operator.clone();

            // TODO: need refactor
            pipeline.set_on_init(move || {
                let table = table.clone();
                let table_info = table_info.clone();
                let ctx = query_ctx.clone();
                let dal = dal.clone();
                let push_downs = push_downs.clone();
                let lazy_init_segments = lazy_init_segments.clone();

                let partitions = Runtime::with_worker_threads(2, None)?.block_on(async move {
                    let (_statistics, partitions) = table
                        .prune_snapshot_blocks(
                            ctx,
                            dal,
                            push_downs,
                            table_info,
                            lazy_init_segments,
                            0,
                        )
                        .await?;

                    Result::<_, ErrorCode>::Ok(partitions)
                })?;

                query_ctx.try_set_partitions(partitions)?;

                Ok(())
            });
        }

        let projection =
            PushDownInfo::projection_of_push_downs(&self.table_info.schema(), &plan.push_downs);
        let max_io_requests = self.adjust_io_request(&ctx, &projection, read_kind)?;
        let block_reader = self.build_block_reader(plan)?;
        let prewhere_reader = self.build_prewhere_reader(plan)?;
        let prewhere_filter =
            self.build_prewhere_filter_executor(ctx.clone(), plan, prewhere_reader.schema())?;
        let remain_reader = self.build_remain_reader(plan)?;

        info!("read block data adjust max io requests:{}", max_io_requests);

        // Add source pipe.
        pipeline.add_source(
            |output| {
                FuseTableSource::create(
                    ctx.clone(),
                    output,
                    block_reader.clone(),
                    prewhere_reader.clone(),
                    prewhere_filter.clone(),
                    remain_reader.clone(),
                    self.storage_format,
                )
            },
            max_io_requests,
        )?;

        // Resize pipeline to max threads.
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let resize_to = std::cmp::min(max_threads, max_io_requests);
        info!(
            "read block pipeline resize from:{} to:{}",
            max_io_requests, resize_to
        );
        pipeline.resize(resize_to)
    }
}
