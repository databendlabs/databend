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

use std::sync::Arc;

use common_catalog::plan::prewhere_of_push_downs;
use common_catalog::plan::projection_of_push_downs;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::Projection;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_pipeline_core::Pipeline;
use common_sql::evaluator::EvalNode;
use common_sql::evaluator::Evaluator;

use super::ParquetTable;
use super::TableContext;
use crate::ParquetReader;
use crate::ParquetTableSource;

impl ParquetTable {
    pub fn create_reader(&self, projection: Projection) -> Result<Arc<ParquetReader>> {
        let table_schema = self.table_info.schema();
        ParquetReader::create(self.operator.clone(), table_schema, projection)
    }

    // Build the block reader.
    fn build_reader(&self, plan: &DataSourcePlan) -> Result<Arc<ParquetReader>> {
        match prewhere_of_push_downs(&plan.push_downs) {
            None => {
                let projection = projection_of_push_downs(&plan.schema(), &plan.push_downs);
                self.create_reader(projection)
            }
            Some(v) => self.create_reader(v.output_columns),
        }
    }

    // Build the prewhere reader.
    fn build_prewhere_reader(&self, plan: &DataSourcePlan) -> Result<Arc<ParquetReader>> {
        match prewhere_of_push_downs(&plan.push_downs) {
            None => {
                let projection = projection_of_push_downs(&plan.schema(), &plan.push_downs);
                self.create_reader(projection)
            }
            Some(v) => self.create_reader(v.prewhere_columns),
        }
    }

    // Build the prewhere filter executor.
    fn build_prewhere_filter_executor(
        &self,
        _ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        schema: DataSchemaRef,
    ) -> Result<Arc<Option<EvalNode>>> {
        Ok(match prewhere_of_push_downs(&plan.push_downs) {
            None => Arc::new(None),
            Some(v) => {
                let executor = Evaluator::eval_expression(&v.filter, schema.as_ref())?;
                Arc::new(Some(executor))
            }
        })
    }

    // Build the remain reader.
    fn build_remain_reader(&self, plan: &DataSourcePlan) -> Result<Arc<Option<ParquetReader>>> {
        Ok(match prewhere_of_push_downs(&plan.push_downs) {
            None => Arc::new(None),
            Some(v) => {
                if v.remain_columns.is_empty() {
                    Arc::new(None)
                } else {
                    Arc::new(Some((*self.create_reader(v.remain_columns)?).clone()))
                }
            }
        })
    }

    fn adjust_io_request(
        &self,
        ctx: &Arc<dyn TableContext>,
        projection: &Projection,
    ) -> Result<usize> {
        let conf = ctx.get_config();
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
        Ok(std::cmp::min(adjust_io_requests, setting_io_requests))
    }

    #[inline]
    pub(super) fn do_read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let projection = projection_of_push_downs(&plan.schema(), &plan.push_downs);
        let max_io_requests = self.adjust_io_request(&ctx, &projection)?;
        let block_reader = self.build_reader(plan)?;
        let prewhere_reader = self.build_prewhere_reader(plan)?;
        let prewhere_filter =
            self.build_prewhere_filter_executor(ctx.clone(), plan, prewhere_reader.schema())?;
        let remain_reader = self.build_remain_reader(plan)?;

        // Add source pipe.
        pipeline.add_source(
            |output| {
                ParquetTableSource::create(
                    ctx.clone(),
                    output,
                    block_reader.clone(),
                    prewhere_reader.clone(),
                    prewhere_filter.clone(),
                    remain_reader.clone(),
                )
            },
            max_io_requests,
        )?;

        // Resize pipeline to max threads.
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let resize_to = std::cmp::min(max_threads, max_io_requests);

        pipeline.resize(resize_to)
    }
}
