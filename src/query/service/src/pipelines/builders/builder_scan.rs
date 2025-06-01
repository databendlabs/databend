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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sources::OneBlockSource;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::evaluator::CompoundBlockOperator;
use databend_common_sql::executor::physical_plans::CacheScan;
use databend_common_sql::executor::physical_plans::ConstantTableScan;
use databend_common_sql::executor::physical_plans::ExpressionScan;
use databend_common_sql::executor::physical_plans::TableScan;
use databend_common_sql::plans::CacheSource;

use crate::pipelines::processors::transforms::CacheSourceState;
use crate::pipelines::processors::transforms::HashJoinCacheState;
use crate::pipelines::processors::transforms::TransformAddInternalColumns;
use crate::pipelines::processors::transforms::TransformCacheScan;
use crate::pipelines::processors::transforms::TransformExpressionScan;
use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_table_scan(&mut self, scan: &TableScan) -> Result<()> {
        let table = self.ctx.build_table_from_source_plan(&scan.source)?;
        self.ctx.set_partitions(scan.source.parts.clone())?;
        self.ctx
            .set_wait_runtime_filter(scan.scan_id, self.contain_sink_processor);
        if self.ctx.get_settings().get_enable_prune_pipeline()? {
            if let Some(prune_pipeline) = table.build_prune_pipeline(
                self.ctx.clone(),
                &scan.source,
                &mut self.main_pipeline,
                scan.plan_id,
            )? {
                self.pipelines.push(prune_pipeline);
            }
        }
        table.read_data(
            self.ctx.clone(),
            &scan.source,
            &mut self.main_pipeline,
            true,
        )?;

        // Fill internal columns if needed.
        if let Some(internal_columns) = &scan.internal_column {
            self.main_pipeline
                .add_transformer(|| TransformAddInternalColumns::new(internal_columns.clone()));
        }

        let schema = scan.source.schema();
        let mut projection = scan
            .name_mapping
            .keys()
            .map(|name| schema.index_of(name.as_str()))
            .collect::<Result<Vec<usize>>>()?;
        projection.sort();

        // if projection is sequential, no need to add projection
        if projection != (0..schema.fields().len()).collect::<Vec<usize>>() {
            let ops = vec![BlockOperator::Project { projection }];
            let num_input_columns = schema.num_fields();
            self.main_pipeline.add_transformer(|| {
                CompoundBlockOperator::new(ops.clone(), self.func_ctx.clone(), num_input_columns)
            });
        }

        Ok(())
    }

    pub(crate) fn build_constant_table_scan(&mut self, scan: &ConstantTableScan) -> Result<()> {
        self.main_pipeline.add_source(
            |output| {
                let block = if !scan.values.is_empty() {
                    DataBlock::new_from_columns(scan.values.clone())
                } else {
                    DataBlock::new(vec![], scan.num_rows)
                };
                OneBlockSource::create(output, block)
            },
            1,
        )
    }

    pub(crate) fn build_cache_scan(&mut self, scan: &CacheScan) -> Result<()> {
        let max_threads = self.settings.get_max_threads()?;
        let max_block_size = self.settings.get_max_block_size()? as usize;
        let cache_source_state = match &scan.cache_source {
            CacheSource::HashJoinBuild((cache_index, column_indexes)) => {
                let hash_join_state = match self.hash_join_states.get(cache_index) {
                    Some(hash_join_state) => hash_join_state.clone(),
                    None => {
                        return Err(ErrorCode::Internal(
                            "Hash join state not found during building cache scan".to_string(),
                        ));
                    }
                };
                CacheSourceState::HashJoinCacheState(HashJoinCacheState::new(
                    column_indexes.clone(),
                    hash_join_state,
                    max_block_size,
                ))
            }
        };

        self.main_pipeline.add_source(
            |output| {
                TransformCacheScan::create(self.ctx.clone(), output, cache_source_state.clone())
            },
            max_threads as usize,
        )
    }

    pub(crate) fn build_expression_scan(&mut self, scan: &ExpressionScan) -> Result<()> {
        self.build_pipeline(&scan.input)?;

        let values = scan
            .values
            .iter()
            .map(|row| {
                row.iter()
                    .map(|scalar| scalar.as_expr(&BUILTIN_FUNCTIONS))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        let fun_ctx = self.func_ctx.clone();

        self.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(TransformExpressionScan::create(
                input,
                output,
                values.clone(),
                fun_ctx.clone(),
            )))
        })?;

        Ok(())
    }
}
