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

use std::str::FromStr;
use std::sync::Arc;

use common_catalog::table::AppendMode;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::BlockThresholds;
use common_expression::DataField;
use common_expression::Expr;
use common_expression::SortColumnDescription;
use common_functions::BUILTIN_FUNCTIONS;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::Pipeline;
use common_pipeline_transforms::processors::transforms::transform_block_compact_for_copy::BlockCompactorForCopy;
use common_pipeline_transforms::processors::transforms::BlockCompactor;
use common_pipeline_transforms::processors::transforms::TransformCompact;
use common_pipeline_transforms::processors::transforms::TransformSortPartial;
use common_sql::evaluator::BlockOperator;
use common_sql::evaluator::CompoundBlockOperator;

use crate::operations::common::TransformSerializeBlock;
use crate::statistics::ClusterStatsGenerator;
use crate::FuseTable;

impl FuseTable {
    pub fn do_append_data(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        append_mode: AppendMode,
    ) -> Result<()> {
        let block_thresholds = self.get_block_thresholds();

        match append_mode {
            AppendMode::Normal => {
                pipeline.add_transform(|transform_input_port, transform_output_port| {
                    Ok(ProcessorPtr::create(TransformCompact::try_create(
                        transform_input_port,
                        transform_output_port,
                        BlockCompactor::new(block_thresholds),
                    )?))
                })?;
            }
            AppendMode::Copy => {
                pipeline.try_resize(1)?;
                pipeline.add_transform(|transform_input_port, transform_output_port| {
                    Ok(ProcessorPtr::create(TransformCompact::try_create(
                        transform_input_port,
                        transform_output_port,
                        BlockCompactorForCopy::new(block_thresholds),
                    )?))
                })?;
                pipeline.try_resize(ctx.get_settings().get_max_threads()? as usize)?;
            }
        }

        let cluster_stats_gen =
            self.cluster_gen_for_append(ctx.clone(), pipeline, block_thresholds)?;
        pipeline.add_transform(|input, output| {
            let proc = TransformSerializeBlock::try_create(
                ctx.clone(),
                input,
                output,
                self,
                cluster_stats_gen.clone(),
            )?;
            proc.into_processor()
        })?;

        Ok(())
    }

    pub fn cluster_gen_for_append(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        block_thresholds: BlockThresholds,
    ) -> Result<ClusterStatsGenerator> {
        let cluster_stats_gen = self.get_cluster_stats_gen(ctx.clone(), 0, block_thresholds)?;

        let operators = cluster_stats_gen.operators.clone();
        if !operators.is_empty() {
            let num_input_columns = self.table_info.schema().fields().len();
            let func_ctx2 = cluster_stats_gen.func_ctx.clone();
            pipeline.add_transform(move |input, output| {
                Ok(ProcessorPtr::create(CompoundBlockOperator::create(
                    input,
                    output,
                    num_input_columns,
                    func_ctx2.clone(),
                    operators.clone(),
                )))
            })?;
        }

        let cluster_keys = &cluster_stats_gen.cluster_key_index;
        if !cluster_keys.is_empty() {
            let sort_descs: Vec<SortColumnDescription> = cluster_keys
                .iter()
                .map(|index| SortColumnDescription {
                    offset: *index,
                    asc: true,
                    nulls_first: false,
                    is_nullable: false, // This information is not needed here.
                })
                .collect();

            pipeline.add_transform(|transform_input_port, transform_output_port| {
                Ok(ProcessorPtr::create(TransformSortPartial::try_create(
                    transform_input_port,
                    transform_output_port,
                    None,
                    sort_descs.clone(),
                )?))
            })?;
        }
        Ok(cluster_stats_gen)
    }

    pub fn get_cluster_stats_gen(
        &self,
        ctx: Arc<dyn TableContext>,
        level: i32,
        block_thresholds: BlockThresholds,
    ) -> Result<ClusterStatsGenerator> {
        let cluster_keys = self.cluster_keys(ctx.clone());
        if cluster_keys.is_empty() {
            return Ok(ClusterStatsGenerator::default());
        }

        let input_schema = self.table_info.schema();
        let mut merged: Vec<DataField> =
            input_schema.fields().iter().map(DataField::from).collect();

        let mut cluster_key_index = Vec::with_capacity(cluster_keys.len());
        let mut extra_key_num = 0;

        let mut exprs = Vec::with_capacity(cluster_keys.len());

        for remote_expr in &cluster_keys {
            let expr: Expr = remote_expr
                .as_expr(&BUILTIN_FUNCTIONS)
                .project_column_ref(|name| input_schema.index_of(name).unwrap());
            let index = match &expr {
                Expr::ColumnRef { id, .. } => *id,
                _ => {
                    let cname = format!("{}", expr);
                    merged.push(DataField::new(cname.as_str(), expr.data_type().clone()));
                    exprs.push(expr);

                    let offset = merged.len() - 1;
                    extra_key_num += 1;
                    offset
                }
            };
            cluster_key_index.push(index);
        }

        let operators = if exprs.is_empty() {
            vec![]
        } else {
            vec![BlockOperator::Map { exprs }]
        };

        Ok(ClusterStatsGenerator::new(
            self.cluster_key_meta.as_ref().unwrap().0,
            cluster_key_index,
            extra_key_num,
            self.get_max_page_size(),
            level,
            block_thresholds,
            operators,
            merged,
            ctx.get_function_context()?,
        ))
    }

    pub fn get_option<T: FromStr>(&self, opt_key: &str, default: T) -> T {
        self.table_info
            .options()
            .get(opt_key)
            .and_then(|s| s.parse::<T>().ok())
            .unwrap_or(default)
    }
}
