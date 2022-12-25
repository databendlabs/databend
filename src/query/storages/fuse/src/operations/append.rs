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

use std::str::FromStr;
use std::sync::Arc;

use common_catalog::table::AppendMode;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::ChunkCompactThresholds;
use common_expression::DataField;
use common_expression::Expr;
use common_expression::SortColumnDescription;
use common_functions_v2::scalars::BUILTIN_FUNCTIONS;
use common_pipeline_core::Pipeline;
use common_pipeline_transforms::processors::transforms::transform_chunk_compact_no_split::ChunkCompactorNoSplit;
use common_pipeline_transforms::processors::transforms::ChunkCompactor;
use common_pipeline_transforms::processors::transforms::TransformCompact;
use common_pipeline_transforms::processors::transforms::TransformSortPartial;
use common_sql::evaluator::ChunkOperator;
use common_sql::evaluator::CompoundChunkOperator;

use crate::operations::FuseTableSink;
use crate::statistics::ClusterStatsGenerator;
use crate::FuseTable;
use crate::DEFAULT_BLOCK_PER_SEGMENT;
use crate::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;

impl FuseTable {
    pub fn do_append_data(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        append_mode: AppendMode,
        need_output: bool,
    ) -> Result<()> {
        let block_per_seg =
            self.get_option(FUSE_OPT_KEY_BLOCK_PER_SEGMENT, DEFAULT_BLOCK_PER_SEGMENT);

        let block_compact_thresholds = self.get_chunk_compact_thresholds();
        match append_mode {
            AppendMode::Normal => {
                pipeline.add_transform(|transform_input_port, transform_output_port| {
                    TransformCompact::try_create(
                        transform_input_port,
                        transform_output_port,
                        ChunkCompactor::new(block_compact_thresholds, true),
                    )
                })?;
            }
            AppendMode::Copy => {
                let size = pipeline.output_len();
                pipeline.resize(1)?;
                pipeline.add_transform(|transform_input_port, transform_output_port| {
                    TransformCompact::try_create(
                        transform_input_port,
                        transform_output_port,
                        ChunkCompactorNoSplit::new(block_compact_thresholds),
                    )
                })?;
                pipeline.resize(size)?;
            }
        }

        let cluster_stats_gen =
            self.get_cluster_stats_gen(ctx.clone(), pipeline, 0, block_compact_thresholds)?;

        let cluster_keys = self.cluster_keys(ctx.clone());
        if !cluster_keys.is_empty() {
            let schema = self.table_info.schema();
            // sort
            let sort_descs: Vec<SortColumnDescription> = self
                .cluster_keys(ctx.clone())
                .iter()
                .map(|remote_expr| {
                    let expr = remote_expr
                        .into_expr(&BUILTIN_FUNCTIONS)
                        .unwrap()
                        .project_column_ref(|name| schema.index_of(name).unwrap());
                    let index = match expr {
                        Expr::ColumnRef { id, .. } => id,
                        _ => unreachable!("invalid expr"),
                    };
                    SortColumnDescription {
                        index,
                        asc: true,
                        nulls_first: false,
                    }
                })
                .collect();

            pipeline.add_transform(|transform_input_port, transform_output_port| {
                TransformSortPartial::try_create(
                    transform_input_port,
                    transform_output_port,
                    None,
                    sort_descs.clone(),
                )
            })?;
        }

        if need_output {
            pipeline.add_transform(|transform_input_port, transform_output_port| {
                FuseTableSink::try_create(
                    transform_input_port,
                    ctx.clone(),
                    block_per_seg,
                    self.operator.clone(),
                    self.meta_location_generator().clone(),
                    cluster_stats_gen.clone(),
                    block_compact_thresholds,
                    self.table_info.schema(),
                    self.storage_format,
                    Some(transform_output_port),
                )
            })?;
        } else {
            pipeline.add_sink(|input| {
                FuseTableSink::try_create(
                    input,
                    ctx.clone(),
                    block_per_seg,
                    self.operator.clone(),
                    self.meta_location_generator().clone(),
                    cluster_stats_gen.clone(),
                    block_compact_thresholds,
                    self.table_info.schema(),
                    self.storage_format,
                    None,
                )
            })?;
        }
        Ok(())
    }

    pub fn get_cluster_stats_gen(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        level: i32,
        chunk_compactor: ChunkCompactThresholds,
    ) -> Result<ClusterStatsGenerator> {
        let cluster_keys = self.cluster_keys(ctx.clone());
        if cluster_keys.is_empty() {
            return Ok(ClusterStatsGenerator::default());
        }

        let input_schema = self.table_info.schema();
        let mut merged: Vec<DataField> = input_schema
            .fields()
            .iter()
            .map(|f| DataField::from(f))
            .collect();

        let mut cluster_key_index = Vec::with_capacity(cluster_keys.len());
        let mut extra_key_index = Vec::with_capacity(cluster_keys.len());
        let mut operators = Vec::with_capacity(cluster_keys.len());

        for remote_expr in &cluster_keys {
            let expr: Expr = remote_expr
                .into_expr(&BUILTIN_FUNCTIONS)
                .unwrap()
                .project_column_ref(|name| input_schema.index_of(name).unwrap());
            let index = match &expr {
                Expr::ColumnRef { id, .. } => *id,
                _ => {
                    let cname = format!("{}", expr);

                    merged.push(DataField::new(cname.as_str(), expr.data_type().clone()));
                    let index = merged.len() - 1;
                    extra_key_index.push(index);

                    operators.push(ChunkOperator::Map { index, expr });
                    index
                }
            };
            cluster_key_index.push(index);
        }

        if !operators.is_empty() {
            let func_ctx = ctx.try_get_function_context()?;
            pipeline.add_transform(move |input, output| {
                Ok(CompoundChunkOperator::create(
                    input,
                    output,
                    func_ctx.clone(),
                    operators.clone(),
                ))
            })?;
        }

        Ok(ClusterStatsGenerator::new(
            self.cluster_key_meta.as_ref().unwrap().0,
            cluster_key_index,
            extra_key_index,
            level,
            chunk_compactor,
            vec![],
            merged,
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
