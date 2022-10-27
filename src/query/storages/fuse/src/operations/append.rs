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

use common_catalog::table_context::TableContext;
use common_datablocks::SortColumnDescription;
use common_datavalues::DataField;
use common_exception::Result;
use common_pipeline_core::Pipeline;
use common_pipeline_transforms::processors::transforms::TransformCompact;
use common_pipeline_transforms::processors::transforms::TransformSortPartial;
use common_sql::evaluator::ChunkOperator;
use common_sql::evaluator::CompoundChunkOperator;
use common_sql::evaluator::Evaluator;

use crate::io::BlockCompactor;
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
        need_output: bool,
    ) -> Result<()> {
        let block_per_seg =
            self.get_option(FUSE_OPT_KEY_BLOCK_PER_SEGMENT, DEFAULT_BLOCK_PER_SEGMENT);

        let block_compactor = self.get_block_compactor();
        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformCompact::try_create(
                transform_input_port,
                transform_output_port,
                block_compactor.to_compactor(false),
            )
        })?;

        let cluster_stats_gen =
            self.get_cluster_stats_gen(ctx.clone(), pipeline, 0, block_compactor)?;

        if !self.cluster_keys.is_empty() {
            // sort
            let sort_descs: Vec<SortColumnDescription> = self
                .cluster_keys
                .iter()
                .map(|expr| SortColumnDescription {
                    // todo(sundy): use index instead
                    column_name: expr.pretty_display(),
                    asc: true,
                    nulls_first: false,
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
        block_compactor: BlockCompactor,
    ) -> Result<ClusterStatsGenerator> {
        if self.cluster_keys.is_empty() {
            return Ok(ClusterStatsGenerator::default());
        }

        let input_schema = self.table_info.schema();
        let mut merged = input_schema.fields().clone();

        let mut cluster_key_index = Vec::with_capacity(self.cluster_keys.len());
        let mut extra_key_index = Vec::with_capacity(self.cluster_keys.len());

        let mut operators = Vec::with_capacity(self.cluster_keys.len());

        for expr in &self.cluster_keys {
            let cname = expr.pretty_display();

            let index = match merged.iter().position(|x| x.name() == &cname) {
                None => {
                    let field = DataField::new(&cname, expr.data_type());
                    operators.push(ChunkOperator::Map {
                        eval: Evaluator::eval_physical_scalar(expr)?,
                        name: field.name().to_string(),
                    });
                    extra_key_index.push(merged.len() - 1);

                    merged.push(field);
                    merged.len() - 1
                }
                Some(idx) => idx,
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
            block_compactor,
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
