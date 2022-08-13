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
use common_datavalues::DataSchemaRefExt;
use common_exception::Result;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::Pipeline;
use common_pipeline_core::SinkPipeBuilder;
use common_pipeline_transforms::processors::transforms::transform_compact::TransformCompact;
use common_pipeline_transforms::processors::transforms::BlockCompactor;
use common_pipeline_transforms::processors::transforms::ExpressionTransform;
use common_pipeline_transforms::processors::transforms::TransformSortPartial;
use common_pipeline_transforms::processors::ExpressionExecutor;
use common_planners::Expression;

use crate::operations::FuseTableSink;
use crate::statistics::ClusterStatsGenerator;
use crate::FuseTable;
use crate::DEFAULT_BLOCK_PER_SEGMENT;
use crate::DEFAULT_BLOCK_SIZE_IN_MEM_SIZE_THRESHOLD;
use crate::DEFAULT_ROW_PER_BLOCK;
use crate::FUSE_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD;
use crate::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;
use crate::FUSE_OPT_KEY_ROW_PER_BLOCK;

impl FuseTable {
    pub fn do_append2(&self, ctx: Arc<dyn TableContext>, pipeline: &mut Pipeline) -> Result<()> {
        let max_row_per_block = self.get_option(FUSE_OPT_KEY_ROW_PER_BLOCK, DEFAULT_ROW_PER_BLOCK);
        let min_rows_per_block = (max_row_per_block as f64 * 0.8) as usize;
        let max_bytes_per_block = self.get_option(
            FUSE_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD,
            DEFAULT_BLOCK_SIZE_IN_MEM_SIZE_THRESHOLD,
        );

        let block_per_seg =
            self.get_option(FUSE_OPT_KEY_BLOCK_PER_SEGMENT, DEFAULT_BLOCK_PER_SEGMENT);

        let da = ctx.get_storage_operator()?;

        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformCompact::try_create(
                transform_input_port,
                transform_output_port,
                BlockCompactor::new(max_row_per_block, min_rows_per_block, max_bytes_per_block),
            )
        })?;

        let cluster_stats_gen = self.get_cluster_stats_gen(ctx.clone(), pipeline)?;
        if !self.cluster_keys.is_empty() {
            // sort
            let sort_descs: Vec<SortColumnDescription> = self
                .cluster_keys
                .iter()
                .map(|expr| SortColumnDescription {
                    column_name: expr.column_name(),
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

        let mut sink_pipeline_builder = SinkPipeBuilder::create();
        for _ in 0..pipeline.output_len() {
            let input_port = InputPort::create();
            sink_pipeline_builder.add_sink(
                input_port.clone(),
                FuseTableSink::try_create(
                    input_port,
                    ctx.clone(),
                    block_per_seg,
                    da.clone(),
                    self.meta_location_generator().clone(),
                    cluster_stats_gen.clone(),
                )?,
            );
        }

        pipeline.add_pipe(sink_pipeline_builder.finalize());
        Ok(())
    }

    pub fn get_cluster_stats_gen(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
    ) -> Result<ClusterStatsGenerator> {
        if self.cluster_keys.is_empty() {
            return Ok(ClusterStatsGenerator::default());
        }

        let input_schema = self.table_info.schema();
        let mut merged = input_schema.fields().clone();

        let mut cluster_key_index = Vec::with_capacity(self.cluster_keys.len());
        for expr in &self.cluster_keys {
            let cname = expr.column_name();
            let index = match merged.iter().position(|x| x.name() == &cname) {
                None => {
                    merged.push(expr.to_data_field(&input_schema)?);
                    merged.len() - 1
                }
                Some(idx) => idx,
            };
            cluster_key_index.push(index);
        }

        let output_schema = DataSchemaRefExt::create(merged);

        let mut expression_executor = None;
        if output_schema != input_schema {
            pipeline.add_transform(|transform_input_port, transform_output_port| {
                ExpressionTransform::try_create(
                    transform_input_port,
                    transform_output_port,
                    input_schema.clone(),
                    output_schema.clone(),
                    self.cluster_keys.clone(),
                    ctx.clone(),
                )
            })?;

            let exprs: Vec<Expression> = output_schema
                .fields()
                .iter()
                .map(|f| Expression::Column(f.name().to_owned()))
                .collect();

            let executor = ExpressionExecutor::try_create(
                ctx.clone(),
                "remove unused columns",
                output_schema.clone(),
                input_schema.clone(),
                exprs,
                true,
            )?;
            executor.validate()?;
            expression_executor = Some(executor);
        }

        Ok(ClusterStatsGenerator::new(
            self.cluster_key_meta.as_ref().unwrap().0,
            cluster_key_index,
            expression_executor,
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
