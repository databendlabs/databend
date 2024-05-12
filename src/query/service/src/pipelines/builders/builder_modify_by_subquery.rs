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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::DataType;
use databend_common_expression::FilterExecutor;
use databend_common_expression::SelectExprBuilder;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::processors::AsyncAccumulatingTransformer;
use databend_common_sql::executor::physical_plans::ModifyBySubquery;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_storages_factory::Table;
use databend_common_storages_fuse::operations::MutationGenerator;
use databend_common_storages_fuse::operations::TableMutationAggregator;
use databend_common_storages_fuse::operations::TransformAccumulateSegment;
use databend_common_storages_fuse::operations::TransformMutationSubquery;
use databend_common_storages_fuse::operations::TransformSerializeBlock;
use databend_common_storages_fuse::operations::TransformSerializeSegment;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;

use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_modify_by_subquery(
        &mut self,
        modify_by_subquery: &ModifyBySubquery,
    ) -> Result<()> {
        let input = modify_by_subquery.input.as_ref();
        self.build_pipeline(input)?;

        let ctx = self.ctx.clone();

        let table = ctx.build_table_by_table_info(
            &modify_by_subquery.catalog_info,
            &modify_by_subquery.table_info,
            None,
        )?;
        let table = FuseTable::try_from_table(table.as_ref())?;

        let filter_executor =
            if let PhysicalPlan::Filter(filter) = modify_by_subquery.filter.as_ref() {
                let predicate = filter
                    .predicates
                    .iter()
                    .map(|expr| expr.as_expr(&BUILTIN_FUNCTIONS))
                    .try_reduce(|lhs, rhs| {
                        check_function(None, "and_filters", &[], &[lhs, rhs], &BUILTIN_FUNCTIONS)
                    })
                    .transpose()
                    .unwrap_or_else(|| {
                        Err(ErrorCode::Internal(
                            "Invalid empty predicate list".to_string(),
                        ))
                    })?;
                assert_eq!(predicate.data_type(), &DataType::Boolean);
                let mut builder = SelectExprBuilder::new();
                let (select_expr, has_or) = builder.build(&predicate).into();
                let max_block_size = ctx.get_settings().get_max_block_size()? as usize;

                FilterExecutor::new(
                    select_expr,
                    self.func_ctx.clone(),
                    has_or,
                    max_block_size,
                    None,
                    &BUILTIN_FUNCTIONS,
                    true,
                )
            } else {
                unreachable!()
            };

        // 1: add TransformMutationSubquery
        self.main_pipeline.add_transform(|input, output| {
            TransformMutationSubquery::try_create(
                ctx.get_function_context()?,
                input,
                output,
                modify_by_subquery.typ.clone().into(),
                filter_executor.clone(),
                table.schema().num_fields(),
            )?
            .into_processor()
        })?;

        // 2: add TransformSerializeBlock
        let block_thresholds = table.get_block_thresholds();
        let cluster_stats_gen =
            table.get_cluster_stats_gen(ctx.clone(), 0, block_thresholds, None)?;
        self.main_pipeline.add_transform(|input, output| {
            let proc = TransformSerializeBlock::try_create(
                ctx.clone(),
                input,
                output,
                table,
                cluster_stats_gen.clone(),
                MutationKind::Replace,
            )?;
            proc.into_processor()
        })?;

        // 3: add TransformSerializeSegment
        self.main_pipeline.add_transform(|input, output| {
            let proc =
                TransformSerializeSegment::new(ctx.clone(), input, output, table, block_thresholds);
            proc.into_processor()
        })?;

        // 4: add TableMutationAggregator
        self.main_pipeline.add_transform(|input, output| {
            let aggregator =
                TableMutationAggregator::new(table, ctx.clone(), vec![], MutationKind::Replace);
            Ok(ProcessorPtr::create(AsyncAccumulatingTransformer::create(
                input, output, aggregator,
            )))
        })?;

        self.main_pipeline.resize(1, true)?;
        self.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(Box::new(
                TransformAccumulateSegment::new(input, output, table.get_id()),
            )))
        })?;

        // 5: add CommitSink
        let snapshot_gen = MutationGenerator::new(
            modify_by_subquery.snapshot.clone(),
            MutationKind::ReplaceBySubquery,
        );
        let lock = None;
        self.main_pipeline.add_sink(|input| {
            databend_common_storages_fuse::operations::CommitSink::try_create(
                table,
                ctx.clone(),
                None,
                vec![],
                snapshot_gen.clone(),
                input,
                None,
                lock.clone(),
                None,
                None,
            )
        })?;

        Ok(())
    }
}
