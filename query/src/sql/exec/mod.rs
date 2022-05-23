// Copyright 2021 Datafuse Labs.
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

mod data_schema_builder;
mod expression_builder;
mod util;

use std::sync::Arc;

use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::find_aggregate_exprs;
use common_planners::find_aggregate_exprs_in_expr;
use common_planners::Expression;
use common_planners::RewriteHelper;
pub use util::decode_field_name;
pub use util::format_field_name;

use super::plans::RelOperator;
use super::MetadataRef;
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::AggregatorParams;
use crate::pipelines::new::processors::AggregatorTransformParams;
use crate::pipelines::new::processors::ChainingHashTable;
use crate::pipelines::new::processors::ExpressionTransform;
use crate::pipelines::new::processors::HashJoinState;
use crate::pipelines::new::processors::ProjectionTransform;
use crate::pipelines::new::processors::SinkBuildHashTable;
use crate::pipelines::new::processors::Sinker;
use crate::pipelines::new::processors::SortMergeCompactor;
use crate::pipelines::new::processors::TransformAggregator;
use crate::pipelines::new::processors::TransformFilter;
use crate::pipelines::new::processors::TransformHashJoinProbe;
use crate::pipelines::new::processors::TransformLimit;
use crate::pipelines::new::processors::TransformSortMerge;
use crate::pipelines::new::processors::TransformSortPartial;
use crate::pipelines::new::NewPipeline;
use crate::pipelines::new::SinkPipeBuilder;
use crate::pipelines::transforms::get_sort_descriptions;
use crate::sessions::QueryContext;
use crate::sql::exec::data_schema_builder::DataSchemaBuilder;
use crate::sql::exec::expression_builder::ExpressionBuilder;
use crate::sql::exec::util::check_physical;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::AggregatePlan;
use crate::sql::plans::AndExpr;
use crate::sql::plans::EvalScalar;
use crate::sql::plans::FilterPlan;
use crate::sql::plans::LimitPlan;
use crate::sql::plans::PhysicalHashJoin;
use crate::sql::plans::PhysicalScan;
use crate::sql::plans::Project;
use crate::sql::plans::ScalarExpr;
use crate::sql::plans::SortPlan;
use crate::sql::IndexType;

/// Helper to build a `Pipeline` from `SExpr`
pub struct PipelineBuilder {
    ctx: Arc<QueryContext>,
    metadata: MetadataRef,
    result_columns: Vec<(IndexType, String)>,
    expression: SExpr,
    pipelines: Vec<NewPipeline>,
    limit: Option<usize>,
    offset: usize,
}

impl PipelineBuilder {
    pub fn new(
        ctx: Arc<QueryContext>,
        result_columns: Vec<(IndexType, String)>,
        metadata: MetadataRef,
        expression: SExpr,
    ) -> Self {
        PipelineBuilder {
            ctx,
            metadata,
            result_columns,
            expression,
            pipelines: vec![],
            limit: None,
            offset: 0,
        }
    }

    fn get_field_name(&self, column_index: IndexType) -> String {
        let name = &self.metadata.read().column(column_index).name.clone();
        format_field_name(name.as_str(), column_index)
    }

    pub fn spawn(mut self) -> Result<(NewPipeline, Vec<NewPipeline>)> {
        let expr = self.expression.clone();
        let mut pipeline = NewPipeline::create();
        let schema = self.build_pipeline(self.ctx.clone(), &expr, &mut pipeline)?;
        self.align_data_schema(schema, &mut pipeline)?;
        let settings = self.ctx.get_settings();
        pipeline.set_max_threads(settings.get_max_threads()? as usize);
        for pipeline in self.pipelines.iter_mut() {
            pipeline.set_max_threads(settings.get_max_threads()? as usize);
        }
        Ok((pipeline, self.pipelines))
    }

    fn align_data_schema(
        &mut self,
        input_schema: DataSchemaRef,
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        let mut projections = Vec::with_capacity(self.result_columns.len());
        let mut output_fields = Vec::with_capacity(self.result_columns.len());
        for (index, name) in self.result_columns.iter() {
            let column_entry = self.metadata.read().column(*index).clone();
            projections.push(Expression::Alias(
                name.clone(),
                Box::new(Expression::Column(self.get_field_name(*index))),
            ));
            let field = DataField::new(name.as_str(), column_entry.data_type.clone());
            output_fields.push(field);
        }
        let output_schema = Arc::new(DataSchema::new(output_fields));

        pipeline.add_transform(|transform_input_port, transform_output_port| {
            ProjectionTransform::try_create(
                transform_input_port,
                transform_output_port,
                input_schema.clone(),
                output_schema.clone(),
                projections.clone(),
                self.ctx.clone(),
            )
        })?;
        Ok(())
    }

    fn build_pipeline(
        &mut self,
        context: Arc<QueryContext>,
        expression: &SExpr,
        pipeline: &mut NewPipeline,
    ) -> Result<DataSchemaRef> {
        if !check_physical(expression) {
            return Err(ErrorCode::LogicalError("Invalid physical plan"));
        }

        let plan = expression.plan();

        match plan {
            RelOperator::PhysicalScan(physical_scan) => {
                self.build_physical_scan(physical_scan, pipeline)
            }
            RelOperator::Project(project) => {
                let input_schema =
                    self.build_pipeline(context, &expression.children()[0], pipeline)?;
                self.build_project(project, input_schema, pipeline)
            }
            RelOperator::EvalScalar(eval_scalar) => {
                let input_schema =
                    self.build_pipeline(context, &expression.children()[0], pipeline)?;
                self.build_eval_scalar(eval_scalar, input_schema, pipeline)
            }
            RelOperator::Filter(filter) => {
                let input_schema =
                    self.build_pipeline(context, &expression.children()[0], pipeline)?;
                self.build_filter(filter, input_schema, pipeline)
            }
            RelOperator::Aggregate(aggregate) => {
                let input_schema =
                    self.build_pipeline(context, &expression.children()[0], pipeline)?;
                self.build_aggregate(aggregate, input_schema, pipeline)
            }
            RelOperator::PhysicalHashJoin(hash_join) => {
                let probe_schema =
                    self.build_pipeline(context.clone(), &expression.children()[0], pipeline)?;
                let mut child_pipeline = NewPipeline::create();
                let build_schema = self.build_pipeline(
                    QueryContext::create_from(context),
                    &expression.children()[1],
                    &mut child_pipeline,
                )?;
                self.build_hash_join(
                    hash_join,
                    build_schema,
                    probe_schema,
                    child_pipeline,
                    pipeline,
                )
            }
            RelOperator::Sort(sort_plan) => {
                let input_schema =
                    self.build_pipeline(context, &expression.children()[0], pipeline)?;
                self.build_order_by(sort_plan, input_schema, pipeline)
            }
            RelOperator::Limit(limit_plan) => {
                let input_schema =
                    self.build_pipeline(context, &expression.children()[0], pipeline)?;
                self.build_limit(limit_plan, input_schema, pipeline)
            }
            _ => Err(ErrorCode::LogicalError("Invalid physical plan")),
        }
    }

    fn build_project(
        &mut self,
        project: &Project,
        input_schema: DataSchemaRef,
        pipeline: &mut NewPipeline,
    ) -> Result<DataSchemaRef> {
        let schema_builder = DataSchemaBuilder::new(self.metadata.clone());
        let output_schema = schema_builder.build_project(project)?;
        let mut expressions = Vec::with_capacity(project.columns.len());
        for index in project.columns.iter() {
            let expression = Expression::Column(self.get_field_name(*index));
            expressions.push(expression);
        }
        pipeline.add_transform(|transform_input_port, transform_output_port| {
            ProjectionTransform::try_create(
                transform_input_port,
                transform_output_port,
                input_schema.clone(),
                output_schema.clone(),
                expressions.clone(),
                self.ctx.clone(),
            )
        })?;

        Ok(output_schema)
    }
    fn build_eval_scalar(
        &mut self,
        eval_scalar: &EvalScalar,
        input_schema: DataSchemaRef,
        pipeline: &mut NewPipeline,
    ) -> Result<DataSchemaRef> {
        let schema_builder = DataSchemaBuilder::new(self.metadata.clone());
        let output_schema = schema_builder.build_eval_scalar(eval_scalar, input_schema.clone())?;
        let mut expressions = Vec::with_capacity(eval_scalar.items.len());
        let expr_builder = ExpressionBuilder::create(self.metadata.clone());
        for item in eval_scalar.items.iter() {
            let scalar = &item.scalar;
            let expression = expr_builder.build_and_rename(scalar, item.index)?;
            expressions.push(expression);
        }
        pipeline.add_transform(|transform_input_port, transform_output_port| {
            ProjectionTransform::try_create(
                transform_input_port,
                transform_output_port,
                input_schema.clone(),
                output_schema.clone(),
                expressions.clone(),
                self.ctx.clone(),
            )
        })?;

        Ok(output_schema)
    }

    fn build_filter(
        &mut self,
        filter: &FilterPlan,
        input_schema: DataSchemaRef,
        pipeline: &mut NewPipeline,
    ) -> Result<DataSchemaRef> {
        let output_schema = input_schema.clone();
        let eb = ExpressionBuilder::create(self.metadata.clone());
        let scalars = &filter.predicates;
        let pred = scalars.iter().cloned().reduce(|acc, v| {
            AndExpr {
                left: Box::new(acc),
                right: Box::new(v),
            }
            .into()
        });
        let mut pred = eb.build(&pred.unwrap())?;
        let no_agg_expression = find_aggregate_exprs_in_expr(&pred).is_empty();
        if !no_agg_expression && !filter.is_having {
            return Err(ErrorCode::SyntaxException(
                "WHERE clause cannot contain aggregate functions",
            ));
        }
        if !no_agg_expression && filter.is_having {
            pred = eb.normalize_aggr_to_col(pred.clone())?;
        }
        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformFilter::try_create(
                input_schema.clone(),
                pred.clone(),
                transform_input_port,
                transform_output_port,
                self.ctx.clone(),
            )
        })?;
        Ok(output_schema)
    }

    fn build_physical_scan(
        &mut self,
        scan: &PhysicalScan,
        pipeline: &mut NewPipeline,
    ) -> Result<DataSchemaRef> {
        let table_entry = self.metadata.read().table(scan.table_index).clone();
        let plan = table_entry.source;

        let table = self.ctx.build_table_from_source_plan(&plan)?;
        self.ctx.try_set_partitions(plan.parts.clone())?;
        table.read2(self.ctx.clone(), &plan, pipeline)?;
        let columns: Vec<IndexType> = scan.columns.iter().cloned().collect();
        let projections: Vec<Expression> = columns
            .iter()
            .map(|index| {
                let name = self.metadata.read().column(*index).name.clone();
                Expression::Alias(
                    self.get_field_name(*index),
                    Box::new(Expression::Column(name)),
                )
            })
            .collect();
        let schema_builder = DataSchemaBuilder::new(self.metadata.clone());
        let input_schema = schema_builder.build_canonical_schema(&columns);
        let output_schema = schema_builder.build_physical_scan(scan)?;

        pipeline.add_transform(|transform_input_port, transform_output_port| {
            ProjectionTransform::try_create(
                transform_input_port,
                transform_output_port,
                input_schema.clone(),
                output_schema.clone(),
                projections.clone(),
                self.ctx.clone(),
            )
        })?;

        Ok(output_schema)
    }

    fn build_aggregate(
        &mut self,
        aggregate: &AggregatePlan,
        input_schema: DataSchemaRef,
        pipeline: &mut NewPipeline,
    ) -> Result<DataSchemaRef> {
        let mut output_fields = vec![];
        let mut agg_expressions = Vec::with_capacity(aggregate.aggregate_functions.len());
        let expr_builder = ExpressionBuilder::create(self.metadata.clone());
        for item in aggregate.aggregate_functions.iter() {
            let expr = expr_builder.build(&item.scalar)?;
            agg_expressions.push(expr);
        }

        let mut group_expressions = Vec::with_capacity(aggregate.group_items.len());
        for item in aggregate.group_items.iter() {
            let expr = Expression::Column(self.get_field_name(item.index));
            group_expressions.push(expr);
        }

        // Reformat output of aggregator
        let mut rename_expressions = Vec::with_capacity(agg_expressions.capacity());
        for (i, item) in aggregate.group_items.iter().enumerate() {
            let expr = &group_expressions[i];
            let name = self.get_field_name(item.index);
            rename_expressions.push(Expression::Alias(
                name.clone(),
                Box::new(Expression::Column(expr.column_name())),
            ));
            output_fields.push(DataField::new(name.as_str(), item.scalar.data_type()));
        }

        for (i, item) in aggregate.aggregate_functions.iter().enumerate() {
            let expr = &agg_expressions[i];
            let name = self.get_field_name(item.index);
            rename_expressions.push(Expression::Alias(
                name.clone(),
                Box::new(Expression::Column(expr.column_name())),
            ));
            output_fields.push(DataField::new(name.as_str(), item.scalar.data_type()));
        }

        if !aggregate.from_distinct && !find_aggregate_exprs(&group_expressions).is_empty() {
            return Err(ErrorCode::SyntaxException(
                "Group by clause cannot contain aggregate functions",
            ));
        }

        // Get partial schema from agg_expressions
        let partial_data_fields =
            RewriteHelper::exprs_to_fields(agg_expressions.as_slice(), &input_schema)?;
        let partial_schema = DataSchemaRefExt::create(partial_data_fields);

        // Get final schema from agg_expression and group expression
        let mut final_exprs = agg_expressions.to_owned();
        final_exprs.extend_from_slice(group_expressions.as_slice());
        let final_data_fields =
            RewriteHelper::exprs_to_fields(final_exprs.as_slice(), &input_schema)?;
        let final_schema = DataSchemaRefExt::create(final_data_fields);

        let partial_aggr_params = AggregatorParams::try_create(
            &agg_expressions,
            &group_expressions,
            &input_schema,
            &partial_schema,
        )?;
        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformAggregator::try_create_partial(
                transform_input_port.clone(),
                transform_output_port.clone(),
                AggregatorTransformParams::try_create(
                    transform_input_port,
                    transform_output_port,
                    &partial_aggr_params,
                )?,
                self.ctx.clone(),
            )
        })?;

        pipeline.resize(1)?;
        let final_aggr_params = AggregatorParams::try_create(
            &agg_expressions,
            &group_expressions,
            &input_schema,
            &final_schema,
        )?;

        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformAggregator::try_create_final(
                transform_input_port.clone(),
                transform_output_port.clone(),
                AggregatorTransformParams::try_create(
                    transform_input_port,
                    transform_output_port,
                    &final_aggr_params,
                )?,
                self.ctx.clone(),
            )
        })?;

        let output_schema = DataSchemaRefExt::create(output_fields);

        pipeline.add_transform(|input, output| {
            ProjectionTransform::try_create(
                input,
                output,
                final_schema.clone(),
                output_schema.clone(),
                rename_expressions.clone(),
                self.ctx.clone(),
            )
        })?;

        Ok(output_schema)
    }

    fn build_hash_join(
        &mut self,
        hash_join: &PhysicalHashJoin,
        build_schema: DataSchemaRef,
        probe_schema: DataSchemaRef,
        mut child_pipeline: NewPipeline,
        pipeline: &mut NewPipeline,
    ) -> Result<DataSchemaRef> {
        let builder = DataSchemaBuilder::new(self.metadata.clone());
        let output_schema = builder.build_join(probe_schema.clone(), build_schema.clone());

        let eb = ExpressionBuilder::create(self.metadata.clone());
        let build_expressions = hash_join
            .build_keys
            .iter()
            .map(|scalar| eb.build(scalar))
            .collect::<Result<Vec<Expression>>>()?;
        let probe_expressions = hash_join
            .probe_keys
            .iter()
            .map(|scalar| eb.build(scalar))
            .collect::<Result<Vec<Expression>>>()?;

        let hash_join_state = Arc::new(ChainingHashTable::try_create(
            build_expressions,
            probe_expressions,
            build_schema,
            probe_schema,
            self.ctx.clone(),
        )?);

        // Build side
        self.build_sink_hash_table(hash_join_state.clone(), &mut child_pipeline)?;

        // Probe side
        pipeline.add_transform(|input, output| {
            Ok(TransformHashJoinProbe::create(
                self.ctx.clone(),
                input,
                output,
                hash_join_state.clone(),
                output_schema.clone(),
            ))
        })?;

        self.pipelines.push(child_pipeline);

        Ok(output_schema)
    }

    fn build_sink_hash_table(
        &mut self,
        state: Arc<dyn HashJoinState>,
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        let mut sink_pipeline_builder = SinkPipeBuilder::create();
        for _ in 0..pipeline.output_len() {
            let input_port = InputPort::create();
            sink_pipeline_builder.add_sink(
                input_port.clone(),
                Sinker::<SinkBuildHashTable>::create(
                    input_port,
                    SinkBuildHashTable::try_create(state.clone())?,
                ),
            );
        }

        pipeline.add_pipe(sink_pipeline_builder.finalize());
        Ok(())
    }

    fn build_order_by(
        &mut self,
        sort_plan: &SortPlan,
        input_schema: DataSchemaRef,
        pipeline: &mut NewPipeline,
    ) -> Result<DataSchemaRef> {
        let mut expressions = Vec::with_capacity(sort_plan.items.len());
        for item in sort_plan.items.iter() {
            let expr = Expression::Column(self.get_field_name(item.index));
            let asc = item.asc.unwrap_or(true);
            // NULLS FIRST is the default for DESC order, and NULLS LAST otherwise
            let nulls_first = item.nulls_first.unwrap_or(!asc);
            expressions.push(Expression::Sort {
                expr: Box::new(expr.clone()),
                asc,
                nulls_first,
                origin_expr: Box::new(expr),
            })
        }

        let schema_builder = DataSchemaBuilder::new(self.metadata.clone());
        let output_schema = schema_builder.build_sort(&input_schema, expressions.as_slice())?;

        pipeline.add_transform(|transform_input_port, transform_output_port| {
            ExpressionTransform::try_create(
                transform_input_port,
                transform_output_port,
                input_schema.clone(),
                output_schema.clone(),
                expressions.clone(),
                self.ctx.clone(),
            )
        })?;

        let rows_limit = self.limit.map(|limit| limit + self.offset);
        // processor 1: block ---> sort_stream
        // processor 2: block ---> sort_stream
        // processor 3: block ---> sort_stream
        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformSortPartial::try_create(
                transform_input_port,
                transform_output_port,
                rows_limit,
                get_sort_descriptions(&output_schema, expressions.as_slice())?,
            )
        })?;

        // processor 1: [sorted blocks ...] ---> merge to one sorted block
        // processor 2: [sorted blocks ...] ---> merge to one sorted block
        // processor 3: [sorted blocks ...] ---> merge to one sorted block
        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformSortMerge::try_create(
                transform_input_port,
                transform_output_port,
                SortMergeCompactor::new(
                    rows_limit,
                    get_sort_descriptions(&output_schema, expressions.as_slice())?,
                ),
            )
        })?;

        // processor1 sorted block --
        //                             \
        // processor2 sorted block ----> processor  --> merge to one sorted block
        //                             /
        // processor3 sorted block --
        pipeline.resize(1)?;
        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformSortMerge::try_create(
                transform_input_port,
                transform_output_port,
                SortMergeCompactor::new(
                    rows_limit,
                    get_sort_descriptions(&output_schema, expressions.as_slice())?,
                ),
            )
        })?;

        Ok(output_schema)
    }

    fn build_limit(
        &mut self,
        limit_plan: &LimitPlan,
        input_schema: DataSchemaRef,
        pipeline: &mut NewPipeline,
    ) -> Result<DataSchemaRef> {
        self.limit = limit_plan.limit;
        self.offset = limit_plan.offset;
        pipeline.resize(1)?;

        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformLimit::try_create(
                limit_plan.limit,
                limit_plan.offset,
                transform_input_port,
                transform_output_port,
            )
        })?;

        Ok(input_schema)
    }
}
