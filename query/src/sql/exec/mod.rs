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
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::find_aggregate_exprs;
use common_planners::find_aggregate_exprs_in_expr;
use common_planners::Expression;
use common_planners::RewriteHelper;
pub use util::decode_field_name;
pub use util::format_field_name;

use super::plans::BasePlan;
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::AggregatorParams;
use crate::pipelines::new::processors::AggregatorTransformParams;
use crate::pipelines::new::processors::ChainingHashTable;
use crate::pipelines::new::processors::ExpressionTransform;
use crate::pipelines::new::processors::HashJoinState;
use crate::pipelines::new::processors::ProjectionTransform;
use crate::pipelines::new::processors::SinkBuildHashTable;
use crate::pipelines::new::processors::Sinker;
use crate::pipelines::new::processors::TransformAggregator;
use crate::pipelines::new::processors::TransformFilter;
use crate::pipelines::new::processors::TransformHashJoinProbe;
use crate::pipelines::new::NewPipeline;
use crate::pipelines::new::SinkPipeBuilder;
use crate::sessions::QueryContext;
use crate::sql::exec::data_schema_builder::DataSchemaBuilder;
use crate::sql::exec::expression_builder::ExpressionBuilder;
use crate::sql::exec::util::check_physical;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::AggregatePlan;
use crate::sql::plans::AndExpr;
use crate::sql::plans::FilterPlan;
use crate::sql::plans::PhysicalHashJoin;
use crate::sql::plans::PhysicalScan;
use crate::sql::plans::PlanType;
use crate::sql::plans::ProjectPlan;
use crate::sql::IndexType;
use crate::sql::Metadata;

/// Helper to build a `Pipeline` from `SExpr`
pub struct PipelineBuilder {
    ctx: Arc<QueryContext>,
    metadata: Metadata,
    result_columns: Vec<(IndexType, String)>,
    expression: SExpr,

    pipelines: Vec<NewPipeline>,
}

impl PipelineBuilder {
    pub fn new(
        ctx: Arc<QueryContext>,
        result_columns: Vec<(IndexType, String)>,
        metadata: Metadata,
        expression: SExpr,
    ) -> Self {
        PipelineBuilder {
            ctx,
            metadata,
            result_columns,
            expression,

            pipelines: vec![],
        }
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
            let column_entry = self.metadata.column(*index);
            let field_name = &column_entry.name;
            projections.push(Expression::Alias(
                name.clone(),
                Box::new(Expression::Column(format_field_name(
                    field_name.as_str(),
                    *index,
                ))),
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

        match plan.plan_type() {
            PlanType::PhysicalScan => {
                let physical_scan: PhysicalScan = plan.try_into()?;
                self.build_physical_scan(&physical_scan, pipeline)
            }
            PlanType::Project => {
                let project: ProjectPlan = plan.try_into()?;
                let input_schema =
                    self.build_pipeline(context, &expression.children()[0], pipeline)?;
                self.build_project(&project, input_schema, pipeline)
            }
            PlanType::Filter => {
                let filter: FilterPlan = plan.try_into()?;
                let input_schema =
                    self.build_pipeline(context, &expression.children()[0], pipeline)?;
                self.build_filter(&filter, input_schema, pipeline)
            }
            PlanType::Aggregate => {
                let aggregate: AggregatePlan = plan.try_into()?;
                let input_schema =
                    self.build_pipeline(context, &expression.children()[0], pipeline)?;
                self.build_aggregate(&aggregate, input_schema, pipeline)
            }
            PlanType::PhysicalHashJoin => {
                let hash_join: PhysicalHashJoin = plan.try_into()?;
                let probe_schema =
                    self.build_pipeline(context.clone(), &expression.children()[0], pipeline)?;
                let mut child_pipeline = NewPipeline::create();
                let build_schema = self.build_pipeline(
                    QueryContext::create_from(context),
                    &expression.children()[1],
                    &mut child_pipeline,
                )?;
                self.build_hash_join(
                    &hash_join,
                    build_schema,
                    probe_schema,
                    child_pipeline,
                    pipeline,
                )
            }
            _ => Err(ErrorCode::LogicalError("Invalid physical plan")),
        }
    }

    fn build_project(
        &mut self,
        project: &ProjectPlan,
        input_schema: DataSchemaRef,
        pipeline: &mut NewPipeline,
    ) -> Result<DataSchemaRef> {
        let schema_builder = DataSchemaBuilder::new(&self.metadata);
        let output_schema = schema_builder.build_project(project, input_schema.clone())?;
        let mut expressions = Vec::with_capacity(project.items.len());
        let expr_builder = ExpressionBuilder::create(&self.metadata);
        for item in project.items.iter() {
            let scalar = &item.expr;
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
        let eb = ExpressionBuilder::create(&self.metadata);
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
        let table_entry = self.metadata.table(scan.table_index);
        let plan = table_entry.source.clone();

        let table = self.ctx.build_table_from_source_plan(&plan)?;
        self.ctx.try_set_partitions(plan.parts.clone())?;
        table.read2(self.ctx.clone(), &plan, pipeline)?;
        let columns: Vec<IndexType> = scan.columns.iter().cloned().collect();
        let projections: Vec<Expression> = columns
            .iter()
            .map(|index| {
                let column_entry = self.metadata.column(*index);
                Expression::Alias(
                    format_field_name(column_entry.name.as_str(), column_entry.column_index),
                    Box::new(Expression::Column(column_entry.name.clone())),
                )
            })
            .collect();
        let schema_builder = DataSchemaBuilder::new(&self.metadata);
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
        let mut agg_expressions = Vec::with_capacity(aggregate.agg_expr.len());
        let expr_builder = ExpressionBuilder::create(&self.metadata);
        for scalar in aggregate.agg_expr.iter() {
            let expr = expr_builder.build(scalar)?;
            agg_expressions.push(expr);
        }

        let mut group_expressions = Vec::with_capacity(aggregate.group_expr.len());
        for scalar in aggregate.group_expr.iter() {
            let expr = expr_builder.build(scalar)?;
            group_expressions.push(expr);
        }

        if !find_aggregate_exprs(&group_expressions).is_empty() {
            return Err(ErrorCode::SyntaxException(
                "Group by clause cannot contain aggregate functions",
            ));
        }

        // Process group by with non-column expression, such as `a+1`
        // TODO(xudong963): move to aggregate transform
        let schema_builder = DataSchemaBuilder::new(&self.metadata);
        let pre_input_schema = input_schema.clone();
        let input_schema =
            schema_builder.build_group_by(input_schema, group_expressions.as_slice())?;
        pipeline.add_transform(|transform_input_port, transform_output_port| {
            ExpressionTransform::try_create(
                transform_input_port,
                transform_output_port,
                pre_input_schema.clone(),
                input_schema.clone(),
                group_expressions.clone(),
                self.ctx.clone(),
            )
        })?;

        // Process aggregation function with non-column expression, such as sum(3)
        let pre_input_schema = input_schema.clone();
        let res =
            schema_builder.build_agg_func(pre_input_schema.clone(), agg_expressions.as_slice())?;
        let input_schema = res.0;
        pipeline.add_transform(|transform_input_port, transform_output_port| {
            ExpressionTransform::try_create(
                transform_input_port,
                transform_output_port,
                pre_input_schema.clone(),
                input_schema.clone(),
                res.1.clone(),
                self.ctx.clone(),
            )
        })?;

        // Get partial schema from agg_expressions
        let partial_data_fields =
            RewriteHelper::exprs_to_fields(agg_expressions.as_slice(), &input_schema)?;
        let partial_schema = schema_builder.build_aggregate(partial_data_fields, &input_schema)?;

        // Get final schema from agg_expression and group expression
        let mut final_exprs = agg_expressions.to_owned();
        final_exprs.extend_from_slice(group_expressions.as_slice());
        let final_data_fields =
            RewriteHelper::exprs_to_fields(final_exprs.as_slice(), &input_schema)?;
        let final_schema = schema_builder.build_aggregate(final_data_fields, &input_schema)?;

        let partial_aggr_params = AggregatorParams::try_create_v2(
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
        let final_aggr_params = AggregatorParams::try_create_v2(
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

        Ok(final_schema)
    }

    fn build_hash_join(
        &mut self,
        hash_join: &PhysicalHashJoin,
        build_schema: DataSchemaRef,
        probe_schema: DataSchemaRef,
        mut child_pipeline: NewPipeline,
        pipeline: &mut NewPipeline,
    ) -> Result<DataSchemaRef> {
        let builder = DataSchemaBuilder::new(&self.metadata);
        let output_schema = builder.build_join(probe_schema.clone(), build_schema.clone());

        let eb = ExpressionBuilder::create(&self.metadata);
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
}
