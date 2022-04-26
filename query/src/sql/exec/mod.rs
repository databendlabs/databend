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
use common_planners::Expression;
use common_planners::RewriteHelper;
pub use util::decode_field_name;
pub use util::format_field_name;

use super::plans::BasePlan;
use crate::pipelines::new::processors::AggregatorParams;
use crate::pipelines::new::processors::AggregatorTransformParams;
use crate::pipelines::new::processors::ProjectionTransform;
use crate::pipelines::new::processors::TransformAggregator;
use crate::pipelines::new::processors::TransformFilter;
use crate::pipelines::new::NewPipeline;
use crate::sessions::QueryContext;
use crate::sql::exec::data_schema_builder::DataSchemaBuilder;
use crate::sql::exec::expression_builder::ExpressionBuilder;
use crate::sql::exec::util::check_physical;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::AggregatePlan;
use crate::sql::plans::FilterPlan;
use crate::sql::plans::PhysicalScan;
use crate::sql::plans::PlanType;
use crate::sql::plans::ProjectPlan;
use crate::sql::plans::Scalar;
use crate::sql::IndexType;
use crate::sql::Metadata;

/// Helper to build a `Pipeline` from `SExpr`
pub struct PipelineBuilder {
    ctx: Arc<QueryContext>,
    metadata: Metadata,
    result_columns: Vec<(IndexType, String)>,
    expression: SExpr,
    pipeline: NewPipeline,
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
            pipeline: NewPipeline::create(),
        }
    }

    pub fn spawn(mut self) -> Result<NewPipeline> {
        let expr = self.expression.clone();
        let schema = self.build_pipeline(&expr)?;
        self.align_data_schema(schema)?;
        let settings = self.ctx.get_settings();
        self.pipeline
            .set_max_threads(settings.get_max_threads()? as usize);
        Ok(self.pipeline)
    }

    fn align_data_schema(&mut self, input_schema: DataSchemaRef) -> Result<()> {
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
            let field = if column_entry.nullable {
                DataField::new_nullable(name.as_str(), column_entry.data_type.clone())
            } else {
                DataField::new(name.as_str(), column_entry.data_type.clone())
            };
            output_fields.push(field);
        }
        let output_schema = Arc::new(DataSchema::new(output_fields));

        self.pipeline
            .add_transform(|transform_input_port, transform_output_port| {
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

    fn build_pipeline(&mut self, expression: &SExpr) -> Result<DataSchemaRef> {
        if !check_physical(expression) {
            return Err(ErrorCode::LogicalError("Invalid physical plan"));
        }

        let plan = expression.plan();

        match plan.plan_type() {
            PlanType::PhysicalScan => {
                let physical_scan: PhysicalScan = plan.try_into()?;
                self.build_physical_scan(&physical_scan)
            }
            PlanType::Project => {
                let project: ProjectPlan = plan.try_into()?;
                let input_schema = self.build_pipeline(&expression.children()[0])?;
                self.build_project(&project, input_schema)
            }
            PlanType::Filter => {
                let filter: FilterPlan = plan.try_into()?;
                let input_schema = self.build_pipeline(&expression.children()[0])?;
                self.build_filter(&filter, input_schema)
            }
            PlanType::Aggregate => {
                let aggregate = plan.as_any().downcast_ref::<AggregatePlan>().unwrap();
                let input_schema = self.build_pipeline(&expression.children()[0])?;
                dbg!(input_schema.clone());
                self.build_aggregate(aggregate, input_schema)
            }
            _ => Err(ErrorCode::LogicalError("Invalid physical plan")),
        }
    }

    fn build_project(
        &mut self,
        project: &ProjectPlan,
        input_schema: DataSchemaRef,
    ) -> Result<DataSchemaRef> {
        let schema_builder = DataSchemaBuilder::new(&self.metadata);
        let output_schema = schema_builder.build_project(project, input_schema.clone())?;
        dbg!(output_schema.clone());
        let mut expressions = Vec::with_capacity(project.items.len());
        let expr_builder = ExpressionBuilder::create(&self.metadata);
        for expr in project.items.iter() {
            let scalar = expr.expr.as_any().downcast_ref::<Scalar>().unwrap();
            let expression = expr_builder.build(scalar)?;
            expressions.push(expression);
        }
        self.pipeline
            .add_transform(|transform_input_port, transform_output_port| {
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
    ) -> Result<DataSchemaRef> {
        let output_schema = input_schema.clone();
        let eb = ExpressionBuilder::create(&self.metadata);
        let scalar = filter.predicate.as_any().downcast_ref::<Scalar>().unwrap();
        let pred = eb.build(scalar)?;
        self.pipeline
            .add_transform(|transform_input_port, transform_output_port| {
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

    fn build_physical_scan(&mut self, scan: &PhysicalScan) -> Result<DataSchemaRef> {
        let table_entry = self.metadata.table(scan.table_index);
        let plan = table_entry.source.clone();

        let table = self.ctx.build_table_from_source_plan(&plan)?;
        self.ctx.try_set_partitions(plan.parts.clone())?;
        table.read2(self.ctx.clone(), &plan, &mut self.pipeline)?;
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

        self.pipeline
            .add_transform(|transform_input_port, transform_output_port| {
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
    ) -> Result<DataSchemaRef> {
        let mut agg_expressions = Vec::with_capacity(aggregate.agg_expr.len());
        let expr_builder = ExpressionBuilder::create(&self.metadata);
        for scalar_expr in aggregate.agg_expr.iter() {
            let scalar = scalar_expr.as_any().downcast_ref::<Scalar>().unwrap();
            let expr = expr_builder.build(scalar)?;
            agg_expressions.push(expr);
        }
        let partial_schema = Arc::new(DataSchema::new(RewriteHelper::exprs_to_fields(
            agg_expressions.as_slice(),
            &input_schema,
        )?));

        dbg!(partial_schema.clone());

        let mut group_expressions = Vec::with_capacity(aggregate.group_expr.len());
        for scalar_expr in aggregate.group_expr.iter() {
            let scalar = scalar_expr.as_any().downcast_ref::<Scalar>().unwrap();
            let expr = expr_builder.build(scalar)?;
            group_expressions.push(expr);
        }
        let mut final_exprs = agg_expressions.to_owned();
        final_exprs.extend_from_slice(group_expressions.as_slice());
        let final_schema = Arc::new(DataSchema::new(RewriteHelper::exprs_to_fields(
            final_exprs.as_slice(),
            &input_schema,
        )?));

        dbg!(final_schema.clone());

        let partial_aggr_params = AggregatorParams::try_create_v2(
            &agg_expressions,
            &group_expressions,
            &input_schema,
            &partial_schema,
        )?;
        self.pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                TransformAggregator::try_create_partial(
                    transform_input_port.clone(),
                    transform_output_port.clone(),
                    AggregatorTransformParams::try_create(
                        transform_input_port,
                        transform_output_port,
                        &partial_aggr_params,
                    )?,
                )
            })?;

        let final_aggr_params = AggregatorParams::try_create_v2(
            &agg_expressions,
            &group_expressions,
            &input_schema,
            &final_schema,
        )?;

        self.pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                TransformAggregator::try_create_final(
                    transform_input_port.clone(),
                    transform_output_port.clone(),
                    AggregatorTransformParams::try_create(
                        transform_input_port,
                        transform_output_port,
                        &final_aggr_params,
                    )?,
                )
            })?;

        Ok(final_schema)
    }
}
