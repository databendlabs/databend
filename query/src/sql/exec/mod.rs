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

use common_datablocks::DataBlock;
use common_datablocks::HashMethodKind;
use common_datablocks::HashMethodSerializer;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataTypeImpl;
use common_datavalues::ToDataType;
use common_datavalues::Vu8;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::FunctionFactory;
use common_planners::find_aggregate_exprs;
use common_planners::find_aggregate_exprs_in_expr;
use common_planners::Expression;
use common_planners::RewriteHelper;
use primitive_types::U256;
use primitive_types::U512;
pub use util::decode_field_name;
pub use util::format_field_name;

use super::plans::JoinType;
use super::plans::RelOperator;
use super::MetadataRef;
use crate::common::HashMap;
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::transforms::hash_join::row::RowPtr;
use crate::pipelines::new::processors::AggregatorParams;
use crate::pipelines::new::processors::AggregatorTransformParams;
use crate::pipelines::new::processors::ChainingHashTable;
use crate::pipelines::new::processors::ExpressionTransform;
use crate::pipelines::new::processors::HashJoinState;
use crate::pipelines::new::processors::HashTable;
use crate::pipelines::new::processors::KeyU128HashTable;
use crate::pipelines::new::processors::KeyU16HashTable;
use crate::pipelines::new::processors::KeyU256HashTable;
use crate::pipelines::new::processors::KeyU32HashTable;
use crate::pipelines::new::processors::KeyU512HashTable;
use crate::pipelines::new::processors::KeyU64HashTable;
use crate::pipelines::new::processors::KeyU8HashTable;
use crate::pipelines::new::processors::ProjectionTransform;
use crate::pipelines::new::processors::SerializerHashTable;
use crate::pipelines::new::processors::SinkBuildHashTable;
use crate::pipelines::new::processors::Sinker;
use crate::pipelines::new::processors::SortMergeCompactor;
use crate::pipelines::new::processors::TransformAggregator;
use crate::pipelines::new::processors::TransformApply;
use crate::pipelines::new::processors::TransformFilter;
use crate::pipelines::new::processors::TransformHashJoinProbe;
use crate::pipelines::new::processors::TransformLimit;
use crate::pipelines::new::processors::TransformMax1Row;
use crate::pipelines::new::processors::TransformSortMerge;
use crate::pipelines::new::processors::TransformSortPartial;
use crate::pipelines::new::NewPipeline;
use crate::pipelines::new::SinkPipeBuilder;
use crate::pipelines::transforms::get_sort_descriptions;
use crate::pipelines::transforms::group_by::keys_ref::KeysRef;
use crate::sessions::QueryContext;
use crate::sql::exec::data_schema_builder::DataSchemaBuilder;
use crate::sql::exec::expression_builder::ExpressionBuilder;
use crate::sql::exec::util::check_physical;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::Aggregate;
use crate::sql::plans::AndExpr;
use crate::sql::plans::CrossApply;
use crate::sql::plans::EvalScalar;
use crate::sql::plans::Filter;
use crate::sql::plans::Limit;
use crate::sql::plans::PhysicalHashJoin;
use crate::sql::plans::PhysicalScan;
use crate::sql::plans::Project;
use crate::sql::plans::Scalar;
use crate::sql::plans::ScalarExpr;
use crate::sql::plans::Sort;
use crate::sql::IndexType;

/// Helper to build a `Pipeline` from `SExpr`
pub struct PipelineBuilder {
    ctx: Arc<QueryContext>,
    metadata: MetadataRef,
    result_columns: Vec<(IndexType, String)>,
    expression: SExpr,
    pub pipelines: Vec<NewPipeline>,
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

    pub fn spawn(mut self) -> Result<(NewPipeline, Vec<NewPipeline>, Arc<DataSchema>)> {
        let expr = self.expression.clone();
        let mut pipeline = NewPipeline::create();
        let schema = self.build_pipeline(self.ctx.clone(), &expr, &mut pipeline)?;
        let schema = self.align_data_schema(schema, &mut pipeline)?;
        let settings = self.ctx.get_settings();
        pipeline.set_max_threads(settings.get_max_threads()? as usize);
        for pipeline in self.pipelines.iter_mut() {
            pipeline.set_max_threads(settings.get_max_threads()? as usize);
        }
        Ok((pipeline, self.pipelines, schema))
    }

    fn align_data_schema(
        &mut self,
        input_schema: DataSchemaRef,
        pipeline: &mut NewPipeline,
    ) -> Result<Arc<DataSchema>> {
        let mut projections = Vec::with_capacity(self.result_columns.len());
        let mut output_fields = Vec::with_capacity(self.result_columns.len());
        for (index, name) in self.result_columns.iter() {
            let column_entry = self.metadata.read().column(*index).clone();
            projections.push(Expression::Alias(
                name.clone(),
                Box::new(Expression::Column(self.get_field_name(*index))),
            ));
            let field_name = self.get_field_name(*index);
            let mut data_type = column_entry.data_type.clone();
            // Field info in the input_schema is preferred
            if input_schema.has_field(&field_name) {
                data_type = input_schema
                    .field_with_name(&field_name)?
                    .data_type()
                    .clone();
            }
            let field = DataField::new(name.as_str(), data_type);
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
        Ok(output_schema)
    }

    pub fn build_pipeline(
        &mut self,
        context: Arc<QueryContext>,
        s_expr: &SExpr,
        pipeline: &mut NewPipeline,
    ) -> Result<DataSchemaRef> {
        if !check_physical(s_expr) {
            return Err(ErrorCode::LogicalError("Invalid physical plan"));
        }

        let plan = s_expr.plan();

        match plan {
            RelOperator::PhysicalScan(physical_scan) => {
                self.build_physical_scan(context, physical_scan, pipeline)
            }
            RelOperator::Project(project) => {
                let input_schema =
                    self.build_pipeline(context.clone(), s_expr.child(0)?, pipeline)?;
                self.build_project(context, project, input_schema, pipeline)
            }
            RelOperator::EvalScalar(eval_scalar) => {
                let input_schema =
                    self.build_pipeline(context.clone(), s_expr.child(0)?, pipeline)?;
                self.build_eval_scalar(context, eval_scalar, input_schema, pipeline)
            }
            RelOperator::Filter(filter) => {
                let input_schema =
                    self.build_pipeline(context.clone(), s_expr.child(0)?, pipeline)?;
                self.build_filter(context, filter, input_schema, pipeline)
            }
            RelOperator::Aggregate(aggregate) => {
                let input_schema =
                    self.build_pipeline(context.clone(), s_expr.child(0)?, pipeline)?;
                self.build_aggregate(context, aggregate, input_schema, pipeline)
            }
            RelOperator::PhysicalHashJoin(hash_join) => {
                let probe_schema =
                    self.build_pipeline(context.clone(), s_expr.child(0)?, pipeline)?;
                let mut child_pipeline = NewPipeline::create();
                let build_schema = self.build_pipeline(
                    QueryContext::create_from(context.clone()),
                    s_expr.child(1)?,
                    &mut child_pipeline,
                )?;
                self.build_hash_join(
                    context,
                    hash_join,
                    build_schema,
                    probe_schema,
                    child_pipeline,
                    pipeline,
                )
            }
            RelOperator::Sort(sort_plan) => {
                let input_schema =
                    self.build_pipeline(context.clone(), s_expr.child(0)?, pipeline)?;
                self.build_order_by(context, sort_plan, input_schema, pipeline)
            }
            RelOperator::Limit(limit_plan) => {
                let input_schema =
                    self.build_pipeline(context.clone(), s_expr.child(0)?, pipeline)?;
                self.build_limit(context, limit_plan, input_schema, pipeline)
            }
            RelOperator::CrossApply(apply_plan) => {
                let input_schema =
                    self.build_pipeline(context.clone(), s_expr.child(0)?, pipeline)?;
                self.build_apply(
                    context,
                    apply_plan,
                    s_expr.child(1)?,
                    input_schema,
                    pipeline,
                )
            }
            RelOperator::Max1Row(_) => {
                let input_schema = self.build_pipeline(context, s_expr.child(0)?, pipeline)?;
                pipeline
                    .add_transform(|input, output| Ok(TransformMax1Row::create(input, output)))?;
                Ok(input_schema)
            }
            _ => Err(ErrorCode::LogicalError("Invalid physical plan")),
        }
    }

    fn build_project(
        &mut self,
        ctx: Arc<QueryContext>,
        project: &Project,
        input_schema: DataSchemaRef,
        pipeline: &mut NewPipeline,
    ) -> Result<DataSchemaRef> {
        let schema_builder = DataSchemaBuilder::new(self.metadata.clone());
        let output_schema = schema_builder.build_project(project, input_schema.clone())?;
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
                ctx.clone(),
            )
        })?;

        Ok(output_schema)
    }

    fn build_eval_scalar(
        &mut self,
        ctx: Arc<QueryContext>,
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
                ctx.clone(),
            )
        })?;

        Ok(output_schema)
    }

    fn build_filter(
        &mut self,
        ctx: Arc<QueryContext>,
        filter: &Filter,
        input_schema: DataSchemaRef,
        pipeline: &mut NewPipeline,
    ) -> Result<DataSchemaRef> {
        let output_schema = input_schema.clone();
        let eb = ExpressionBuilder::create(self.metadata.clone());
        let scalars = &filter.predicates;
        let pred = scalars.iter().cloned().reduce(|acc, v| {
            let func = FunctionFactory::instance()
                .get("and", &[&acc.data_type(), &v.data_type()])
                .unwrap();
            AndExpr {
                left: Box::new(acc),
                right: Box::new(v),
                return_type: func.return_type(),
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
                ctx.clone(),
            )
        })?;
        Ok(output_schema)
    }

    fn build_physical_scan(
        &mut self,
        ctx: Arc<QueryContext>,
        scan: &PhysicalScan,
        pipeline: &mut NewPipeline,
    ) -> Result<DataSchemaRef> {
        let table_entry = self.metadata.read().table(scan.table_index).clone();
        let plan = table_entry.source;

        let table = ctx.build_table_from_source_plan(&plan)?;
        ctx.try_set_partitions(plan.parts.clone())?;
        table.read2(ctx.clone(), &plan, pipeline)?;
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
                ctx.clone(),
            )
        })?;

        Ok(output_schema)
    }

    fn build_aggregate(
        &mut self,
        ctx: Arc<QueryContext>,
        aggregate: &Aggregate,
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
        let mut partial_data_fields = partial_data_fields
            .iter()
            .map(|f| DataField::new(f.name(), Vu8::to_data_type()))
            .collect::<Vec<_>>();

        if !group_expressions.is_empty() {
            // Fields. [aggrs,  key]
            // aggrs: aggr_len aggregate states
            // key: Varint by hash method
            let group_cols: Vec<String> = group_expressions
                .iter()
                .map(|expr| expr.column_name())
                .collect();
            let sample_block = DataBlock::empty_with_schema(input_schema.clone());
            let method = DataBlock::choose_hash_method(&sample_block, &group_cols)?;
            partial_data_fields.push(DataField::new("_group_by_key", method.data_type()));
        }
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
                ctx.clone(),
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
                ctx.clone(),
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
                ctx.clone(),
            )
        })?;

        Ok(output_schema)
    }

    fn build_hash_join(
        &mut self,
        ctx: Arc<QueryContext>,
        hash_join: &PhysicalHashJoin,
        build_schema: DataSchemaRef,
        probe_schema: DataSchemaRef,
        mut child_pipeline: NewPipeline,
        pipeline: &mut NewPipeline,
    ) -> Result<DataSchemaRef> {
        let builder = DataSchemaBuilder::new(self.metadata.clone());
        let output_schema = builder.build_join(
            probe_schema.clone(),
            build_schema.clone(),
            &hash_join.join_type,
        );

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

        let hash_join_state = create_join_state(
            ctx.clone(),
            hash_join.join_type.clone(),
            hash_join.other_conditions.clone(),
            build_expressions,
            probe_expressions,
            build_schema,
            probe_schema,
        )?;

        // Build side
        self.build_sink_hash_table(hash_join_state.clone(), &mut child_pipeline)?;

        // Probe side
        pipeline.add_transform(|input, output| {
            Ok(TransformHashJoinProbe::create(
                ctx.clone(),
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
        ctx: Arc<QueryContext>,
        sort_plan: &Sort,
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
                ctx.clone(),
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
        _ctx: Arc<QueryContext>,
        limit_plan: &Limit,
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

    fn build_apply(
        &mut self,
        ctx: Arc<QueryContext>,
        apply_plan: &CrossApply,
        subquery: &SExpr,
        input_schema: DataSchemaRef,
        pipeline: &mut NewPipeline,
    ) -> Result<DataSchemaRef> {
        let schema_builder = DataSchemaBuilder::new(self.metadata.clone());
        let subquery_schema = DataSchemaRefExt::create(
            apply_plan
                .subquery_output
                .iter()
                .map(|index| {
                    let col = self.metadata.read().column(*index).clone();
                    DataField::new(
                        format_field_name(col.name.as_str(), col.column_index).as_str(),
                        col.data_type.clone(),
                    )
                })
                .collect(),
        );
        pipeline.add_transform(|input, output| {
            Ok(TransformApply::create(
                input,
                output,
                ctx.clone(),
                self.metadata.clone(),
                apply_plan.correlated_columns.clone(),
                subquery.clone(),
            ))
        })?;
        Ok(schema_builder.build_join(input_schema, subquery_schema, &JoinType::Inner))
    }
}

fn create_join_state(
    ctx: Arc<QueryContext>,
    join_type: JoinType,
    other_conditions: Vec<Scalar>,
    build_expressions: Vec<Expression>,
    probe_expressions: Vec<Expression>,
    build_schema: DataSchemaRef,
    probe_schema: DataSchemaRef,
) -> Result<Arc<ChainingHashTable>> {
    let hash_key_types = build_expressions
        .iter()
        .map(|expr| expr.to_data_type(&build_schema))
        .collect::<Result<Vec<DataTypeImpl>>>()?;
    let method = DataBlock::choose_hash_method_with_types(&hash_key_types)?;
    Ok(match method {
        HashMethodKind::SingleString(_) | HashMethodKind::Serializer(_) => {
            Arc::new(ChainingHashTable::try_create(
                ctx,
                join_type,
                other_conditions,
                HashTable::SerializerHashTable(SerializerHashTable {
                    hash_table: HashMap::<KeysRef, Vec<RowPtr>>::create(),
                    hash_method: HashMethodSerializer::default(),
                }),
                build_expressions,
                probe_expressions,
                build_schema,
                probe_schema,
            )?)
        }
        HashMethodKind::KeysU8(hash_method) => Arc::new(ChainingHashTable::try_create(
            ctx,
            join_type,
            other_conditions,
            HashTable::KeyU8HashTable(KeyU8HashTable {
                hash_table: HashMap::<u8, Vec<RowPtr>>::create(),
                hash_method,
            }),
            build_expressions,
            probe_expressions,
            build_schema,
            probe_schema,
        )?),
        HashMethodKind::KeysU16(hash_method) => Arc::new(ChainingHashTable::try_create(
            ctx,
            join_type,
            other_conditions,
            HashTable::KeyU16HashTable(KeyU16HashTable {
                hash_table: HashMap::<u16, Vec<RowPtr>>::create(),
                hash_method,
            }),
            build_expressions,
            probe_expressions,
            build_schema,
            probe_schema,
        )?),
        HashMethodKind::KeysU32(hash_method) => Arc::new(ChainingHashTable::try_create(
            ctx,
            join_type,
            other_conditions,
            HashTable::KeyU32HashTable(KeyU32HashTable {
                hash_table: HashMap::<u32, Vec<RowPtr>>::create(),
                hash_method,
            }),
            build_expressions,
            probe_expressions,
            build_schema,
            probe_schema,
        )?),
        HashMethodKind::KeysU64(hash_method) => Arc::new(ChainingHashTable::try_create(
            ctx,
            join_type,
            other_conditions,
            HashTable::KeyU64HashTable(KeyU64HashTable {
                hash_table: HashMap::<u64, Vec<RowPtr>>::create(),
                hash_method,
            }),
            build_expressions,
            probe_expressions,
            build_schema,
            probe_schema,
        )?),
        HashMethodKind::KeysU128(hash_method) => Arc::new(ChainingHashTable::try_create(
            ctx,
            join_type,
            other_conditions,
            HashTable::KeyU128HashTable(KeyU128HashTable {
                hash_table: HashMap::<u128, Vec<RowPtr>>::create(),
                hash_method,
            }),
            build_expressions,
            probe_expressions,
            build_schema,
            probe_schema,
        )?),
        HashMethodKind::KeysU256(hash_method) => Arc::new(ChainingHashTable::try_create(
            ctx,
            join_type,
            other_conditions,
            HashTable::KeyU256HashTable(KeyU256HashTable {
                hash_table: HashMap::<U256, Vec<RowPtr>>::create(),
                hash_method,
            }),
            build_expressions,
            probe_expressions,
            build_schema,
            probe_schema,
        )?),
        HashMethodKind::KeysU512(hash_method) => Arc::new(ChainingHashTable::try_create(
            ctx,
            join_type,
            other_conditions,
            HashTable::KeyU512HashTable(KeyU512HashTable {
                hash_table: HashMap::<U512, Vec<RowPtr>>::create(),
                hash_method,
            }),
            build_expressions,
            probe_expressions,
            build_schema,
            probe_schema,
        )?),
    })
}
