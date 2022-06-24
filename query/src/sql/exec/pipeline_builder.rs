// Copyright 2022 Datafuse Labs.
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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use common_datablocks::SortColumnDescription;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::aggregates::AggregateFunctionFactory;
use common_functions::aggregates::AggregateFunctionRef;
use common_functions::scalars::FunctionFactory;
use common_planners::ReadDataSourcePlan;

use crate::common::EvalNode;
use crate::common::Evaluator;
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::transforms::ExpressionTransformV2;
use crate::pipelines::new::processors::transforms::TransformFilterV2;
use crate::pipelines::new::processors::transforms::TransformProject;
use crate::pipelines::new::processors::transforms::TransformRename;
use crate::pipelines::new::processors::AggregatorParams;
use crate::pipelines::new::processors::AggregatorTransformParams;
use crate::pipelines::new::processors::ChainingHashTable;
use crate::pipelines::new::processors::HashJoinState;
use crate::pipelines::new::processors::SinkBuildHashTable;
use crate::pipelines::new::processors::Sinker;
use crate::pipelines::new::processors::SortMergeCompactor;
use crate::pipelines::new::processors::TransformAggregator;
use crate::pipelines::new::processors::TransformApply;
use crate::pipelines::new::processors::TransformHashJoinProbe;
use crate::pipelines::new::processors::TransformLimit;
use crate::pipelines::new::processors::TransformMax1Row;
use crate::pipelines::new::processors::TransformSortMerge;
use crate::pipelines::new::processors::TransformSortPartial;
use crate::pipelines::new::NewPipeline;
use crate::pipelines::new::SinkPipeBuilder;
use crate::sessions::QueryContext;
use crate::sql::exec::physical_plan::ColumnID;
use crate::sql::exec::physical_plan::PhysicalPlan;
use crate::sql::exec::AggregateFunctionDesc;
use crate::sql::exec::PhysicalScalar;
use crate::sql::exec::SortDesc;
use crate::sql::plans::JoinType;
use crate::sql::ColumnBinding;

#[derive(Default)]
pub struct PipelineBuilder {
    pub pipelines: Vec<NewPipeline>,
}

impl PipelineBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn build_pipeline(
        &mut self,
        context: Arc<QueryContext>,
        plan: &PhysicalPlan,
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        match plan {
            PhysicalPlan::TableScan {
                name_mapping,
                source,
                ..
            } => self.build_table_scan(context, name_mapping, source, pipeline),
            PhysicalPlan::Filter { input, predicates } => {
                self.build_pipeline(context.clone(), input, pipeline)?;
                self.build_filter(context, predicates, pipeline)
            }
            PhysicalPlan::Project { input, projections } => {
                self.build_pipeline(context, input, pipeline)?;
                self.build_project(projections, pipeline)
            }
            PhysicalPlan::EvalScalar { input, scalars } => {
                self.build_pipeline(context.clone(), input, pipeline)?;
                self.build_eval_scalar(context, scalars, pipeline)
            }
            PhysicalPlan::AggregatePartial {
                input,
                group_by,
                agg_funcs,
            } => {
                self.build_pipeline(context.clone(), input, pipeline)?;
                self.build_aggregate_partial(
                    context,
                    input.output_schema()?,
                    plan.output_schema()?,
                    group_by,
                    agg_funcs,
                    pipeline,
                )
            }
            PhysicalPlan::AggregateFinal {
                input,
                group_by,
                agg_funcs,
                before_group_by_schema,
            } => {
                self.build_pipeline(context.clone(), input, pipeline)?;
                self.build_aggregate_final(
                    context,
                    before_group_by_schema.clone(),
                    plan.output_schema()?,
                    group_by,
                    agg_funcs,
                    pipeline,
                )
            }
            PhysicalPlan::Sort { input, order_by } => {
                self.build_pipeline(context.clone(), input, pipeline)?;
                self.build_sort(context, order_by, pipeline)
            }
            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                self.build_pipeline(context, input, pipeline)?;
                self.build_limit(*limit, *offset, pipeline)
            }
            PhysicalPlan::HashJoin {
                build,
                probe,
                build_keys,
                probe_keys,
                other_conditions,
                join_type,
            } => {
                let mut build_side_pipeline = NewPipeline::create();
                let build_side_context = QueryContext::create_from(context.clone());
                self.build_pipeline(build_side_context, build, &mut build_side_pipeline)?;
                self.build_pipeline(context.clone(), probe, pipeline)?;
                self.build_hash_join(
                    context,
                    plan.output_schema()?,
                    build.output_schema()?,
                    build_keys,
                    probe_keys,
                    other_conditions,
                    join_type.clone(),
                    build_side_pipeline,
                    pipeline,
                )?;
                Ok(())
            }
            PhysicalPlan::CrossApply {
                input,
                subquery,
                correlated_columns,
            } => {
                self.build_pipeline(context.clone(), input, pipeline)?;
                self.build_apply(context, subquery, correlated_columns, pipeline)
            }
            PhysicalPlan::Max1Row { input } => {
                self.build_pipeline(context, input, pipeline)?;
                self.build_max_one_row(pipeline)
            }
        }
    }

    pub fn render_result_set(
        &mut self,
        input_schema: DataSchemaRef,
        result_columns: &[ColumnBinding],
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        let mut projections = Vec::with_capacity(result_columns.len());
        let mut result_fields = Vec::with_capacity(result_columns.len());
        for column_binding in result_columns {
            let index = column_binding.index;
            let name = column_binding.column_name.clone();
            let data_type = input_schema
                .field_with_name(index.to_string().as_str())?
                .data_type()
                .clone();
            projections.push(input_schema.index_of(index.to_string().as_str())?);
            result_fields.push(DataField::new(name.as_str(), data_type.clone()));
        }
        let output_schema = DataSchemaRefExt::create(result_fields);
        pipeline.add_transform(|input, output| {
            Ok(TransformProject::create(input, output, projections.clone()))
        })?;
        pipeline.add_transform(|input, output| {
            Ok(TransformRename::create(
                input,
                output,
                output_schema.clone(),
            ))
        })?;

        Ok(())
    }

    pub fn build_table_scan(
        &mut self,
        context: Arc<QueryContext>,
        name_mapping: &BTreeMap<String, ColumnID>,
        source: &ReadDataSourcePlan,
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        let table = context.build_table_from_source_plan(source)?;
        context.try_set_partitions(source.parts.clone())?;
        table.read2(context.clone(), source, pipeline)?;
        let schema = DataSchemaRefExt::create(
            source
                .schema()
                .fields()
                .iter()
                .map(|field| {
                    Ok(DataField::new(
                        name_mapping
                            .get(field.name())
                            .cloned()
                            .ok_or_else(|| {
                                ErrorCode::LogicalError(format!(
                                    "Column {} is not in the output schema of the table scan",
                                    field.name()
                                ))
                            })?
                            .as_str(),
                        field.data_type().clone(),
                    ))
                })
                .collect::<Result<_>>()?,
        );

        pipeline.add_transform(|input, output| {
            Ok(TransformRename::create(input, output, schema.clone()))
        })?;

        Ok(())
    }

    pub fn build_filter(
        &mut self,
        context: Arc<QueryContext>,
        predicates: &[PhysicalScalar],
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        if predicates.is_empty() {
            return Err(ErrorCode::LogicalError(
                "Invalid empty predicate list".to_string(),
            ));
        }
        let mut predicate = predicates[0].clone();
        for pred in predicates.iter().skip(1) {
            let left_type = predicate.data_type();
            let right_type = pred.data_type();
            let data_types = vec![&left_type, &right_type];
            let func = FunctionFactory::instance().get("and", &data_types)?;
            predicate = PhysicalScalar::Function {
                name: "and".to_string(),
                args: vec![
                    (predicate.clone(), predicate.data_type()),
                    (pred.clone(), pred.data_type()),
                ],
                return_type: func.return_type(),
            };
        }
        let func_ctx = context.try_get_function_context()?;

        pipeline.add_transform(|input, output| {
            TransformFilterV2::try_create(
                input,
                output,
                Evaluator::eval_physical_scalar(&predicate)?,
                func_ctx.clone(),
            )
        })?;

        Ok(())
    }

    pub fn build_project(
        &mut self,
        projections: &[usize],
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        pipeline.add_transform(|input, output| {
            Ok(TransformProject::create(
                input,
                output,
                projections.to_vec(),
            ))
        })
    }

    pub fn build_eval_scalar(
        &mut self,
        context: Arc<QueryContext>,
        scalars: &[(PhysicalScalar, ColumnID)],
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        let eval_nodes: Vec<(EvalNode<ColumnID>, String)> = scalars
            .iter()
            .map(|(scalar, id)| Ok((Evaluator::eval_physical_scalar(scalar)?, id.clone())))
            .collect::<Result<_>>()?;
        let func_ctx = context.try_get_function_context()?;

        pipeline.add_transform(|input, output| {
            Ok(ExpressionTransformV2::create(
                input,
                output,
                eval_nodes.clone(),
                func_ctx.clone(),
            ))
        })?;

        Ok(())
    }

    pub fn build_aggregate_partial(
        &mut self,
        context: Arc<QueryContext>,
        input_schema: DataSchemaRef,
        output_schema: DataSchemaRef,
        group_by: &[ColumnID],
        agg_funcs: &[AggregateFunctionDesc],
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        let params =
            Self::build_aggregator_params(input_schema, output_schema, group_by, agg_funcs)?;

        pipeline.add_transform(|input, output| {
            TransformAggregator::try_create_partial(
                input.clone(),
                output.clone(),
                AggregatorTransformParams::try_create(input, output, &params)?,
                context.clone(),
            )
        })?;

        Ok(())
    }

    pub fn build_aggregate_final(
        &mut self,
        context: Arc<QueryContext>,
        input_schema: DataSchemaRef,
        output_schema: DataSchemaRef,
        group_by: &[ColumnID],
        agg_funcs: &[AggregateFunctionDesc],
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        let params =
            Self::build_aggregator_params(input_schema, output_schema, group_by, agg_funcs)?;

        pipeline.resize(1)?;
        pipeline.add_transform(|input, output| {
            TransformAggregator::try_create_final(
                input.clone(),
                output.clone(),
                AggregatorTransformParams::try_create(input, output, &params)?,
                context.clone(),
            )
        })?;

        Ok(())
    }

    pub fn build_aggregator_params(
        input_schema: DataSchemaRef,
        output_schema: DataSchemaRef,
        group_by: &[ColumnID],
        agg_funcs: &[AggregateFunctionDesc],
    ) -> Result<Arc<AggregatorParams>> {
        let before_schema = input_schema.clone();
        let mut output_names = Vec::with_capacity(agg_funcs.len() + group_by.len());
        let mut group_fields = Vec::with_capacity(group_by.len());
        let mut agg_args = Vec::with_capacity(agg_funcs.len());
        let aggs: Vec<AggregateFunctionRef> = agg_funcs
            .iter()
            .map(|agg_func| {
                agg_args.push(agg_func.args.clone());
                AggregateFunctionFactory::instance().get(
                    agg_func.sig.name.as_str(),
                    agg_func.sig.params.clone(),
                    agg_func
                        .args
                        .iter()
                        .map(|id| input_schema.field_with_name(id.as_str()).cloned())
                        .collect::<Result<_>>()?,
                )
            })
            .collect::<Result<_>>()?;
        for agg in agg_funcs {
            output_names.push(agg.column_id.clone());
        }
        for group in group_by {
            let group_field = before_schema.field_with_name(group.as_str())?.clone();
            group_fields.push(group_field);
        }

        let params = AggregatorParams::try_create_v2(
            output_schema,
            before_schema,
            &group_fields,
            &aggs,
            &output_names,
            &agg_args,
        )?;

        Ok(params)
    }

    pub fn build_sort(
        &mut self,
        _context: Arc<QueryContext>,
        order_by: &[SortDesc],
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        let sort_desc: Vec<SortColumnDescription> = order_by
            .iter()
            .map(|desc| SortColumnDescription {
                column_name: desc.order_by.clone(),
                asc: desc.asc,
                nulls_first: desc.nulls_first,
            })
            .collect();

        // Sort
        pipeline.add_transform(|input, output| {
            TransformSortPartial::try_create(input, output, None, sort_desc.clone())
        })?;

        // Merge
        pipeline.add_transform(|input, output| {
            TransformSortMerge::try_create(
                input,
                output,
                SortMergeCompactor::new(None, sort_desc.clone()),
            )
        })?;

        pipeline.resize(1)?;

        // Concat merge in single thread
        pipeline.add_transform(|input, output| {
            TransformSortMerge::try_create(
                input,
                output,
                SortMergeCompactor::new(None, sort_desc.clone()),
            )
        })?;

        Ok(())
    }

    pub fn build_limit(
        &mut self,
        limit: Option<usize>,
        offset: usize,
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        pipeline.add_transform(|input, output| {
            TransformLimit::try_create(limit, offset, input, output)
        })?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn build_hash_join(
        &mut self,
        ctx: Arc<QueryContext>,
        output_schema: DataSchemaRef,
        build_schema: DataSchemaRef,
        build_keys: &[PhysicalScalar],
        probe_keys: &[PhysicalScalar],
        other_conditions: &[PhysicalScalar],
        join_type: JoinType,
        mut child_pipeline: NewPipeline,
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        let hash_join_state = ChainingHashTable::create_join_state(
            ctx.clone(),
            join_type,
            build_keys,
            probe_keys,
            other_conditions,
            build_schema,
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

        Ok(())
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

    pub fn build_apply(
        &mut self,
        context: Arc<QueryContext>,
        subquery: &PhysicalPlan,
        outer_columns: &BTreeSet<ColumnID>,
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        pipeline.add_transform(|input, output| {
            Ok(TransformApply::create(
                input,
                output,
                context.clone(),
                outer_columns.clone(),
                subquery.clone(),
            ))
        })?;

        Ok(())
    }

    pub fn build_max_one_row(&mut self, pipeline: &mut NewPipeline) -> Result<()> {
        pipeline.add_transform(|input, output| Ok(TransformMax1Row::create(input, output)))?;

        Ok(())
    }
}
