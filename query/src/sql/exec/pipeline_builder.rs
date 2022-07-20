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

use crate::evaluator::EvalNode;
use crate::evaluator::Evaluator;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::transforms::ExpressionTransformV2;
use crate::pipelines::processors::transforms::TransformFilterV2;
use crate::pipelines::processors::transforms::TransformMarkJoin;
use crate::pipelines::processors::transforms::TransformProject;
use crate::pipelines::processors::transforms::TransformRename;
use crate::pipelines::processors::AggregatorParams;
use crate::pipelines::processors::AggregatorTransformParams;
use crate::pipelines::processors::JoinHashTable;
use crate::pipelines::processors::MarkJoinCompactor;
use crate::pipelines::processors::SinkBuildHashTable;
use crate::pipelines::processors::Sinker;
use crate::pipelines::processors::SortMergeCompactor;
use crate::pipelines::processors::TransformAggregator;
use crate::pipelines::processors::TransformApply;
use crate::pipelines::processors::TransformHashJoinProbe;
use crate::pipelines::processors::TransformLimit;
use crate::pipelines::processors::TransformMax1Row;
use crate::pipelines::processors::TransformSortMerge;
use crate::pipelines::processors::TransformSortPartial;
use crate::pipelines::Pipeline;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::SinkPipeBuilder;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::exec::physical_plan::ColumnID;
use crate::sql::exec::physical_plan::PhysicalPlan;
use crate::sql::exec::AggregateFunctionDesc;
use crate::sql::exec::PhysicalScalar;
use crate::sql::exec::SortDesc;
use crate::sql::plans::JoinType;
use crate::sql::ColumnBinding;

pub struct PipelineBuilder {
    ctx: Arc<QueryContext>,
    main_pipeline: Pipeline,
    pub pipelines: Vec<Pipeline>,
}

impl PipelineBuilder {
    pub fn create(ctx: Arc<QueryContext>) -> PipelineBuilder {
        PipelineBuilder {
            ctx,
            pipelines: vec![],
            main_pipeline: Pipeline::create(),
        }
    }

    pub fn finalize(mut self, plan: &PhysicalPlan) -> Result<PipelineBuildResult> {
        self.build_pipeline(plan)?;

        for source_pipeline in &self.pipelines {
            if !source_pipeline.is_complete_pipeline()? {
                return Err(ErrorCode::IllegalPipelineState(
                    "Source pipeline must be complete pipeline.",
                ));
            }
        }

        Ok(PipelineBuildResult {
            main_pipeline: self.main_pipeline,
            sources_pipelines: self.pipelines,
        })
    }

    fn build_pipeline(&mut self, plan: &PhysicalPlan) -> Result<()> {
        match plan {
            PhysicalPlan::TableScan {
                name_mapping,
                source,
                ..
            } => self.build_table_scan(plan.output_schema()?, name_mapping, source),
            PhysicalPlan::Filter { input, predicates } => {
                self.build_pipeline(input)?;
                self.build_filter(predicates)
            }
            PhysicalPlan::Project { input, projections } => {
                self.build_pipeline(input)?;
                self.build_project(projections)
            }
            PhysicalPlan::EvalScalar { input, scalars } => {
                self.build_pipeline(input)?;
                self.build_eval_scalar(scalars)
            }
            PhysicalPlan::AggregatePartial {
                input,
                group_by,
                agg_funcs,
            } => {
                self.build_pipeline(input)?;
                self.build_aggregate_partial(
                    input.output_schema()?,
                    plan.output_schema()?,
                    group_by,
                    agg_funcs,
                )
            }
            PhysicalPlan::AggregateFinal {
                input,
                group_by,
                agg_funcs,
                before_group_by_schema,
            } => {
                self.build_pipeline(input)?;
                self.build_aggregate_final(
                    before_group_by_schema.clone(),
                    plan.output_schema()?,
                    group_by,
                    agg_funcs,
                )
            }
            PhysicalPlan::Sort { input, order_by } => {
                self.build_pipeline(input)?;
                self.build_sort(order_by)
            }
            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                self.build_pipeline(input)?;
                self.build_limit(*limit, *offset)
            }
            PhysicalPlan::HashJoin {
                build,
                probe,
                build_keys,
                probe_keys,
                other_conditions,
                join_type,
                marker_index,
            } => {
                let predicate = Self::join_predicate(other_conditions)?;
                let join_state = JoinHashTable::create_join_state(
                    self.ctx.clone(),
                    join_type.clone(),
                    build_keys,
                    probe_keys,
                    predicate.as_ref(),
                    build.output_schema()?,
                    *marker_index,
                )?;

                self.expand_build_side_pipeline(build, join_state.clone())?;

                self.build_pipeline(probe)?;
                self.build_hash_join(plan.output_schema()?, join_type.clone(), join_state)?;
                Ok(())
            }
            v @ PhysicalPlan::CrossApply {
                input,
                subquery,
                correlated_columns,
            } => {
                self.build_pipeline(input)?;
                self.build_apply(subquery, correlated_columns, v.output_schema()?)
            }
            PhysicalPlan::Max1Row { input } => {
                self.build_pipeline(input)?;
                self.build_max_one_row()
            }
        }
    }

    fn expand_build_side_pipeline(
        &mut self,
        build: &PhysicalPlan,
        join_state: Arc<JoinHashTable>,
    ) -> Result<()> {
        let build_side_context = QueryContext::create_from(self.ctx.clone());
        let build_side_builder = PipelineBuilder::create(build_side_context);
        let mut build_res = build_side_builder.finalize(build)?;

        assert!(build_res.main_pipeline.is_pulling_pipeline()?);
        let mut sink_pipeline_builder = SinkPipeBuilder::create();
        for _index in 0..build_res.main_pipeline.output_len() {
            let input_port = InputPort::create();
            sink_pipeline_builder.add_sink(
                input_port.clone(),
                Sinker::<SinkBuildHashTable>::create(
                    input_port,
                    SinkBuildHashTable::try_create(join_state.clone())?,
                ),
            );
        }

        build_res
            .main_pipeline
            .add_pipe(sink_pipeline_builder.finalize());

        self.pipelines.push(build_res.main_pipeline);
        self.pipelines
            .extend(build_res.sources_pipelines.into_iter());
        Ok(())
    }

    fn join_predicate(other_conditions: &[PhysicalScalar]) -> Result<Option<PhysicalScalar>> {
        other_conditions.iter().cloned().fold(
            Result::<Option<PhysicalScalar>>::Ok(None),
            |acc, next| {
                if let Ok(None) = acc {
                    Ok(Some(next))
                } else if let Ok(Some(prev)) = acc {
                    let left_type = prev.data_type();
                    let right_type = next.data_type();
                    let data_types = vec![&left_type, &right_type];
                    let func = FunctionFactory::instance().get("and", &data_types)?;
                    Ok(Some(PhysicalScalar::Function {
                        name: "and".to_string(),
                        args: vec![
                            (prev.clone(), prev.data_type()),
                            (next.clone(), next.data_type()),
                        ],
                        return_type: func.return_type(),
                    }))
                } else {
                    acc
                }
            },
        )
    }

    pub fn render_result_set(
        input_schema: DataSchemaRef,
        result_columns: &[ColumnBinding],
        pipeline: &mut Pipeline,
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

    fn build_table_scan(
        &mut self,
        output_schema: DataSchemaRef,
        name_mapping: &BTreeMap<String, ColumnID>,
        source: &ReadDataSourcePlan,
    ) -> Result<()> {
        let table = self.ctx.build_table_from_source_plan(source)?;
        self.ctx.try_set_partitions(source.parts.clone())?;
        table.read2(self.ctx.clone(), source, &mut self.main_pipeline)?;
        let schema = source.schema();
        let projections = name_mapping
            .iter()
            .map(|(name, _)| schema.index_of(name.as_str()))
            .collect::<Result<Vec<usize>>>()?;

        self.main_pipeline.add_transform(|input, output| {
            Ok(TransformProject::create(input, output, projections.clone()))
        })?;

        self.main_pipeline.add_transform(|input, output| {
            Ok(TransformRename::create(
                input,
                output,
                output_schema.clone(),
            ))
        })
    }

    fn build_filter(&mut self, predicates: &[PhysicalScalar]) -> Result<()> {
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
        let func_ctx = self.ctx.try_get_function_context()?;

        self.main_pipeline.add_transform(|input, output| {
            TransformFilterV2::try_create(
                input,
                output,
                Evaluator::eval_physical_scalar(&predicate)?,
                func_ctx.clone(),
            )
        })?;

        Ok(())
    }

    fn build_project(&mut self, projections: &[usize]) -> Result<()> {
        self.main_pipeline.add_transform(|input, output| {
            Ok(TransformProject::create(
                input,
                output,
                projections.to_vec(),
            ))
        })
    }

    fn build_eval_scalar(&mut self, scalars: &[(PhysicalScalar, ColumnID)]) -> Result<()> {
        let eval_nodes: Vec<(EvalNode<ColumnID>, String)> = scalars
            .iter()
            .map(|(scalar, id)| Ok((Evaluator::eval_physical_scalar(scalar)?, id.clone())))
            .collect::<Result<_>>()?;
        let func_ctx = self.ctx.try_get_function_context()?;

        self.main_pipeline.add_transform(|input, output| {
            Ok(ExpressionTransformV2::create(
                input,
                output,
                eval_nodes.clone(),
                func_ctx.clone(),
            ))
        })?;

        Ok(())
    }

    fn build_aggregate_partial(
        &mut self,
        input_schema: DataSchemaRef,
        output_schema: DataSchemaRef,
        group_by: &[ColumnID],
        agg_funcs: &[AggregateFunctionDesc],
    ) -> Result<()> {
        let params =
            Self::build_aggregator_params(input_schema, output_schema, group_by, agg_funcs)?;

        self.main_pipeline.add_transform(|input, output| {
            TransformAggregator::try_create_partial(
                input.clone(),
                output.clone(),
                AggregatorTransformParams::try_create(input, output, &params)?,
                self.ctx.clone(),
            )
        })?;

        Ok(())
    }

    fn build_aggregate_final(
        &mut self,
        input_schema: DataSchemaRef,
        output_schema: DataSchemaRef,
        group_by: &[ColumnID],
        agg_funcs: &[AggregateFunctionDesc],
    ) -> Result<()> {
        let params =
            Self::build_aggregator_params(input_schema, output_schema, group_by, agg_funcs)?;

        self.main_pipeline.resize(1)?;
        self.main_pipeline.add_transform(|input, output| {
            TransformAggregator::try_create_final(
                input.clone(),
                output.clone(),
                AggregatorTransformParams::try_create(input, output, &params)?,
                self.ctx.clone(),
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

    fn build_sort(&mut self, order_by: &[SortDesc]) -> Result<()> {
        let sort_desc: Vec<SortColumnDescription> = order_by
            .iter()
            .map(|desc| SortColumnDescription {
                column_name: desc.order_by.clone(),
                asc: desc.asc,
                nulls_first: desc.nulls_first,
            })
            .collect();

        // Sort
        self.main_pipeline.add_transform(|input, output| {
            TransformSortPartial::try_create(input, output, None, sort_desc.clone())
        })?;

        // Merge
        self.main_pipeline.add_transform(|input, output| {
            TransformSortMerge::try_create(
                input,
                output,
                SortMergeCompactor::new(None, sort_desc.clone()),
            )
        })?;

        self.main_pipeline.resize(1)?;

        // Concat merge in single thread
        self.main_pipeline.add_transform(|input, output| {
            TransformSortMerge::try_create(
                input,
                output,
                SortMergeCompactor::new(None, sort_desc.clone()),
            )
        })
    }

    fn build_limit(&mut self, limit: Option<usize>, offset: usize) -> Result<()> {
        self.main_pipeline.resize(1)?;

        self.main_pipeline
            .add_transform(|input, output| TransformLimit::try_create(limit, offset, input, output))
    }

    #[allow(clippy::too_many_arguments)]
    fn build_hash_join(
        &mut self,
        output_schema: DataSchemaRef,
        join_type: JoinType,
        hash_join_state: Arc<JoinHashTable>,
    ) -> Result<()> {
        // Probe side
        self.main_pipeline.add_transform(|input, output| {
            Ok(TransformHashJoinProbe::create(
                self.ctx.clone(),
                input,
                output,
                hash_join_state.clone(),
                output_schema.clone(),
            ))
        })?;

        if join_type == JoinType::Mark {
            self.main_pipeline.resize(1)?;
            self.main_pipeline.add_transform(|input, output| {
                TransformMarkJoin::try_create(
                    input,
                    output,
                    MarkJoinCompactor::create(hash_join_state.clone()),
                )
            })?;
        }

        Ok(())
    }

    fn build_apply(
        &mut self,
        subquery: &PhysicalPlan,
        outer_columns: &BTreeSet<ColumnID>,
        output_schema: DataSchemaRef,
    ) -> Result<()> {
        let ctx = self.ctx.clone();
        self.main_pipeline.add_transform(|input, output| {
            Ok(TransformApply::create(
                input,
                output,
                ctx.clone(),
                outer_columns.clone(),
                output_schema.clone(),
                subquery.clone(),
            ))
        })
    }

    fn build_max_one_row(&mut self) -> Result<()> {
        self.main_pipeline
            .add_transform(|input, output| Ok(TransformMax1Row::create(input, output)))
    }
}
