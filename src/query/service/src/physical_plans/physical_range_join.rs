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

use std::any::Any;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::common_super_type;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sinks::Sinker;
use databend_common_sql::binder::wrap_cast;
use databend_common_sql::binder::JoinPredicate;
use databend_common_sql::optimizer::ir::RelExpr;
use databend_common_sql::optimizer::ir::RelationalProperty;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::JoinType;
use databend_common_sql::ColumnSet;
use databend_common_sql::ScalarExpr;
use databend_common_sql::TypeCheck;

use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::RangeJoinFormatter;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::physical_plans::PhysicalPlanBuilder;
use crate::pipelines::processors::transforms::range_join::RangeJoinState;
use crate::pipelines::processors::transforms::range_join::TransformRangeJoinLeft;
use crate::pipelines::processors::transforms::range_join::TransformRangeJoinRight;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RangeJoin {
    pub meta: PhysicalPlanMeta,
    pub left: PhysicalPlan,
    pub right: PhysicalPlan,
    // The first two conditions: (>, >=, <, <=)
    // Condition's left/right side only contains one table's column
    pub conditions: Vec<RangeJoinCondition>,
    // The other conditions
    pub other_conditions: Vec<RemoteExpr>,
    // Now only support inner join, will support left/right join later
    pub join_type: JoinType,
    pub range_join_type: RangeJoinType,
    pub output_schema: DataSchemaRef,

    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

#[typetag::serde]
impl IPhysicalPlan for RangeJoin {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    #[recursive::recursive]
    fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.output_schema.clone())
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.left).chain(std::iter::once(&self.right)))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.left).chain(std::iter::once(&mut self.right)))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(RangeJoinFormatter::create(self))
    }

    fn get_desc(&self) -> Result<String> {
        let mut condition = self
            .conditions
            .iter()
            .map(|condition| {
                let left = condition
                    .left_expr
                    .as_expr(&BUILTIN_FUNCTIONS)
                    .sql_display();
                let right = condition
                    .right_expr
                    .as_expr(&BUILTIN_FUNCTIONS)
                    .sql_display();
                format!("{left} {:?} {right}", condition.operator)
            })
            .collect::<Vec<_>>();

        condition.extend(
            self.other_conditions
                .iter()
                .map(|x| x.as_expr(&BUILTIN_FUNCTIONS).sql_display()),
        );

        Ok(condition.join(" AND "))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 2);
        let right_child = children.pop().unwrap();
        let left_child = children.pop().unwrap();

        PhysicalPlan::new(RangeJoin {
            meta: self.meta.clone(),
            left: left_child,
            right: right_child,
            conditions: self.conditions.clone(),
            other_conditions: self.other_conditions.clone(),
            join_type: self.join_type,
            range_join_type: self.range_join_type.clone(),
            output_schema: self.output_schema.clone(),
            stat_info: self.stat_info.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let state = Arc::new(RangeJoinState::new(builder.ctx.clone(), self));
        self.build_right(state.clone(), builder)?;
        self.build_left(state, builder)
    }
}

impl RangeJoin {
    // Build the left-side pipeline for Range Join
    fn build_left(&self, state: Arc<RangeJoinState>, builder: &mut PipelineBuilder) -> Result<()> {
        self.left.build_pipeline(builder)?;

        let max_threads = builder.settings.get_max_threads()? as usize;
        builder.main_pipeline.try_resize(max_threads)?;
        builder.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(TransformRangeJoinLeft::create(
                input,
                output,
                state.clone(),
            )))
        })
    }

    // Build the right-side pipeline for Range Join
    fn build_right(&self, state: Arc<RangeJoinState>, builder: &mut PipelineBuilder) -> Result<()> {
        let right_side_builder = builder.create_sub_pipeline_builder();

        let mut right_res = right_side_builder.finalize(&self.right)?;
        right_res.main_pipeline.add_sink(|input| {
            Ok(ProcessorPtr::create(
                Sinker::<TransformRangeJoinRight>::create(
                    input,
                    TransformRangeJoinRight::create(state.clone()),
                ),
            ))
        })?;

        builder
            .pipelines
            .push(right_res.main_pipeline.finalize(None));
        builder.pipelines.extend(right_res.sources_pipelines);
        Ok(())
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum RangeJoinType {
    IEJoin,
    Merge,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RangeJoinCondition {
    pub left_expr: RemoteExpr,
    pub right_expr: RemoteExpr,
    // "gt" | "lt" | "gte" | "lte"
    pub operator: String,
}

impl PhysicalPlanBuilder {
    pub async fn build_range_join(
        &mut self,
        join_type: JoinType,
        s_expr: &SExpr,
        left_required: ColumnSet,
        right_required: ColumnSet,
        mut range_conditions: Vec<ScalarExpr>,
        mut other_conditions: Vec<ScalarExpr>,
    ) -> Result<PhysicalPlan> {
        let left_prop = RelExpr::with_s_expr(s_expr.right_child()).derive_relational_prop()?;
        let right_prop = RelExpr::with_s_expr(s_expr.left_child()).derive_relational_prop()?;

        debug_assert!(!range_conditions.is_empty());

        let range_join_type = if range_conditions.len() >= 2 {
            // Contain more than 2 ie conditions, use ie join
            while range_conditions.len() > 2 {
                other_conditions.push(range_conditions.pop().unwrap());
            }
            RangeJoinType::IEJoin
        } else {
            RangeJoinType::Merge
        };

        // Construct IEJoin
        let (right_side, left_side) = self
            .build_join_sides(s_expr, None, left_required, right_required)
            .await?;

        let left_schema = self.prepare_probe_schema(join_type, &left_side)?;
        let right_schema = self.prepare_build_schema(join_type, &right_side)?;

        let mut output_schema = Vec::clone(left_schema.fields());
        output_schema.extend_from_slice(right_schema.fields());

        let merged_schema = DataSchemaRefExt::create(
            left_schema
                .fields()
                .iter()
                .chain(right_schema.fields())
                .cloned()
                .collect::<Vec<_>>(),
        );

        Ok(PhysicalPlan::new(RangeJoin {
            left: left_side,
            right: right_side,
            meta: PhysicalPlanMeta::new("RangeJoin"),
            conditions: range_conditions
                .iter()
                .map(|scalar| {
                    resolve_range_condition(
                        scalar,
                        &left_schema,
                        &right_schema,
                        &left_prop,
                        &right_prop,
                    )
                })
                .collect::<Result<_>>()?,
            other_conditions: other_conditions
                .iter()
                .map(|scalar| resolve_scalar(scalar, &merged_schema))
                .collect::<Result<_>>()?,
            join_type,
            range_join_type,
            output_schema: Arc::new(DataSchema::new(output_schema)),
            stat_info: Some(self.build_plan_stat_info(s_expr)?),
        }))
    }
}

fn resolve_range_condition(
    expr: &ScalarExpr,
    left_schema: &DataSchemaRef,
    right_schema: &DataSchemaRef,
    left_prop: &RelationalProperty,
    right_prop: &RelationalProperty,
) -> Result<RangeJoinCondition> {
    match expr {
        ScalarExpr::FunctionCall(func) => {
            let mut left = None;
            let mut right = None;
            let mut opposite = false;
            let mut arg1 = func.arguments[0].clone();
            let mut arg2 = func.arguments[1].clone();
            // Try to find common type for left_expr/right_expr
            let arg1_data_type = arg1.data_type()?;
            let arg2_data_type = arg2.data_type()?;
            if arg1_data_type.ne(&arg2_data_type) {
                let cast_rules = &BUILTIN_FUNCTIONS.get_auto_cast_rules(&func.func_name);
                let common_type =
                    common_super_type(arg1_data_type.clone(), arg2_data_type.clone(), cast_rules)
                        .ok_or_else(|| {
                        ErrorCode::IllegalDataType(format!(
                            "Cannot find common type for {arg1_data_type} and {arg2_data_type}"
                        ))
                    })?;
                arg1 = wrap_cast(&arg1, &common_type);
                arg2 = wrap_cast(&arg2, &common_type);
            };
            for (idx, arg) in [arg1, arg2].iter().enumerate() {
                let join_predicate = JoinPredicate::new(arg, left_prop, right_prop);
                match join_predicate {
                    JoinPredicate::Left(_) => {
                        left = Some(arg.type_check(left_schema.as_ref())?.project_column_ref(
                            |index| left_schema.index_of(&index.to_string()),
                        )?);
                    }
                    JoinPredicate::Right(_) => {
                        if idx == 0 {
                            opposite = true;
                        }
                        right = Some(arg.type_check(right_schema.as_ref())?.project_column_ref(
                            |index| right_schema.index_of(&index.to_string()),
                        )?);
                    }
                    JoinPredicate::ALL(_)
                    | JoinPredicate::Both { .. }
                    | JoinPredicate::Other(_) => unreachable!(),
                }
            }
            let op = if opposite {
                match func.func_name.as_str() {
                    "gt" => "lt",
                    "lt" => "gt",
                    "gte" => "lte",
                    "lte" => "gte",
                    _ => unreachable!(),
                }
            } else {
                func.func_name.as_str()
            };
            Ok(RangeJoinCondition {
                left_expr: left.unwrap().as_remote_expr(),
                right_expr: right.unwrap().as_remote_expr(),
                operator: op.to_string(),
            })
        }
        _ => unreachable!(),
    }
}

fn resolve_scalar(scalar: &ScalarExpr, schema: &DataSchemaRef) -> Result<RemoteExpr> {
    let expr = scalar
        .type_check(schema.as_ref())?
        .project_column_ref(|index| schema.index_of(&index.to_string()))?;
    Ok(expr.as_remote_expr())
}
