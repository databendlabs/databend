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

use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::common_super_type;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::binder::wrap_cast;
use crate::binder::JoinPredicate;
use crate::executor::explain::PlanStatsInfo;
use crate::executor::{IPhysicalPlan, PhysicalPlan, PhysicalPlanMeta};
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::SExpr;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::ColumnSet;
use crate::executor::physical_plan::PhysicalPlanDeriveHandle;
use crate::ScalarExpr;
use crate::TypeCheck;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RangeJoin {
    pub meta: PhysicalPlanMeta,
    pub left: Box<dyn IPhysicalPlan>,
    pub right: Box<dyn IPhysicalPlan>,
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
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.output_schema.clone())
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item=&'a Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&self.left).chain(std::iter::once(&self.right)))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item=&'a mut Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&mut self.left).chain(std::iter::once(&mut self.right)))
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

    fn derive_with(&self, handle: &mut Box<dyn PhysicalPlanDeriveHandle>) -> Box<dyn IPhysicalPlan> {
        let derive_left = self.left.derive_with(handle);
        let derive_right = self.right.derive_with(handle);

        match handle.derive(self, vec![derive_left, derive_right]) {
            Ok(v) => v,
            Err(children) => {
                let mut new_range_join = self.clone();
                assert_eq!(children.len(), 2);
                new_range_join.left = children[0];
                new_range_join.right = children[1];
                Box::new(new_range_join)
            }
        }
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
        join: &Join,
        s_expr: &SExpr,
        left_required: ColumnSet,
        right_required: ColumnSet,
        mut range_conditions: Vec<ScalarExpr>,
        mut other_conditions: Vec<ScalarExpr>,
    ) -> Result<Box<dyn IPhysicalPlan>> {
        let left_prop = RelExpr::with_s_expr(s_expr.child(1)?).derive_relational_prop()?;
        let right_prop = RelExpr::with_s_expr(s_expr.child(0)?).derive_relational_prop()?;

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
            .build_join_sides(s_expr, left_required, right_required)
            .await?;

        let left_schema = self.prepare_probe_schema(&join.join_type, &left_side)?;
        let right_schema = self.prepare_build_schema(&join.join_type, &right_side)?;

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

        Ok(Box::new(RangeJoin {
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
            join_type: join.join_type.clone(),
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
                            |index| left_schema.index_of(&index.to_string()).unwrap(),
                        ));
                    }
                    JoinPredicate::Right(_) => {
                        if idx == 0 {
                            opposite = true;
                        }
                        right = Some(arg.type_check(right_schema.as_ref())?.project_column_ref(
                            |index| right_schema.index_of(&index.to_string()).unwrap(),
                        ));
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
        .project_column_ref(|index| schema.index_of(&index.to_string()).unwrap());
    Ok(expr.as_remote_expr())
}
