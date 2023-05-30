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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::type_check::common_super_type;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;
use common_expression::RemoteExpr;
use common_functions::BUILTIN_FUNCTIONS;

use crate::binder::wrap_cast;
use crate::binder::JoinPredicate;
use crate::executor::IEJoin;
use crate::executor::IEJoinCondition;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::SExpr;
use crate::optimizer::StatInfo;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::ScalarExpr;
use crate::TypeCheck;

impl PhysicalPlanBuilder {
    pub async fn build_ie_join(&mut self, join: &Join, s_expr: &SExpr) -> Result<PhysicalPlan> {
        let mut other_conditions = Vec::new();
        let mut ie_conditions = Vec::new();
        let left_prop = RelExpr::with_s_expr(s_expr.child(0)?).derive_relational_prop()?;
        let right_prop = RelExpr::with_s_expr(s_expr.child(1)?).derive_relational_prop()?;
        for condition in join.non_equi_conditions.iter() {
            if check_ie_join_condition(condition, &left_prop, &right_prop)
                && ie_conditions.len() < 2
            {
                ie_conditions.push(condition);
            } else {
                other_conditions.push(condition);
            }
        }
        // Construct IEJoin
        let left_side = self.build(s_expr.child(0)?).await?;
        let right_side = self.build(s_expr.child(1)?).await?;

        let left_schema = left_side.output_schema()?;
        let right_schema = right_side.output_schema()?;

        let merged_schema = DataSchemaRefExt::create(
            left_side
                .output_schema()?
                .fields()
                .iter()
                .chain(right_side.output_schema()?.fields())
                .cloned()
                .collect::<Vec<_>>(),
        );

        Ok(PhysicalPlan::IEJoin(IEJoin {
            plan_id: self.next_plan_id(),
            left: Box::new(left_side),
            right: Box::new(right_side),
            conditions: ie_conditions
                .iter()
                .map(|scalar| {
                    resolve_ie_scalar(scalar, &left_schema, &right_schema, &left_prop, &right_prop)
                })
                .collect::<Result<_>>()?,
            other_conditions: other_conditions
                .iter()
                .map(|scalar| resolve_scalar(scalar, &merged_schema))
                .collect::<Result<_>>()?,
            join_type: JoinType::Inner,
            stat_info: Some(self.build_plan_stat_info(s_expr)?),
        }))
    }
}

fn check_ie_join_condition(
    expr: &ScalarExpr,
    left_prop: &RelationalProperty,
    right_prop: &RelationalProperty,
) -> bool {
    if let ScalarExpr::FunctionCall(func) = expr {
        if matches!(func.func_name.as_str(), "gt" | "lt" | "gte" | "lte") {
            debug_assert_eq!(func.arguments.len(), 2);
            let mut left = false;
            let mut right = false;
            for arg in func.arguments.iter() {
                let join_predicate = JoinPredicate::new(arg, left_prop, right_prop);
                match join_predicate {
                    JoinPredicate::Left(_) => left = true,
                    JoinPredicate::Right(_) => right = true,
                    JoinPredicate::Both { .. } | JoinPredicate::Other(_) => {
                        return false;
                    }
                }
            }
            if left && right {
                return true;
            }
        }
    }
    false
}

fn resolve_ie_scalar(
    expr: &ScalarExpr,
    left_schema: &DataSchemaRef,
    right_schema: &DataSchemaRef,
    left_prop: &RelationalProperty,
    right_prop: &RelationalProperty,
) -> Result<IEJoinCondition> {
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
                let common_type = common_super_type(
                    arg1_data_type.clone(),
                    arg2_data_type.clone(),
                    &BUILTIN_FUNCTIONS.default_cast_rules,
                )
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
                        left = Some(
                            arg.resolve_and_check(left_schema.as_ref())?
                                .project_column_ref(|index| {
                                    left_schema.index_of(&index.to_string()).unwrap()
                                }),
                        );
                    }
                    JoinPredicate::Right(_) => {
                        if idx == 0 {
                            opposite = true;
                        }
                        right = Some(
                            arg.resolve_and_check(right_schema.as_ref())?
                                .project_column_ref(|index| {
                                    right_schema.index_of(&index.to_string()).unwrap()
                                }),
                        );
                    }
                    JoinPredicate::Both { .. } | JoinPredicate::Other(_) => unreachable!(),
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
            Ok(IEJoinCondition {
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
        .resolve_and_check(schema.as_ref())?
        .project_column_ref(|index| schema.index_of(&index.to_string()).unwrap());
    Ok(expr.as_remote_expr())
}
