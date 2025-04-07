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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::binder::JoinPredicate;
use crate::executor::cast_expr_to_non_null_boolean;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::plans::ConstantExpr;
use crate::plans::Filter;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::ColumnSet;
use crate::MetadataRef;
use crate::ScalarExpr;
use crate::TypeCheck;

pub fn outer_join_to_inner_join(s_expr: &SExpr, metadata: MetadataRef) -> Result<(SExpr, bool)> {
    let mut join: Join = s_expr.child(0)?.plan().clone().try_into()?;
    if !join.join_type.is_outer_join() {
        return Ok((s_expr.clone(), false));
    }

    let filter: Filter = s_expr.plan().clone().try_into()?;
    let join_s_expr = s_expr.child(0)?;
    let join_rel_expr = RelExpr::with_s_expr(join_s_expr);

    let mut can_filter_left_null = false;
    let mut can_filter_right_null = false;
    let left_prop = join_rel_expr.derive_relational_prop_child(0)?;
    let right_prop = join_rel_expr.derive_relational_prop_child(1)?;
    for predicate in &filter.predicates {
        let pred = JoinPredicate::new(predicate, &left_prop, &right_prop);
        match pred {
            JoinPredicate::Left(_)
                if can_filter_null(
                    predicate,
                    &left_prop.output_columns,
                    &join.join_type,
                    metadata.clone(),
                )? =>
            {
                can_filter_left_null = true;
            }
            JoinPredicate::Right(_)
                if can_filter_null(
                    predicate,
                    &right_prop.output_columns,
                    &join.join_type,
                    metadata.clone(),
                )? =>
            {
                can_filter_right_null = true;
            }
            JoinPredicate::Both { .. } | JoinPredicate::Other(_) => {
                if can_filter_null(
                    predicate,
                    &left_prop.output_columns,
                    &join.join_type,
                    metadata.clone(),
                )? {
                    can_filter_left_null = true;
                }
                if can_filter_null(
                    predicate,
                    &right_prop.output_columns,
                    &join.join_type,
                    metadata.clone(),
                )? {
                    can_filter_right_null = true;
                }
            }
            _ => (),
        }
    }

    let original_join_type = join.join_type.clone();
    join.join_type =
        eliminate_outer_join_type(join.join_type, can_filter_left_null, can_filter_right_null);
    if join.join_type == original_join_type {
        return Ok((s_expr.clone(), false));
    }

    if matches!(
        original_join_type,
        JoinType::LeftSingle | JoinType::RightSingle
    ) {
        join.join_type = original_join_type.clone();
        join.single_to_inner = Some(original_join_type);
    }

    let result = SExpr::create_unary(
        Arc::new(filter.into()),
        Arc::new(SExpr::create_binary(
            Arc::new(join.into()),
            Arc::new(join_s_expr.child(0)?.clone()),
            Arc::new(join_s_expr.child(1)?.clone()),
        )),
    );

    Ok((result, true))
}

fn eliminate_outer_join_type(
    join_type: JoinType,
    can_filter_left_null: bool,
    can_filter_right_null: bool,
) -> JoinType {
    match join_type {
        JoinType::Left | JoinType::LeftSingle if can_filter_right_null => JoinType::Inner,
        JoinType::Right | JoinType::RightSingle if can_filter_left_null => JoinType::Inner,
        JoinType::Full => {
            if can_filter_left_null && can_filter_right_null {
                JoinType::Inner
            } else if can_filter_left_null {
                JoinType::Left
            } else if can_filter_right_null {
                JoinType::Right
            } else {
                join_type
            }
        }
        _ => join_type,
    }
}

pub fn can_filter_null(
    predicate: &ScalarExpr,
    columns_can_be_replaced: &ColumnSet,
    join_type: &JoinType,
    metadata: MetadataRef,
) -> Result<bool> {
    if !matches!(join_type, JoinType::Left | JoinType::Right | JoinType::Full) {
        return Ok(true);
    }

    struct ReplaceColumnBindingsNull<'a> {
        can_replace: bool,
        columns_can_be_replaced: &'a ColumnSet,
    }

    impl ReplaceColumnBindingsNull<'_> {
        fn replace(&mut self, expr: &mut ScalarExpr) -> Result<()> {
            if !self.can_replace {
                return Ok(());
            }
            match expr {
                ScalarExpr::BoundColumnRef(column_ref) => {
                    if self
                        .columns_can_be_replaced
                        .contains(&column_ref.column.index)
                    {
                        *expr = ScalarExpr::ConstantExpr(ConstantExpr {
                            span: None,
                            value: Scalar::Null,
                        });
                    }
                    Ok(())
                }
                ScalarExpr::FunctionCall(func) => {
                    // If the function is `assume_not_null` or `remove_nullable`, we cannot replace
                    // the column bindings with `Scalar::Null`.
                    if matches!(
                        func.func_name.as_str(),
                        "assume_not_null" | "remove_nullable"
                    ) {
                        self.can_replace = false;
                        return Ok(());
                    }

                    for expr in &mut func.arguments {
                        self.replace(expr)?;
                    }

                    Ok(())
                }
                ScalarExpr::CastExpr(cast) => self.replace(&mut cast.argument),
                ScalarExpr::ConstantExpr(_) => Ok(()),
                _ => {
                    self.can_replace = false;
                    Ok(())
                }
            }
        }
    }

    // Replace the column bindings of predicate with `Scalar::Null` and evaluate the result.
    let mut replace = ReplaceColumnBindingsNull {
        can_replace: true,
        columns_can_be_replaced,
    };
    let mut null_scalar_expr = predicate.clone();
    replace.replace(&mut null_scalar_expr).unwrap();
    if replace.can_replace {
        let columns = null_scalar_expr.columns_and_data_types(metadata);
        let expr = convert_scalar_expr_to_expr(null_scalar_expr, columns)?;
        let func_ctx = &FunctionContext::default();
        let (expr, _) = ConstantFolder::fold(&expr, func_ctx, &BUILTIN_FUNCTIONS);
        if expr.contains_column_ref() {
            return Ok(false);
        }
        let data_block = DataBlock::empty();
        let evaluator = Evaluator::new(&data_block, func_ctx, &BUILTIN_FUNCTIONS);
        if let Value::Scalar(scalar) = evaluator.run(&expr)? {
            // if null column can be filtered, return true.
            if matches!(scalar, Scalar::Boolean(false) | Scalar::Null) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

// Convert `ScalarExpr` to `Expr`.
fn convert_scalar_expr_to_expr(
    scalar_expr: ScalarExpr,
    columns: HashMap<usize, DataType>,
) -> Result<Expr> {
    let fields = columns
        .into_iter()
        .map(|(index, data_type)| DataField::new(index.to_string().as_str(), data_type))
        .collect();
    let schema = Arc::new(DataSchema::new(fields));
    let remote_expr = scalar_expr
        .type_check(schema.as_ref())?
        .project_column_ref(|index| schema.index_of(&index.to_string()).unwrap())
        .as_remote_expr();
    let expr = remote_expr.as_expr(&BUILTIN_FUNCTIONS);
    cast_expr_to_non_null_boolean(expr)
}
