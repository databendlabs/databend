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

use std::collections::HashSet;

use databend_common_column::bitmap::MutableBitmap;

use crate::arrow::bitmap_into_mut;
use crate::types::BooleanType;
use crate::ColumnIndex;
use crate::Constant;
use crate::ConstantFolder;
use crate::Expr;
use crate::FunctionContext;
use crate::FunctionRegistry;
use crate::Scalar;
use crate::Value;

pub struct FilterHelpers;

impl FilterHelpers {
    #[inline]
    pub fn is_all_unset(predicate: &Value<BooleanType>) -> bool {
        match &predicate {
            Value::Scalar(v) => !v,
            Value::Column(bitmap) => bitmap.null_count() == bitmap.len(),
        }
    }

    pub fn filter_to_bitmap(predicate: Value<BooleanType>, rows: usize) -> MutableBitmap {
        match predicate {
            Value::Scalar(true) => MutableBitmap::from_len_set(rows),
            Value::Scalar(false) => MutableBitmap::from_len_zeroed(rows),
            Value::Column(bitmap) => bitmap_into_mut(bitmap),
        }
    }

    pub fn find_leveled_eq_filters<I: ColumnIndex>(
        expr: &Expr<I>,
        level_names: &[&str],
        func_ctx: &FunctionContext,
        fn_registry: &FunctionRegistry,
    ) -> databend_common_exception::Result<Vec<Vec<Scalar>>> {
        let mut scalars = vec![];
        for name in level_names {
            let mut values = Vec::new();
            expr.find_function_literals("eq", &mut |col_name, scalar, _| {
                if col_name.name() == *name {
                    values.push(scalar.clone());
                }
            });
            values.dedup();

            let mut results = Vec::with_capacity(values.len());
            let mut invalid_results = Vec::with_capacity(values.len());

            if !values.is_empty() {
                for value in values.iter() {
                    // replace eq with false, true
                    for (idx, t) in [false, true].iter().enumerate() {
                        let expr =
                            expr.replace_function_literals("eq", &mut |col_name, scalar, func| {
                                if col_name.name() == *name {
                                    if scalar == value {
                                        let data_type = func.function.signature.return_type.clone();
                                        Some(Expr::Constant(Constant {
                                            span: None,
                                            scalar: Scalar::Boolean(*t),
                                            data_type,
                                        }))
                                    } else {
                                        // for other values, we just ignore it
                                        None
                                    }
                                } else {
                                    // for other columns, we just ignore it
                                    None
                                }
                            });

                        let (folded_expr, _) = ConstantFolder::fold(&expr, func_ctx, fn_registry);

                        if let Expr::Constant(Constant {
                            scalar: Scalar::Boolean(false),
                            ..
                        }) = folded_expr
                        {
                            if idx == 0 {
                                results.push(value.clone());
                            } else {
                                // may contains negative values
                                invalid_results.push(value.clone());
                            }
                        }
                    }
                }

                values.retain(|v| !invalid_results.contains(v));
                if results.is_empty() && !values.is_empty() {
                    let mut results_all_used = true;
                    // let's check or function that
                    // for the equality columns set,let's call `ecs`
                    // if any side of `or` of the name columns is not in `ecs`, it's valid
                    // otherwise, it's invalid
                    expr.visit_func(&["or", "or_filters"], &mut |call| {
                        for arg in call.args.iter() {
                            let mut ecs = HashSet::new();
                            arg.find_function_literals("eq", &mut |col_name, _scalar, _| {
                                ecs.insert(col_name.name());
                            });

                            if !ecs.contains(*name) {
                                results_all_used = false;
                            }
                        }
                    });

                    if results_all_used {
                        results = values;
                    }
                }
                scalars.push(results);
            } else {
                scalars.push(vec![]);
            }
        }
        Ok(scalars)
    }
}
