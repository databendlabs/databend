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

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use itertools::Itertools;

use crate::filter::SelectExpr;
use crate::filter::SelectOp;
use crate::types::DataType;
use crate::EvalContext;
use crate::Evaluator;
use crate::Expr;
use crate::Scalar;
use crate::Value;

// SelectStrategy is used to determine the iteration strategy of the index.
// (1) True: iterate true index in `true_selection`.
// (2) False: iterate false index in `false_selection`.
// (3) All: iterate all index by Range.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SelectStrategy {
    True,
    False,
    All,
}

// The `Selector` is used to process the `SelectExpr`, it is used in `FilterExecutor`.
pub struct Selector<'a> {
    evaluator: Evaluator<'a>,
    num_rows: usize,
}

impl<'a> Selector<'a> {
    pub fn new(evaluator: Evaluator<'a>, num_rows: usize) -> Self {
        Self {
            evaluator,
            num_rows,
        }
    }

    // Process the `SelectExpr` and return the number of indices that are selected,
    // all selected indices are stored in `true_selection`.
    pub fn select(
        &self,
        select_expr: &SelectExpr,
        true_selection: &mut [u32],
        false_selection: &mut [u32],
    ) -> Result<usize> {
        let mut mutable_true_idx = 0;
        let mut mutable_false_idx = 0;
        self.process_select_expr(
            select_expr,
            None,
            true_selection,
            (false_selection, false),
            &mut mutable_true_idx,
            &mut mutable_false_idx,
            SelectStrategy::All,
            self.num_rows,
        )
    }

    // Process `SelectExpr`.
    #[allow(clippy::too_many_arguments)]
    fn process_select_expr(
        &self,
        select_expr: &SelectExpr,
        validity: Option<Bitmap>,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        mutable_true_idx: &mut usize,
        mutable_false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> Result<usize> {
        let count = match select_expr {
            SelectExpr::And(exprs) => self.process_and(
                exprs,
                validity,
                true_selection,
                false_selection,
                mutable_true_idx,
                mutable_false_idx,
                select_strategy,
                count,
            )?,
            SelectExpr::Or(exprs) => self.process_or(
                exprs,
                validity,
                true_selection,
                false_selection,
                mutable_true_idx,
                mutable_false_idx,
                select_strategy,
                count,
            )?,
            SelectExpr::Compare((select_op, exprs, generics)) => self.process_compare(
                select_op,
                exprs,
                generics,
                validity,
                true_selection,
                false_selection,
                mutable_true_idx,
                mutable_false_idx,
                select_strategy,
                count,
            )?,
            SelectExpr::Others(expr) => self.process_others(
                expr,
                validity,
                true_selection,
                false_selection,
                mutable_true_idx,
                mutable_false_idx,
                select_strategy,
                count,
            )?,
            SelectExpr::BooleanColumn((id, data_type)) => self.process_boolean_column(
                *id,
                data_type,
                true_selection,
                false_selection,
                mutable_true_idx,
                mutable_false_idx,
                select_strategy,
                count,
            )?,
            SelectExpr::BooleanScalar((constant, data_type)) => self.process_boolean_constant(
                constant.clone(),
                data_type,
                true_selection,
                false_selection,
                mutable_true_idx,
                mutable_false_idx,
                select_strategy,
                count,
            )?,
        };

        Ok(count)
    }

    // Process SelectExpr::And.
    #[allow(clippy::too_many_arguments)]
    fn process_and(
        &self,
        exprs: &Vec<SelectExpr>,
        validity: Option<Bitmap>,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        mutable_true_idx: &mut usize,
        mutable_false_idx: &mut usize,
        mut select_strategy: SelectStrategy,
        mut count: usize,
    ) -> Result<usize> {
        let mut temp_mutable_true_idx = *mutable_true_idx;
        let mut temp_mutable_false_idx = *mutable_false_idx;
        let exprs_len = exprs.len();
        for (i, expr) in exprs.iter().enumerate() {
            let true_count = self.process_select_expr(
                expr,
                validity.clone(),
                true_selection,
                (false_selection.0, false_selection.1),
                &mut temp_mutable_true_idx,
                &mut temp_mutable_false_idx,
                select_strategy,
                count,
            )?;
            if (true_count < count && select_strategy == SelectStrategy::All)
                || select_strategy == SelectStrategy::False
            {
                select_strategy = SelectStrategy::True;
            }
            count = true_count;
            if count == 0 {
                *mutable_true_idx = temp_mutable_true_idx;
                break;
            }
            if i != exprs_len - 1 {
                temp_mutable_true_idx = *mutable_true_idx;
            } else {
                *mutable_true_idx = temp_mutable_true_idx;
            }
        }
        *mutable_false_idx = temp_mutable_false_idx;
        Ok(count)
    }

    // Process SelectExpr::Or.
    #[allow(clippy::too_many_arguments)]
    fn process_or(
        &self,
        exprs: &Vec<SelectExpr>,
        validity: Option<Bitmap>,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        mutable_true_idx: &mut usize,
        mutable_false_idx: &mut usize,
        mut select_strategy: SelectStrategy,
        mut count: usize,
    ) -> Result<usize> {
        let mut temp_mutable_true_idx = *mutable_true_idx;
        let mut temp_mutable_false_idx = *mutable_false_idx;
        let exprs_len = exprs.len();
        for (i, expr) in exprs.iter().enumerate() {
            let true_count = self.process_select_expr(
                expr,
                validity.clone(),
                true_selection,
                (false_selection.0, true),
                &mut temp_mutable_true_idx,
                &mut temp_mutable_false_idx,
                select_strategy,
                count,
            )?;
            if (true_count > 0 && select_strategy == SelectStrategy::All)
                || select_strategy == SelectStrategy::True
            {
                select_strategy = SelectStrategy::False;
            }
            count -= true_count;
            if count == 0 {
                *mutable_false_idx = temp_mutable_false_idx;
                break;
            }
            if i != exprs_len - 1 {
                temp_mutable_false_idx = *mutable_false_idx;
            } else {
                *mutable_false_idx = temp_mutable_false_idx;
            }
        }
        let count = temp_mutable_true_idx - *mutable_true_idx;
        *mutable_true_idx = temp_mutable_true_idx;
        Ok(count)
    }

    // Process SelectExpr::Compare.
    #[allow(clippy::too_many_arguments)]
    fn process_compare(
        &self,
        select_op: &SelectOp,
        exprs: &[Expr],
        generics: &[DataType],
        validity: Option<Bitmap>,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        mutable_true_idx: &mut usize,
        mutable_false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> Result<usize> {
        let children = self.evaluator.get_children(exprs, validity)?;
        let (left_value, left_data_type) = children[0].clone();
        let (right_value, right_data_type) = children[1].clone();
        let left_data_type = self
            .evaluator
            .remove_generics_data_type(generics, &left_data_type);
        let right_data_type = self
            .evaluator
            .remove_generics_data_type(generics, &right_data_type);
        self.select_values(
            select_op,
            left_value,
            right_value,
            left_data_type,
            right_data_type,
            true_selection,
            false_selection,
            mutable_true_idx,
            mutable_false_idx,
            select_strategy,
            count,
        )
    }

    // Process SelectExpr::Others.
    #[allow(clippy::too_many_arguments)]
    fn process_others(
        &self,
        expr: &Expr,
        validity: Option<Bitmap>,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        mutable_true_idx: &mut usize,
        mutable_false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> Result<usize> {
        match expr {
            Expr::FunctionCall {
                function,
                args,
                generics,
                ..
            } if function.signature.name == "if" => {
                let result = self.evaluator.eval_if(args, generics, validity)?;
                let data_type = self
                    .evaluator
                    .remove_generics_data_type(generics, &function.signature.return_type);
                self.select_value(
                    result,
                    &data_type,
                    true_selection,
                    false_selection,
                    mutable_true_idx,
                    mutable_false_idx,
                    select_strategy,
                    count,
                )
            }
            Expr::FunctionCall {
                span,
                id,
                function,
                generics,
                args,
                return_type,
                ..
            } => {
                debug_assert!(
                    matches!(return_type, DataType::Boolean | DataType::Nullable(box DataType::Boolean))
                );
                let args = args
                    .iter()
                    .map(|expr| self.evaluator.partial_run(expr, validity.clone()))
                    .collect::<Result<Vec<_>>>()?;
                assert!(
                    args.iter()
                        .filter_map(|val| match val {
                            Value::Column(col) => Some(col.len()),
                            Value::Scalar(_) => None,
                        })
                        .all_equal()
                );
                let cols_ref = args.iter().map(Value::as_ref).collect::<Vec<_>>();
                let mut ctx = EvalContext {
                    generics,
                    num_rows: self.evaluator.data_block().num_rows(),
                    validity,
                    errors: None,
                    func_ctx: self.evaluator.func_ctx(),
                };
                let (_, eval) = function.eval.as_scalar().unwrap();
                let result = (eval)(cols_ref.as_slice(), &mut ctx);
                ctx.render_error(*span, id.params(), &args, &function.signature.name)?;
                let data_type = self
                    .evaluator
                    .remove_generics_data_type(generics, &function.signature.return_type);
                self.select_value(
                    result,
                    &data_type,
                    true_selection,
                    false_selection,
                    mutable_true_idx,
                    mutable_false_idx,
                    select_strategy,
                    count,
                )
            }
            Expr::Cast {
                span,
                is_try,
                expr,
                dest_type,
            } => {
                let value = self.evaluator.get_select_child(expr, validity.clone())?.0;
                let result = if *is_try {
                    self.evaluator
                        .run_try_cast(*span, expr.data_type(), dest_type, value)?
                } else {
                    self.evaluator
                        .run_cast(*span, expr.data_type(), dest_type, value, validity)?
                };
                self.select_value(
                    result,
                    dest_type,
                    true_selection,
                    false_selection,
                    mutable_true_idx,
                    mutable_false_idx,
                    select_strategy,
                    count,
                )
            }
            Expr::LambdaFunctionCall {
                name,
                args,
                lambda_expr,
                return_type,
                ..
            } => {
                let args = args
                    .iter()
                    .map(|expr| self.evaluator.partial_run(expr, validity.clone()))
                    .collect::<Result<Vec<_>>>()?;
                assert!(
                    args.iter()
                        .filter_map(|val| match val {
                            Value::Column(col) => Some(col.len()),
                            Value::Scalar(_) => None,
                        })
                        .all_equal()
                );
                let result = self.evaluator.run_lambda(name, args, lambda_expr)?;
                self.select_value(
                    result,
                    return_type,
                    true_selection,
                    false_selection,
                    mutable_true_idx,
                    mutable_false_idx,
                    select_strategy,
                    count,
                )
            }
            _ => Err(ErrorCode::UnsupportedDataType(format!(
                "Unsupported filter expression getting {expr}",
            ))),
        }
    }

    // Process SelectExpr::BooleanColumn.
    #[allow(clippy::too_many_arguments)]
    fn process_boolean_column(
        &self,
        id: usize,
        data_type: &DataType,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        mutable_true_idx: &mut usize,
        mutable_false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> Result<usize> {
        let column = self.evaluator.data_block().get_by_offset(id).value.clone();
        self.select_value(
            column,
            data_type,
            true_selection,
            false_selection,
            mutable_true_idx,
            mutable_false_idx,
            select_strategy,
            count,
        )
    }

    // Process SelectExpr::BooleanScalar.
    #[allow(clippy::too_many_arguments)]
    fn process_boolean_constant(
        &self,
        constant: Scalar,
        data_type: &DataType,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        mutable_true_idx: &mut usize,
        mutable_false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> Result<usize> {
        self.select_value(
            Value::Scalar(constant),
            data_type,
            true_selection,
            false_selection,
            mutable_true_idx,
            mutable_false_idx,
            select_strategy,
            count,
        )
    }
}
