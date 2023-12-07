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

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::EvalContext;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::Scalar;
use common_expression::Value;
use itertools::Itertools;

use crate::pipelines::processors::transforms::filter::select_value;
use crate::pipelines::processors::transforms::filter::select_values;
use crate::pipelines::processors::transforms::filter::SelectExpr;
use crate::pipelines::processors::transforms::filter::SelectOp;
use crate::pipelines::processors::transforms::filter::SelectStrategy;

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

    pub fn select(
        &self,
        select_expr: &SelectExpr,
        true_selection: &mut [u32],
        false_selection: &mut [u32],
    ) -> Result<usize> {
        let mut true_idx = 0;
        let mut false_idx = 0;
        self.process_selection(
            select_expr,
            None,
            true_selection,
            (false_selection, false),
            &mut true_idx,
            &mut false_idx,
            SelectStrategy::All,
            self.num_rows,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn process_selection(
        &self,
        select_expr: &SelectExpr,
        validity: Option<Bitmap>,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        true_idx: &mut usize,
        false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> Result<usize> {
        let count = match select_expr {
            SelectExpr::And(exprs) => self.process_and(
                exprs,
                validity,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )?,
            SelectExpr::Or(exprs) => self.process_or(
                exprs,
                validity,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
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
                true_idx,
                false_idx,
                select_strategy,
                count,
            )?,
            SelectExpr::Others(expr) => self.process_others(
                expr,
                validity,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )?,
            SelectExpr::BooleanColumn((id, data_type)) => self.process_boolean_column(
                *id,
                data_type,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )?,
            SelectExpr::BooleanScalar((constant, data_type)) => self.process_boolean_constant(
                constant.clone(),
                data_type,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            )?,
        };

        Ok(count)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn process_and(
        &self,
        exprs: &Vec<SelectExpr>,
        validity: Option<Bitmap>,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        true_idx: &mut usize,
        false_idx: &mut usize,
        mut select_strategy: SelectStrategy,
        mut count: usize,
    ) -> Result<usize> {
        let mut temp_true_idx = *true_idx;
        let mut temp_false_idx = *false_idx;
        let exprs_len = exprs.len();
        for (i, expr) in exprs.iter().enumerate() {
            let true_count = self.process_selection(
                expr,
                validity.clone(),
                true_selection,
                (false_selection.0, false_selection.1),
                &mut temp_true_idx,
                &mut temp_false_idx,
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
                *true_idx = temp_true_idx;
                break;
            }
            if i != exprs_len - 1 {
                temp_true_idx = *true_idx;
            } else {
                *true_idx = temp_true_idx;
            }
        }
        *false_idx = temp_false_idx;
        Ok(count)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn process_or(
        &self,
        exprs: &Vec<SelectExpr>,
        validity: Option<Bitmap>,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        true_idx: &mut usize,
        false_idx: &mut usize,
        mut select_strategy: SelectStrategy,
        mut count: usize,
    ) -> Result<usize> {
        let mut temp_true_idx = *true_idx;
        let mut temp_false_idx = *false_idx;
        let exprs_len = exprs.len();
        for (i, expr) in exprs.iter().enumerate() {
            let true_count = self.process_selection(
                expr,
                validity.clone(),
                true_selection,
                (false_selection.0, true),
                &mut temp_true_idx,
                &mut temp_false_idx,
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
                *false_idx = temp_false_idx;
                break;
            }
            if i != exprs_len - 1 {
                temp_false_idx = *false_idx;
            } else {
                *false_idx = temp_false_idx;
            }
        }
        let count = temp_true_idx - *true_idx;
        *true_idx = temp_true_idx;
        Ok(count)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn process_compare(
        &self,
        select_op: &SelectOp,
        exprs: &[Expr],
        generics: &[DataType],
        validity: Option<Bitmap>,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        true_idx: &mut usize,
        false_idx: &mut usize,
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
        let count = select_values(
            *select_op,
            left_value,
            right_value,
            left_data_type,
            right_data_type,
            true_selection,
            false_selection,
            true_idx,
            false_idx,
            select_strategy,
            count,
        );
        Ok(count)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn process_others(
        &self,
        expr: &Expr,
        validity: Option<Bitmap>,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        true_idx: &mut usize,
        false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> Result<usize> {
        let count = match expr {
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
                select_value(
                    result,
                    &data_type,
                    true_selection,
                    false_selection,
                    true_idx,
                    false_idx,
                    select_strategy,
                    count,
                )
            }
            Expr::FunctionCall {
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
                select_value(
                    result,
                    return_type,
                    true_selection,
                    false_selection,
                    true_idx,
                    false_idx,
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
                select_value(
                    result,
                    dest_type,
                    true_selection,
                    false_selection,
                    true_idx,
                    false_idx,
                    select_strategy,
                    count,
                )
            }
            _ => unreachable!("expr: {expr}"),
        };
        Ok(count)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn process_boolean_column(
        &self,
        id: usize,
        data_type: &DataType,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        true_idx: &mut usize,
        false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> Result<usize> {
        let column = self.evaluator.data_block().get_by_offset(id).value.clone();
        let count = select_value(
            column,
            data_type,
            true_selection,
            false_selection,
            true_idx,
            false_idx,
            select_strategy,
            count,
        );
        Ok(count)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn process_boolean_constant(
        &self,
        constant: Scalar,
        data_type: &DataType,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        true_idx: &mut usize,
        false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> Result<usize> {
        let count = select_value(
            Value::Scalar(constant),
            data_type,
            true_selection,
            false_selection,
            true_idx,
            false_idx,
            select_strategy,
            count,
        );
        Ok(count)
    }
}
