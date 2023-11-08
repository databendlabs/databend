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
use common_exception::Result;
use common_expression::eval_function;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::BlockEntry;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::Value;
use common_functions::BUILTIN_FUNCTIONS;
use common_sql::executor::cast_expr_to_non_null_boolean;
pub struct DeleteByExprMutator {
    expr: Option<Expr>,
    row_id_idx: usize,
    func_ctx: FunctionContext,
    origin_input_columns: usize,
}

impl DeleteByExprMutator {
    pub fn create(
        expr: Option<Expr>,
        func_ctx: FunctionContext,
        row_id_idx: usize,
        origin_input_columns: usize,
    ) -> Self {
        Self {
            expr,
            row_id_idx,
            func_ctx,
            origin_input_columns,
        }
    }

    pub fn delete_by_expr(&self, data_block: DataBlock) -> Result<(DataBlock, DataBlock)> {
        // it's the first update, after update, we need to add a filter column
        if data_block.num_columns() == self.origin_input_columns {
            // assert_eq!(expr.data_type(), &DataType::Boolean);
            self.delete_block(data_block, false)
        } else {
            self.delete_block(data_block, true)
        }
    }

    fn get_filter(
        &self,
        data_block: &DataBlock,
    ) -> Result<(Value<BooleanType>, Value<BooleanType>)> {
        // filter rows that's is not processed
        let filter_entry = data_block.get_by_offset(data_block.num_columns() - 1);
        let old_filter: Value<BooleanType> = filter_entry.value.try_downcast().unwrap();
        // false means this row is not processed, so use `not` to reverse it.
        let (filter_not, _) = eval_function(
            None,
            "not",
            [(old_filter.clone().upcast(), DataType::Boolean)],
            &self.func_ctx,
            data_block.num_rows(),
            &BUILTIN_FUNCTIONS,
        )?;
        let filter_not = filter_not.try_downcast().unwrap();
        Ok((old_filter, filter_not))
    }

    // return block after delete, and the rowIds which are deleted
    pub fn delete_block(
        &self,
        data_block: DataBlock,
        has_filter: bool,
    ) -> Result<(DataBlock, DataBlock)> {
        if self.expr.is_none() {
            if has_filter {
                let (old_predicate, filter_not) = self.get_filter(&data_block)?;
                let block_after_delete = data_block.clone().filter_boolean_value(&old_predicate)?;
                let block_delete_part = data_block.clone().filter_boolean_value(&filter_not)?;
                Ok((
                    block_after_delete,
                    DataBlock::new(
                        vec![block_delete_part.get_by_offset(self.row_id_idx).clone()],
                        block_delete_part.num_rows(),
                    ),
                ))
            } else {
                // delete all
                Ok((
                    DataBlock::empty(),
                    DataBlock::new(
                        vec![data_block.get_by_offset(self.row_id_idx).clone()],
                        data_block.num_rows(),
                    ),
                ))
            }
        } else {
            let filter: Expr = cast_expr_to_non_null_boolean(self.expr.as_ref().unwrap().clone())?;
            assert_eq!(filter.data_type(), &DataType::Boolean);

            let evaluator = Evaluator::new(&data_block, &self.func_ctx, &BUILTIN_FUNCTIONS);

            let predicates = evaluator
                .run(&filter)
                .map_err(|e| e.add_message("eval filter failed:"))?
                .try_downcast::<BooleanType>()
                .unwrap();

            // for delete, we don't need to add new filter to the result block,
            // just filter it.
            if has_filter {
                let (_, filter_not) = self.get_filter(&data_block)?;

                let (res, _) = eval_function(
                    None,
                    "and",
                    [
                        (filter_not.upcast(), DataType::Boolean),
                        (predicates.upcast(), DataType::Boolean),
                    ],
                    &self.func_ctx,
                    data_block.num_rows(),
                    &BUILTIN_FUNCTIONS,
                )?;

                let res: Value<BooleanType> = res.try_downcast().unwrap();
                let (res_not, _) = eval_function(
                    None,
                    "not",
                    [(res.clone().upcast(), DataType::Boolean)],
                    &self.func_ctx,
                    data_block.num_rows(),
                    &BUILTIN_FUNCTIONS,
                )?;
                let filtered_block = data_block.clone().filter_boolean_value(&res)?;
                Ok((
                    data_block.filter_boolean_value(&res_not.try_downcast().unwrap())?,
                    DataBlock::new(
                        vec![filtered_block.get_by_offset(self.row_id_idx).clone()],
                        filtered_block.num_rows(),
                    ),
                ))
            } else {
                let filtered_block = data_block.clone().filter_boolean_value(&predicates)?;
                let (predicates_not, _) = eval_function(
                    None,
                    "not",
                    [(predicates.clone().upcast(), DataType::Boolean)],
                    &self.func_ctx,
                    data_block.num_rows(),
                    &BUILTIN_FUNCTIONS,
                )?;
                // we need to add filter at the end of result block
                let mut res_block = data_block
                    .clone()
                    .filter_boolean_value(&predicates_not.try_downcast().unwrap())?;

                let const_expr = Expr::Constant {
                    span: None,
                    scalar: common_expression::Scalar::Boolean(false),
                    data_type: DataType::Boolean,
                };

                let const_predicates = evaluator
                    .run(&const_expr)
                    .map_err(|e| e.add_message("eval filter failed:"))?
                    .try_downcast::<BooleanType>()
                    .unwrap();

                res_block.add_column(BlockEntry::new(
                    DataType::Boolean,
                    Value::upcast(const_predicates),
                ));

                Ok((
                    res_block,
                    DataBlock::new(
                        vec![filtered_block.get_by_offset(self.row_id_idx).clone()],
                        filtered_block.num_rows(),
                    ),
                ))
            }
        }
    }
}
