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
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_functions::BUILTIN_FUNCTIONS;
use common_sql::executor::cast_expr_to_non_null_boolean;

pub struct SplitByExprMutator {
    expr: Option<Expr>,
    func_ctx: FunctionContext,
}

impl SplitByExprMutator {
    pub fn create(expr: Option<Expr>, func_ctx: FunctionContext) -> Self {
        Self { expr, func_ctx }
    }

    // first datablock satisfy expr, the second doesn't
    pub fn split_by_expr(&self, data_block: DataBlock) -> Result<(DataBlock, DataBlock)> {
        if self.expr.is_none() {
            Ok((data_block, DataBlock::empty()))
        } else {
            let filter: Expr = cast_expr_to_non_null_boolean(self.expr.as_ref().unwrap().clone())?;
            assert_eq!(filter.data_type(), &DataType::Boolean);

            let evaluator = Evaluator::new(&data_block, &self.func_ctx, &BUILTIN_FUNCTIONS);

            let predicates = evaluator
                .run(&filter)
                .map_err(|e| e.add_message("eval filter failed:"))?
                .try_downcast::<BooleanType>()
                .unwrap();
            let (predicates_not, _) = eval_function(
                None,
                "not",
                [(predicates.clone().upcast(), DataType::Boolean)],
                &self.func_ctx,
                data_block.num_rows(),
                &BUILTIN_FUNCTIONS,
            )?;
            Ok((
                data_block.clone().filter_boolean_value(&predicates)?,
                data_block.filter_boolean_value(&predicates_not.try_downcast().unwrap())?,
            ))
        }
    }
}
