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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::FunctionContext;
use databend_common_expression::RemoteExpr;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::executor::cast_expr_to_non_null_boolean;

pub fn filter_block(
    block: DataBlock,
    filter: &RemoteExpr,
    func_ctx: &FunctionContext,
) -> Result<DataBlock> {
    let filter = filter.as_expr(&BUILTIN_FUNCTIONS);
    let predicate = cast_expr_to_non_null_boolean(filter)?;
    assert_eq!(predicate.data_type(), &DataType::Boolean);

    let evaluator = Evaluator::new(&block, func_ctx, &BUILTIN_FUNCTIONS);
    let predicate = evaluator
        .run(&predicate)?
        .try_downcast::<BooleanType>()
        .unwrap();
    block.filter_boolean_value(&predicate)
}
