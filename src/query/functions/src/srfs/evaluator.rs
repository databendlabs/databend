// Copyright 2022 Datafuse Labs.
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
use common_expression::types::AnyType;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::FunctionContext;
use common_expression::FunctionRegistry;
use common_expression::Value;

use crate::srfs::SrfExpr;

pub struct SrfEvaluator<'a> {
    input_columns: &'a DataBlock,
    func_ctx: FunctionContext,
    fn_registry: &'a FunctionRegistry,
}

impl<'a> SrfEvaluator<'a> {
    pub fn new(
        input_columns: &'a DataBlock,
        func_ctx: FunctionContext,
        fn_registry: &'a FunctionRegistry,
    ) -> Self {
        SrfEvaluator {
            input_columns,
            func_ctx,
            fn_registry,
        }
    }

    /// Evaluate an SRF. This will essentially returns a `DataBlock` for each row in the input `DataBlock`.
    pub fn run(&self, srf_expr: &SrfExpr) -> Result<Vec<(Vec<Value<AnyType>>, usize)>> {
        let evaluator = Evaluator::new(self.input_columns, self.func_ctx, self.fn_registry);
        let arg_results = srf_expr
            .args
            .iter()
            .map(|arg| evaluator.run(arg))
            .collect::<Result<Vec<_>>>()?;
        let arg_result_refs = arg_results
            .iter()
            .map(|arg| arg.as_ref())
            .collect::<Vec<_>>();

        let results = (0..self.input_columns.num_rows())
            .map(|i| {
                let args = arg_result_refs
                    .iter()
                    .map(|arg| arg.index(i).unwrap())
                    .collect::<Vec<_>>();
                let arg_types = srf_expr
                    .args
                    .iter()
                    .map(|arg| arg.data_type().clone())
                    .collect::<Vec<_>>();
                (*srf_expr.srf.eval)(&args, &arg_types)
            })
            .collect::<Vec<_>>();

        Ok(results)
    }
}
