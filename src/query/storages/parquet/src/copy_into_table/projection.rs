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

use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_functions::BUILTIN_FUNCTIONS;

pub(crate) struct CopyProjectionEvaluator {
    schema: DataSchemaRef,
    func_ctx: Arc<FunctionContext>,
}

impl CopyProjectionEvaluator {
    pub(crate) fn new(schema: DataSchemaRef, func_ctx: Arc<FunctionContext>) -> Self {
        Self { schema, func_ctx }
    }
    pub(crate) fn project(&self, block: &DataBlock, projection: &[Expr]) -> Result<DataBlock> {
        let evaluator = Evaluator::new(block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let mut entries = Vec::with_capacity(projection.len());
        let num_rows = block.num_rows();
        for (field, expr) in self.schema.fields().iter().zip(projection.iter()) {
            let entry = BlockEntry::from_value(evaluator.run(expr)?, || {
                (field.data_type().clone(), num_rows)
            });
            entries.push(entry);
        }
        Ok(DataBlock::new(entries, num_rows))
    }
}
