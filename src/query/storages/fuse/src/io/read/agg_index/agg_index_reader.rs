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

use databend_common_catalog::plan::AggIndexInfo;
use databend_common_catalog::plan::AggIndexMeta;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_table_meta::table::TableCompression;
use opendal::Operator;

use crate::io::BlockReader;

#[derive(Clone)]
pub struct AggIndexReader {
    index_id: u64,

    pub(super) reader: Arc<BlockReader>,
    pub(super) compression: TableCompression,
    pub(crate) ctx: Arc<dyn TableContext>,

    func_ctx: FunctionContext,
    selection: Vec<(Expr, Option<usize>)>,
    filter: Option<Expr>,

    /// The size of the output fields of a table scan plan without the index.
    actual_table_field_len: usize,
    // If the index is the result of an aggregation query.
    is_agg: bool,
    num_agg_funcs: usize,
}

impl AggIndexReader {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        dal: Operator,
        agg: &AggIndexInfo,
        compression: TableCompression,
        put_cache: bool,
    ) -> Result<Self> {
        let reader = BlockReader::create(
            ctx.clone(),
            dal,
            agg.schema.clone(),
            agg.projection.clone(),
            false,
            false,
            false,
            put_cache,
        )?;

        let func_ctx = ctx.get_function_context()?;
        let selection = agg
            .selection
            .iter()
            .map(|(sel, offset)| (sel.as_expr(&BUILTIN_FUNCTIONS), *offset))
            .collect();
        let filter = agg.filter.as_ref().map(|f| f.as_expr(&BUILTIN_FUNCTIONS));

        Ok(Self {
            index_id: agg.index_id,
            reader,
            ctx,
            func_ctx,
            selection,
            filter,
            actual_table_field_len: agg.actual_table_field_len,
            is_agg: agg.is_agg,
            compression,
            num_agg_funcs: agg.num_agg_funcs,
        })
    }

    #[inline(always)]
    pub fn index_id(&self) -> u64 {
        self.index_id
    }

    pub(super) fn apply_agg_info(&self, block: DataBlock) -> Result<DataBlock> {
        let evaluator = Evaluator::new(&block, &self.func_ctx, &BUILTIN_FUNCTIONS);

        // 1. Filter the block if there is a filter.
        let block = if let Some(filter) = self.filter.as_ref() {
            let filter = evaluator
                .run(filter)?
                .try_downcast::<BooleanType>()
                .unwrap();
            block.filter_boolean_value(&filter)?
        } else {
            block
        };

        // 2. Compute the output block
        // Fill dummy columns first.
        let mut output_columns = vec![
            BlockEntry {
                data_type: DataType::Null,
                value: Value::Scalar(Scalar::Null),
            };
            self.actual_table_field_len
        ];
        let evaluator = Evaluator::new(&block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        for (expr, offset) in self.selection.iter() {
            let data_type = expr.data_type().clone();
            let value = evaluator.run(expr)?;
            let col = BlockEntry { data_type, value };

            if let Some(pos) = offset {
                output_columns[*pos] = col;
            } else {
                output_columns.push(col);
            }
        }

        let num_evals = output_columns.len() - self.actual_table_field_len;

        Ok(DataBlock::new_with_meta(
            output_columns,
            block.num_rows(),
            Some(AggIndexMeta::create(
                self.is_agg,
                num_evals,
                self.num_agg_funcs,
            )),
        ))
    }
}
