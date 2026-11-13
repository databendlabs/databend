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

//! Evaluates and compares declared sort keys, ascending with NULLs last, which is the order a
//! linear cluster key sorts by.
//!
//! Only the first and last rows of each block are checked, which makes the check cheap and
//! repeatable. Disorder between them goes unnoticed, but it only widens the range of one block
//! on write, because blocks are sorted again and their statistics recomputed there.

use std::cmp::Ordering;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::Cast;
use crate::Column;
use crate::ColumnRef;
use crate::DataBlock;
use crate::DataSchema;
use crate::Evaluator;
use crate::Expr;
use crate::FunctionContext;
use crate::RemoteExpr;
use crate::Scalar;
use crate::function::FunctionRegistry;

/// The sort key values of one row.
pub type SortKeyRow = Vec<Scalar>;

/// Evaluates declared sort keys, bound to target table columns by name, on blocks of a
/// source schema whose column types may differ from the table's.
#[derive(Clone)]
pub struct SortKeyEvaluator {
    /// Expressions over the source block that produce the key inputs in the table types.
    inputs: Vec<Expr>,
    /// The sort keys, over the key input block.
    keys: Vec<Expr>,
    func_ctx: FunctionContext,
}

impl SortKeyEvaluator {
    pub fn try_create(
        keys: &[RemoteExpr<String>],
        source_schema: &DataSchema,
        func_ctx: FunctionContext,
        registry: &FunctionRegistry,
    ) -> Result<Self> {
        let mut input_names: Vec<String> = Vec::new();
        let mut inputs = Vec::new();
        let mut key_exprs = Vec::with_capacity(keys.len());
        for key in keys {
            let expr = key.as_expr(registry);
            let mut column_refs = expr.column_refs().into_iter().collect::<Vec<_>>();
            column_refs.sort_by(|a, b| a.0.cmp(&b.0));
            for (name, data_type) in column_refs {
                if input_names.contains(&name) {
                    continue;
                }
                let offset = find_field(source_schema, &name)?;
                let source_type = source_schema.field(offset).data_type().clone();
                let column = Expr::ColumnRef(ColumnRef {
                    span: None,
                    id: offset,
                    data_type: source_type.clone(),
                    display_name: name.clone(),
                });
                let input = if source_type == data_type {
                    column
                } else {
                    Expr::Cast(Cast {
                        span: None,
                        is_try: false,
                        expr: Box::new(column),
                        dest_type: data_type,
                    })
                };
                input_names.push(name);
                inputs.push(input);
            }
            key_exprs.push(expr.project_column_ref(|name| {
                Ok(input_names
                    .iter()
                    .position(|input| input == name)
                    .expect("column ref must be collected"))
            })?);
        }
        Ok(Self {
            inputs,
            keys: key_exprs,
            func_ctx,
        })
    }

    pub fn num_keys(&self) -> usize {
        self.keys.len()
    }

    /// Returns the sort keys of the first and the last rows of `block`, if it has rows.
    pub fn boundary_keys(
        &self,
        block: &DataBlock,
        registry: &FunctionRegistry,
    ) -> Result<Option<(SortKeyRow, SortKeyRow)>> {
        let num_rows = block.num_rows();
        if num_rows == 0 {
            return Ok(None);
        }
        let rows = [0, (num_rows - 1) as u32];
        let keys = self.eval(&block.take(rows.as_slice())?, registry)?;
        Ok(Some((sort_key_row(&keys, 0), sort_key_row(&keys, 1))))
    }

    /// Evaluates the sort key columns of `block`.
    pub fn eval(&self, block: &DataBlock, registry: &FunctionRegistry) -> Result<Vec<Column>> {
        let num_rows = block.num_rows();
        let evaluator = Evaluator::new(block, &self.func_ctx, registry);
        let mut entries = Vec::with_capacity(self.inputs.len());
        for input in &self.inputs {
            let value = evaluator.run(input)?;
            entries.push(value.into_full_column(input.data_type(), num_rows).into());
        }
        let input_block = DataBlock::new(entries, num_rows);
        let evaluator = Evaluator::new(&input_block, &self.func_ctx, registry);
        self.keys
            .iter()
            .map(|key| {
                let value = evaluator.run(key)?;
                Ok(value.into_full_column(key.data_type(), num_rows))
            })
            .collect()
    }
}

fn find_field(schema: &DataSchema, name: &str) -> Result<usize> {
    let fields = schema.fields();
    fields
        .iter()
        .position(|field| field.name() == name)
        .or_else(|| {
            fields
                .iter()
                .position(|field| field.name().eq_ignore_ascii_case(name))
        })
        .ok_or_else(|| {
            ErrorCode::Internal(format!(
                "sort key column `{name}` is missing from the input schema"
            ))
        })
}

/// Compares two key rows, ascending with NULLs last.
pub fn compare_sort_key_rows(left: &[Scalar], right: &[Scalar]) -> Ordering {
    for (l, r) in left.iter().zip(right.iter()) {
        let ordering = l.as_ref().cmp(&r.as_ref());
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

/// Returns the key values of `row`.
pub fn sort_key_row(keys: &[Column], row: usize) -> SortKeyRow {
    keys.iter()
        .map(|column| {
            column
                .index(row)
                .map(|scalar| scalar.to_owned())
                .unwrap_or(Scalar::Null)
        })
        .collect()
}

/// Formats a key row for error messages.
pub fn display_sort_key_row(row: &[Scalar]) -> String {
    let values = row
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>();
    format!("({})", values.join(", "))
}
