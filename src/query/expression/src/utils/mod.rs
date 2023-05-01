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

pub mod arithmetics_type;
pub mod arrow;
pub mod block_debug;
pub mod block_thresholds;
mod column_from;
pub mod date_helper;
pub mod display;
pub mod filter_helper;
pub mod serialize;

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::Result;
use common_exception::Span;

pub use self::column_from::*;
use crate::types::AnyType;
use crate::types::DataType;
use crate::BlockEntry;
use crate::Column;
use crate::DataBlock;
use crate::Evaluator;
use crate::FunctionContext;
use crate::FunctionRegistry;
use crate::RawExpr;
use crate::Value;

/// A convenient shortcut to evaluate a scalar function.
pub fn eval_function(
    span: Span,
    fn_name: &str,
    args: impl IntoIterator<Item = (Value<AnyType>, DataType)>,
    func_ctx: &FunctionContext,
    num_rows: usize,
    fn_registry: &FunctionRegistry,
) -> Result<(Value<AnyType>, DataType)> {
    let (args, cols) = args
        .into_iter()
        .enumerate()
        .map(|(id, (val, ty))| {
            (
                RawExpr::ColumnRef {
                    span,
                    id,
                    data_type: ty.clone(),
                    display_name: String::new(),
                },
                BlockEntry {
                    data_type: ty,
                    value: val,
                },
            )
        })
        .unzip();
    let raw_expr = RawExpr::FunctionCall {
        span,
        name: fn_name.to_string(),
        params: vec![],
        args,
    };
    let expr = crate::type_check::check(&raw_expr, fn_registry)?;
    let block = DataBlock::new(cols, num_rows);
    let evaluator = Evaluator::new(&block, func_ctx, fn_registry);
    Ok((evaluator.run(&expr)?, expr.data_type().clone()))
}

pub fn column_merge_validity(column: &Column, bitmap: Option<Bitmap>) -> Option<Bitmap> {
    match column {
        Column::Nullable(c) => match bitmap {
            None => Some(c.validity.clone()),
            Some(v) => Some(&c.validity & (&v)),
        },
        _ => bitmap,
    }
}
