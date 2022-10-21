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

pub mod arithmetics_type;
pub mod arrow;
mod column_from;
pub mod date_helper;
pub mod display;

pub type Result<T> = std::result::Result<T, (crate::Span, String)>;

use chrono_tz::Tz;
use common_arrow::arrow::bitmap::Bitmap;

pub use self::column_from::*;
use crate::types::AnyType;
use crate::types::DataType;
use crate::Chunk;
use crate::Column;
use crate::Evaluator;
use crate::FunctionRegistry;
use crate::RawExpr;
use crate::Span;
use crate::Value;

/// A convenient shortcut to evaluate a scalar function.
pub fn eval_function(
    span: Span,
    fn_name: &str,
    args: impl Iterator<Item = (Value<AnyType>, DataType)>,
    tz: Tz,
    num_rows: usize,
    fn_registry: &FunctionRegistry,
) -> Result<(Value<AnyType>, DataType)> {
    let (args, cols) = args
        .enumerate()
        .map(|(id, (val, ty))| {
            (
                RawExpr::ColumnRef {
                    span: span.clone(),
                    id,
                    data_type: ty.clone(),
                },
                (val, ty),
            )
        })
        .unzip();
    let raw_expr = RawExpr::FunctionCall {
        span,
        name: fn_name.to_string(),
        params: vec![],
        args,
    };
    let (expr, ty) = crate::type_check::check(&raw_expr, fn_registry)?;
    let chunk = Chunk::new(cols, num_rows);
    let evaluator = Evaluator::new(&chunk, tz);
    Ok((evaluator.run(&expr)?, ty))
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

pub const fn concat_array<T, const A: usize, const B: usize>(a: &[T; A], b: &[T; B]) -> [T; A + B] {
    let mut result = std::mem::MaybeUninit::uninit();
    let dest = result.as_mut_ptr() as *mut T;
    unsafe {
        std::ptr::copy_nonoverlapping(a.as_ptr(), dest, A);
        std::ptr::copy_nonoverlapping(b.as_ptr(), dest.add(A), B);
        result.assume_init()
    }
}
