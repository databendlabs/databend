// Copyright 2021 Datafuse Labs.
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

use common_arrow::arrow::bitmap::binary;
use common_arrow::arrow::bitmap::unary;
use common_datavalues::prelude::*;

use crate::scalars::FunctionContext;
pub trait BooleanSimdImpl: Sync + Send + Clone + 'static {
    fn vector_vector(lhs: &BooleanColumn, rhs: &BooleanColumn) -> BooleanColumn;

    fn vector_const(lhs: &BooleanColumn, rhs: bool) -> BooleanColumn;

    fn const_vector(lhs: bool, rhs: &BooleanColumn) -> BooleanColumn;
}

pub trait StringSearchImpl: Sync + Send + Clone + 'static {
    fn vector_vector(
        lhs: &StringColumn,
        rhs: &StringColumn,
        op: impl Fn(bool) -> bool,
    ) -> BooleanColumn;

    fn vector_const(lhs: &StringColumn, rhs: &[u8], op: impl Fn(bool) -> bool) -> BooleanColumn;
}

pub(crate) struct CommonBooleanOp;

impl CommonBooleanOp {
    /// QUOTE: (From common_arrow::arrow::compute::comparison::boolean)
    pub(crate) fn compare_op<F>(lhs: &BooleanColumn, rhs: &BooleanColumn, op: F) -> BooleanColumn
    where F: Fn(u64, u64) -> u64 {
        let values = binary(lhs.values(), rhs.values(), op);
        BooleanColumn::from_arrow_data(values)
    }

    pub(crate) fn compare_op_scalar<F>(lhs: &BooleanColumn, rhs: bool, op: F) -> BooleanColumn
    where F: Fn(u64, u64) -> u64 {
        let rhs = if rhs { !0 } else { 0 };

        let values = unary(lhs.values(), |x| op(x, rhs));
        BooleanColumn::from_arrow_data(values)
    }
}
