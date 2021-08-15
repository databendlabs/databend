// Copyright 2020 Datafuse Labs.
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
//! Comparison operations on DataColumn.

use common_exception::Result;

use crate::prelude::*;
use crate::DataValueComparisonOperator;

macro_rules! apply_cmp {
    ($self: ident, $rhs: ident, $op: ident) => {{
        let lhs = $self.to_minimal_array()?;
        let rhs = $rhs.to_minimal_array()?;

        let result = lhs.$op(&rhs)?;
        let result: DataColumn = result.into_series().into();
        Ok(result.resize_constant($self.len()))
    }};
}

impl DataColumn {
    #[allow(unused)]
    pub fn compare(&self, op: DataValueComparisonOperator, rhs: &DataColumn) -> Result<DataColumn> {
        match op {
            DataValueComparisonOperator::Eq => apply_cmp! {self, rhs, eq},
            DataValueComparisonOperator::Lt => apply_cmp! {self, rhs, lt},
            DataValueComparisonOperator::LtEq => apply_cmp! {self, rhs, lt_eq},
            DataValueComparisonOperator::Gt => apply_cmp! {self, rhs, gt},
            DataValueComparisonOperator::GtEq => apply_cmp! {self, rhs, gt_eq},
            DataValueComparisonOperator::NotEq => apply_cmp! {self, rhs, neq},
            DataValueComparisonOperator::Like => apply_cmp! {self, rhs, like},
            DataValueComparisonOperator::NotLike => apply_cmp! {self, rhs, nlike},
        }
    }
}

impl PartialEq for &DataColumn {
    fn eq(&self, other: &Self) -> bool {
        let result = self.compare(DataValueComparisonOperator::Eq, other);
        match result {
            Ok(v) => v.to_array().unwrap().bool().unwrap().all_true(),
            Err(_) => false,
        }
    }
}
