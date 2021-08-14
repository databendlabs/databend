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
use crate::DataValueLogicOperator;

macro_rules! apply_logic {
    ($self: ident, $rhs: ident, $op: ident) => {{
        let lhs = $self.to_minimal_array()?;

        let left = lhs.cast_with_type(&DataType::Boolean)?;
        let left = left.bool()?;

        let rhs = $rhs[0].to_minimal_array()?;
        let right = rhs.cast_with_type(&DataType::Boolean)?;
        let right = right.bool()?;

        let result = left.$op(&right)?;
        let result: DataColumn = result.into_series().into();
        Ok(result.resize_constant($self.len()))
    }};

    ($self: ident, $op: ident) => {{
        let lhs = $self.to_minimal_array()?;
        let left = lhs.cast_with_type(&DataType::Boolean)?;
        let left = left.bool()?;

        let result = left.$op()?;
        let result: DataColumn = result.into_series().into();
        Ok(result.resize_constant($self.len()))
    }};
}

impl DataColumn {
    #[allow(unused)]
    pub fn logic(&self, op: DataValueLogicOperator, rhs: &[DataColumn]) -> Result<DataColumn> {
        match op {
            DataValueLogicOperator::And => apply_logic! {self, rhs, and_kleene},
            DataValueLogicOperator::Or => apply_logic! {self, rhs, or_kleene},
            DataValueLogicOperator::Not => apply_logic! {self, not},
        }
    }
}
