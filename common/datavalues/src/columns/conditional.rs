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

use common_exception::Result;

use crate::columns::DataColumn;
use crate::prelude::*;

impl DataColumn {
    pub fn if_then_else(&self, lhs: &DataColumn, rhs: &DataColumn) -> Result<DataColumn> {
        let cond = self.to_minimal_array()?;

        let dtype = aggregate_types(&[lhs.data_type(), rhs.data_type()])?;
        let mut left = lhs.to_minimal_array()?;
        if left.data_type() != &dtype {
            left = left.cast_with_type(&dtype)?;
        }
        let mut right = rhs.to_minimal_array()?;
        if right.data_type() != &dtype {
            right = right.cast_with_type(&dtype)?;
        }

        let result = left.if_then_else(&right, &cond)?;
        let result: DataColumn = result.into();
        Ok(result.resize_constant(self.len()))
    }
}
