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

use common_arrow::arrow::compute;
use common_exception::Result;

use crate::prelude::*;

impl DataColumn {
    pub fn is_null(&self) -> Result<DataColumn> {
        if self.data_type() == DataType::Null {
            return Ok(DFBooleanArray::full(true, self.len()).into_series().into());
        }

        let input = self.to_minimal_array()?;

        let result = DFBooleanArray::new(compute::boolean::is_null(input.get_array_ref().as_ref()));
        let result: DataColumn = result.into_series().into();
        Ok(result.resize_constant(self.len()))
    }

    pub fn is_not_null(&self) -> Result<DataColumn> {
        if self.data_type() == DataType::Null {
            return Ok(DFBooleanArray::full(false, self.len()).into_series().into());
        }

        let input = self.to_minimal_array()?;
        let result = DFBooleanArray::new(compute::boolean::is_not_null(
            input.get_array_ref().as_ref(),
        ));
        let result: DataColumn = result.into_series().into();
        Ok(result.resize_constant(self.len()))
    }
}
