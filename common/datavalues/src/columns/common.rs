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

use std::sync::Arc;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::compute::concat;
use common_exception::Result;

use crate::prelude::*;

pub struct DataColumnCommon;

impl DataColumnCommon {
    pub fn concat(columns: &[DataColumn]) -> Result<DataColumn> {
        let arrays = columns
            .iter()
            .map(|s| s.get_array_ref())
            .collect::<Result<Vec<_>>>()?;

        let dyn_arrays: Vec<&dyn Array> = arrays.iter().map(|arr| arr.as_ref()).collect();

        let array: Arc<dyn Array> = Arc::from(concat::concatenate(&dyn_arrays)?);
        Ok(array.into())
    }
}
