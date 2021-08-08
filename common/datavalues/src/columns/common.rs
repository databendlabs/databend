// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

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
