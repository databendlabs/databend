// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::compute;
use common_exception::Result;

use super::IntoSeries;
use super::Series;
pub struct SeriesHelper;

impl SeriesHelper {
    pub fn concat(series: &[Series]) -> Result<Series> {
        let arrays: Vec<ArrayRef> = series.iter().map(|s| s.get_array_ref()).collect();
        let dyn_arrays: Vec<&dyn Array> = arrays.iter().map(|arr| arr.as_ref()).collect();
        let array = compute::concat(&dyn_arrays)?;
        Ok(array.into_series())
    }
}
