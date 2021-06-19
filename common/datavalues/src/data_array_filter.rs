// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow;
use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::BooleanArray;
use common_exception::Result;

use crate::DataArrayRef;

pub struct DataArrayFilter;

impl DataArrayFilter {
    pub fn filter_batch_array(
        array: Vec<DataArrayRef>,
        predicate: &BooleanArray,
    ) -> Result<Vec<DataArrayRef>> {
        if predicate.null_count() > 0 {
            // this greatly simplifies subsequent filtering code
            // now we only have a boolean mask to deal with
            let predicate = arrow::compute::prep_null_mask_filter(predicate);
            return Self::filter_batch_array(array, &predicate);
        }

        let filter = arrow::compute::build_filter(predicate)?;
        let filtered_arrays = array
            .iter()
            .map(|a| arrow::array::make_array(filter(&a.data())))
            .collect();
        Ok(filtered_arrays)
    }
}
