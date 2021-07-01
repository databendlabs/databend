// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow;
use common_exception::Result;

use crate::prelude::*;
use crate::DFBooleanArray;

pub struct DataArrayFilter;

impl DataArrayFilter {
    pub fn filter_batch_array(
        array: Vec<Series>,
        predicate: &DFBooleanArray,
    ) -> Result<Vec<Series>> {
        if predicate.null_count() > 0 {
            // this greatly simplifies subsequent filtering code
            // now we only have a boolean mask to deal with
            let predicate = arrow::compute::prep_null_mask_filter(predicate.downcast_ref());
            let predicate_array = DFBooleanArray::from_arrow_array(predicate);
            return Self::filter_batch_array(array, &predicate_array);
        }

        let filter = arrow::compute::build_filter(predicate.downcast_ref())?;
        let filtered_arrays = array
            .iter()
            .map(|a| arrow::array::make_array(filter(a.get_array_ref().data())).into_series())
            .collect();
        Ok(filtered_arrays)
    }
}
