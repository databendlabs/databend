// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::ops::BitAnd;

use common_arrow::arrow::array::*;
use common_arrow::arrow::compute::filter::build_filter;
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
            let predicate = Self::remove_null_filter(predicate);
            return Self::filter_batch_array(array, &predicate);
        }

        let filter = build_filter(predicate.downcast_ref())?;
        let filtered_arrays: Vec<Series> = array
            .iter()
            .map(|a| {
                let c = a.get_array_ref();
                let c = filter(c.as_ref());
                let c: Arc<dyn Array> = Arc::from(c);
                c.into_series()
            })
            .collect();

        Ok(filtered_arrays)
    }

    /// Remove null values by do a bitmask AND operation with null bits and the boolean bits.
    fn remove_null_filter(filter: &DFBooleanArray) -> DFBooleanArray {
        let array = filter.downcast_ref();
        let mask = array.values();
        if let Some(v) = array.validity() {
            let mask = mask.bitand(v);
            return DFBooleanArray::from_arrow_array(BooleanArray::from_data(mask, None));
        }
        filter.clone()
    }
}
