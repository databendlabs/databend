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

use std::ops::BitAnd;
use std::sync::Arc;

use common_arrow::arrow::array::*;
use common_arrow::arrow::compute::filter::build_filter;
use common_exception::Result;

use crate::prelude::*;

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

        let filter = build_filter(predicate.inner())?;
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
        let array = filter.inner();
        let filter_mask = array.values();

        match array.validity() {
            None => filter.clone(),
            Some(v) => DFBooleanArray::from_arrow_data(filter_mask.bitand(v), None),
        }
    }
}
