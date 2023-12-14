// Copyright 2021 Datafuse Labs
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

use std::collections::BinaryHeap;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::SortColumnDescription;

use super::Rows;

/// Find the bigger child of the root of the heap.
#[inline(always)]
pub fn find_bigger_child_of_root<T: Ord>(heap: &BinaryHeap<T>) -> &T {
    debug_assert!(heap.len() >= 2);
    let slice = heap.as_slice();
    if heap.len() == 2 {
        &slice[1]
    } else {
        (&slice[1]).max(&slice[2])
    }
}

#[inline(always)]
pub fn get_ordered_rows<R: Rows>(block: &DataBlock, desc: &[SortColumnDescription]) -> Result<R> {
    let order_col = block.columns().last().unwrap().value.as_column().unwrap();
    R::from_column(order_col.clone(), desc).ok_or_else(|| {
        let expected_ty = R::data_type();
        let ty = order_col.data_type();
        ErrorCode::BadDataValueType(format!(
            "Order column type mismatched. Expecetd {expected_ty} but got {ty}"
        ))
    })
}
