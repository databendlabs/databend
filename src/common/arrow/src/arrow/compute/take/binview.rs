// Copyright (c) 2020 Ritchie Vink
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

use crate::arrow::array::Array;
use crate::arrow::array::BinaryViewArray;
use crate::arrow::array::PrimitiveArray;
use crate::arrow::compute::take::primitive::take_values_and_validity_unchecked;
use crate::arrow::types::Index;

/// # Safety
/// No bound checks
pub(super) unsafe fn take_binview_unchecked<I: Index>(
    arr: &BinaryViewArray,
    indices: &PrimitiveArray<I>,
) -> BinaryViewArray {
    let (views, validity) =
        take_values_and_validity_unchecked(arr.views(), arr.validity(), indices);

    BinaryViewArray::new_unchecked_unknown_md(
        arr.data_type().clone(),
        views.into(),
        arr.data_buffers().clone(),
        validity,
        Some(arr.total_buffer_len()),
    )
    .maybe_gc()
}
