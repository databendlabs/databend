// Copyright 2020-2022 Jorge C. Leitão
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

//! Contains the concatenate kernel
//!
//! Example:
//!
//! ```
//! use arrow2::array::Utf8Array;
//! use arrow2::compute::concatenate::concatenate;
//!
//! let arr = concatenate(&[
//!     &Utf8Array::<i32>::from_slice(["hello", "world"]),
//!     &Utf8Array::<i32>::from_slice(["!"]),
//! ])
//! .unwrap();
//! assert_eq!(arr.len(), 3);
//! ```

use crate::arrow::array::growable::make_growable;
use crate::arrow::array::Array;
use crate::arrow::error::Error;
use crate::arrow::error::Result;

/// Concatenate multiple [Array] of the same type into a single [`Array`].
pub fn concatenate(arrays: &[&dyn Array]) -> Result<Box<dyn Array>> {
    if arrays.is_empty() {
        return Err(Error::InvalidArgumentError(
            "concat requires input of at least one array".to_string(),
        ));
    }

    if arrays
        .iter()
        .any(|array| array.data_type() != arrays[0].data_type())
    {
        return Err(Error::InvalidArgumentError(
            "It is not possible to concatenate arrays of different data types.".to_string(),
        ));
    }

    let lengths = arrays.iter().map(|array| array.len()).collect::<Vec<_>>();
    let capacity = lengths.iter().sum();

    let mut mutable = make_growable(arrays, false, capacity);

    for (i, len) in lengths.iter().enumerate() {
        mutable.extend(i, 0, *len)
    }

    Ok(mutable.as_box())
}
