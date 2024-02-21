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

use databend_common_arrow::arrow::array::BinaryViewArray;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::datatypes::DataType;

#[test]
fn not_shared() {
    let array = BinaryViewArray::from([Some("hello"), Some(" "), None]);
    assert!(array.into_mut().is_right());
}

#[test]
#[allow(clippy::redundant_clone)]
fn shared() {
    let validity = Bitmap::from([true]);
    let data = vec![
        Some(b"hello".to_vec()),
        None,
        // larger than 12 bytes.
        Some(b"Databend Cloud is a Cost-Effective alternative to Snowflake.".to_vec()),
    ];

    let array: BinaryViewArray = data.into_iter().collect();
    let array2 = BinaryViewArray::new_unchecked(
        DataType::BinaryView,
        array.views().clone(),
        array.data_buffers().clone(),
        Some(validity.clone()),
        array.total_bytes_len(),
        array.total_buffer_len(),
    );
    assert!(array2.into_mut().is_left())
}
