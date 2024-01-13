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

use databend_common_arrow::arrow::array::Utf8Array;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_arrow::arrow::datatypes::DataType;
use databend_common_arrow::arrow::offset::OffsetsBuffer;

#[test]
fn not_shared() {
    let array = Utf8Array::<i32>::from([Some("hello"), Some(" "), None]);
    assert!(array.into_mut().is_right());
}

#[test]
#[allow(clippy::redundant_clone)]
fn shared_validity() {
    let validity = Bitmap::from([true]);
    let array = Utf8Array::<i32>::new(
        DataType::Utf8,
        vec![0, 1].try_into().unwrap(),
        b"a".to_vec().into(),
        Some(validity.clone()),
    );
    assert!(array.into_mut().is_left())
}

#[test]
#[allow(clippy::redundant_clone)]
fn shared_values() {
    let values: Buffer<u8> = b"a".to_vec().into();
    let array = Utf8Array::<i32>::new(
        DataType::Utf8,
        vec![0, 1].try_into().unwrap(),
        values.clone(),
        Some(Bitmap::from([true])),
    );
    assert!(array.into_mut().is_left())
}

#[test]
#[allow(clippy::redundant_clone)]
fn shared_offsets_values() {
    let offsets: OffsetsBuffer<i32> = vec![0, 1].try_into().unwrap();
    let values: Buffer<u8> = b"a".to_vec().into();
    let array = Utf8Array::<i32>::new(
        DataType::Utf8,
        offsets.clone(),
        values.clone(),
        Some(Bitmap::from([true])),
    );
    assert!(array.into_mut().is_left())
}

#[test]
#[allow(clippy::redundant_clone)]
fn shared_offsets() {
    let offsets: OffsetsBuffer<i32> = vec![0, 1].try_into().unwrap();
    let array = Utf8Array::<i32>::new(
        DataType::Utf8,
        offsets.clone(),
        b"a".to_vec().into(),
        Some(Bitmap::from([true])),
    );
    assert!(array.into_mut().is_left())
}

#[test]
#[allow(clippy::redundant_clone)]
fn shared_all() {
    let array = Utf8Array::<i32>::from([Some("hello"), Some(" "), None]);
    assert!(array.clone().into_mut().is_left())
}
