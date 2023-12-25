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

use databend_common_arrow::arrow::array::PrimitiveArray;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::datatypes::DataType;
use either::Either;

#[test]
fn array_to_mutable() {
    let data = vec![1, 2, 3];
    let arr = PrimitiveArray::new(DataType::Int32, data.into(), None);

    // to mutable push and freeze again
    let mut mut_arr = arr.into_mut().unwrap_right();
    mut_arr.push(Some(5));
    let immut: PrimitiveArray<i32> = mut_arr.into();
    assert_eq!(immut.values().as_slice(), [1, 2, 3, 5]);

    // let's cause a realloc and see if miri is ok
    let mut mut_arr = immut.into_mut().unwrap_right();
    mut_arr.extend_constant(256, Some(9));
    let immut: PrimitiveArray<i32> = mut_arr.into();
    assert_eq!(immut.values().len(), 256 + 4);
}

#[test]
fn array_to_mutable_not_owned() {
    let data = vec![1, 2, 3];
    let arr = PrimitiveArray::new(DataType::Int32, data.into(), None);
    let arr2 = arr.clone();

    // to the `to_mutable` should fail and we should get back the original array
    match arr2.into_mut() {
        Either::Left(arr2) => {
            assert_eq!(arr, arr2);
        }
        _ => panic!(),
    }
}

#[test]
#[allow(clippy::redundant_clone)]
fn array_to_mutable_validity() {
    let data = vec![1, 2, 3];

    // both have a single reference should be ok
    let bitmap = Bitmap::from_iter([true, false, true]);
    let arr = PrimitiveArray::new(DataType::Int32, data.clone().into(), Some(bitmap));
    assert!(matches!(arr.into_mut(), Either::Right(_)));

    // now we clone the bitmap increasing the ref count
    let bitmap = Bitmap::from_iter([true, false, true]);
    let arr = PrimitiveArray::new(DataType::Int32, data.into(), Some(bitmap.clone()));
    assert!(matches!(arr.into_mut(), Either::Left(_)));
}
