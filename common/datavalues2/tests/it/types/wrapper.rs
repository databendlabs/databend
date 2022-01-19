// Copyright 2021 Datafuse Labs.
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

use common_datavalues2::prelude::*;
use common_exception::Result;
use pretty_assertions::assert_eq;

#[test]
fn test_boolean_wrapper() -> Result<()> {
    let column = Series::from_data(vec![true, true, false, false]);
    let wrapper = ColumnViewer::<bool>::create(&column)?;

    assert_eq!(wrapper.len(), 4);
    assert!(!wrapper.null_at(0));
    for i in 0..2 {
        assert!(*wrapper.value(i));
    }
    for i in 2..4 {
        assert!(!*wrapper.value(i));
    }
    Ok(())
}

#[test]
fn test_primitive_wrapper() -> Result<()> {
    let column = Series::from_data(vec![1i8, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let wrapper = ColumnViewer::<i8>::create(&column)?;

    assert_eq!(wrapper.len(), 10);
    assert!(!wrapper.null_at(0));
    for i in 0..wrapper.len() {
        assert_eq!(*wrapper.value(i), (i + 1) as i8);
    }
    Ok(())
}

#[test]
fn test_nullable_wrapper() -> Result<()> {
    let column = Series::from_data(vec![Some(1i8), None, Some(3), Some(4), Some(5)]);
    let wrapper = ColumnViewer::<i8>::create(&column)?;

    assert_eq!(wrapper.len(), 5);
    assert!(!wrapper.null_at(0));
    assert!(wrapper.null_at(1));

    for i in 0..wrapper.len() {
        if wrapper.null_at(i) {
            assert_eq!(*wrapper.value(i), 0);
        } else {
            assert_eq!(*wrapper.value(i), (i + 1) as i8);
        }
    }
    Ok(())
}

#[test]
fn test_constant_wrapper() -> Result<()> {
    let ty = Int16Type::arc();
    let column = ty.create_constant_column(&DataValue::Int64(123), 1024)?;
    let wrapper = ColumnViewer::<i16>::create(&column)?;

    assert_eq!(wrapper.len(), 1024);
    assert!(!wrapper.null_at(0));
    assert!(!wrapper.null_at(99));

    for i in 0..wrapper.len() {
        if wrapper.null_at(i) {
            assert_eq!(*wrapper.value(i), 0);
        } else {
            assert_eq!(*wrapper.value(i), 123i16);
        }
    }
    Ok(())
}
