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

use common_datavalues::prelude::*;
use common_exception::Result;
use pretty_assertions::assert_eq;

#[test]
fn test_primitive_viewer() -> Result<()> {
    let column = Series::from_data(vec![1i8, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let viewer = i8::try_create_viewer(&column)?;

    assert_eq!(viewer.size(), 10);
    assert!(!viewer.null_at(0));
    for (i, value) in viewer.iter().enumerate() {
        assert_eq!(value, (i + 1) as i8);
    }
    Ok(())
}

#[test]
fn test_nullable_viewer() -> Result<()> {
    let column = Series::from_data(vec![Some(1i8), None, Some(3), Some(4), Some(5)]);
    let viewer = i8::try_create_viewer(&column)?;

    assert_eq!(viewer.size(), 5);
    assert!(!viewer.null_at(0));
    assert!(viewer.null_at(1));

    for (i, value) in viewer.iter().enumerate() {
        if viewer.null_at(i) {
            assert_eq!(value, 0);
        } else {
            assert_eq!(value, (i + 1) as i8);
        }
    }
    Ok(())
}

#[test]
fn test_constant_viewer() -> Result<()> {
    let ty = Int16Type::arc();
    let column = ty.create_constant_column(&DataValue::Int64(123), 1024)?;

    let viewer = i16::try_create_viewer(&column)?;

    assert_eq!(viewer.size(), 1024);
    assert!(!viewer.null_at(0));
    assert!(!viewer.null_at(99));

    for (idx, value) in viewer.iter().enumerate() {
        if viewer.null_at(idx) {
            assert_eq!(value, 0);
        } else {
            assert_eq!(value, 123i16);
        }
    }
    Ok(())
}
