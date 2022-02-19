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

use std::sync::Arc;

use common_arrow::arrow::bitmap::MutableBitmap;
use common_datavalues::prelude::*;
use common_datavalues::with_match_scalar_types_error;
use common_exception::Result;

#[test]
fn test_builder() -> Result<()> {
    struct Test {
        name: &'static str,
        column: ColumnRef,
        inject_null: bool,
    }

    let tests = vec![
        Test {
            name: "test_i32",
            column: Series::from_data(vec![1, 2, 3, 4, 5, 6, 7]),
            inject_null: false,
        },
        Test {
            name: "test_i32_nullable",
            column: Series::from_data(vec![1, 2, 3, 4, 5, 6, 7]),
            inject_null: true,
        },
        Test {
            name: "test_f64",
            column: Series::from_data(&[1.0f64, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]),
            inject_null: false,
        },
        Test {
            name: "test_bool",
            column: Series::from_data(&[true, false, true, true, false]),
            inject_null: false,
        },
        Test {
            name: "test_string",
            column: Series::from_data(&["1", "2", "3", "$"]),
            inject_null: false,
        },
        Test {
            name: "test_string_nullable",
            column: Series::from_data(&["1", "2", "3", "$"]),
            inject_null: false,
        },
        Test {
            name: "test_f64_nullable",
            column: Series::from_data(&[1.0f64, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]),
            inject_null: true,
        },
        Test {
            name: "test_u64_nullable",
            column: Series::from_data(&[
                143u64, 434, 344, 24234, 24, 324, 32, 432, 4, 324, 32, 432423,
            ]),
            inject_null: true,
        },
    ];

    fn get_column<T: Scalar>(column: &ColumnRef) -> Result<ColumnRef>
    where T::ColumnType: Clone + 'static {
        let size = column.len();

        if column.is_nullable() {
            let mut builder = NullableColumnBuilder::<T>::with_capacity(size);
            let viewer = T::try_create_viewer(column)?;

            for i in 0..viewer.size() {
                builder.append(viewer.value_at(i), viewer.valid_at(i));
            }
            let result = builder.build(size);
            Ok(result)
        } else {
            let mut builder = ColumnBuilder::<T>::with_capacity(size);
            let viewer = T::try_create_viewer(column)?;

            for i in 0..viewer.size() {
                builder.append(viewer.value_at(i));
            }

            let result = builder.build(size);
            Ok(result)
        }
    }

    for t in tests {
        let mut column = t.column;
        let size = column.len();
        if t.inject_null {
            let mut bm = MutableBitmap::with_capacity(size);
            for i in 0..size {
                bm.push(i % 3 == 1);
            }
            column = Arc::new(NullableColumn::new(column, bm.into()));
        }

        let ty = remove_nullable(&column.data_type());
        let ty = ty.data_type_id().to_physical_type();

        with_match_scalar_types_error!(ty, |$T| {
            let result = get_column::<$T>(&column)?;
            assert!(result == column, "test {}", t.name);
        });
    }
    Ok(())
}
