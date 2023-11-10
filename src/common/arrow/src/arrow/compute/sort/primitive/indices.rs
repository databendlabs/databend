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

use super::super::common;
use super::super::SortOptions;
use crate::arrow::array::PrimitiveArray;
use crate::arrow::types::Index;
use crate::arrow::types::NativeType;

/// Unstable sort of indices.
pub fn indices_sorted_unstable_by<I, T, F>(
    array: &PrimitiveArray<T>,
    cmp: F,
    options: &SortOptions,
    limit: Option<usize>,
) -> PrimitiveArray<I>
where
    I: Index,
    T: NativeType,
    F: Fn(&T, &T) -> std::cmp::Ordering,
{
    let values = array.values().as_slice();
    unsafe {
        common::indices_sorted_unstable_by(
            array.validity(),
            |x: usize| *values.get_unchecked(x),
            cmp,
            array.len(),
            options,
            limit,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::ord;
    use crate::arrow::array::*;
    use crate::arrow::datatypes::DataType;

    fn test<T>(
        data: &[Option<T>],
        data_type: DataType,
        options: SortOptions,
        limit: Option<usize>,
        expected_data: &[i32],
    ) where
        T: NativeType + std::cmp::Ord,
    {
        let input = PrimitiveArray::<T>::from(data).to(data_type);
        let expected = Int32Array::from_slice(expected_data);
        let output =
            indices_sorted_unstable_by::<i32, _, _>(&input, ord::total_cmp, &options, limit);
        assert_eq!(output, expected)
    }

    #[test]
    fn ascending_nulls_first() {
        test::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            SortOptions {
                descending: false,
                nulls_first: true,
            },
            None,
            &[0, 5, 3, 1, 4, 2],
        );
    }

    #[test]
    fn ascending_nulls_last() {
        test::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            SortOptions {
                descending: false,
                nulls_first: false,
            },
            None,
            &[3, 1, 4, 2, 0, 5],
        );
    }

    #[test]
    fn descending_nulls_first() {
        test::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            SortOptions {
                descending: true,
                nulls_first: true,
            },
            None,
            &[0, 5, 2, 1, 4, 3],
        );
    }

    #[test]
    fn descending_nulls_last() {
        test::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
            None,
            &[2, 1, 4, 3, 0, 5],
        );
    }

    #[test]
    fn limit_ascending_nulls_first() {
        // nulls sorted
        test::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            SortOptions {
                descending: false,
                nulls_first: true,
            },
            Some(2),
            &[0, 5],
        );

        // nulls and values sorted
        test::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            SortOptions {
                descending: false,
                nulls_first: true,
            },
            Some(4),
            &[0, 5, 3, 1],
        );
    }

    #[test]
    fn limit_ascending_nulls_last() {
        // values
        test::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            SortOptions {
                descending: false,
                nulls_first: false,
            },
            Some(2),
            &[3, 1],
        );

        // values and nulls
        test::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            SortOptions {
                descending: false,
                nulls_first: false,
            },
            Some(5),
            &[3, 1, 4, 2, 0],
        );
    }

    #[test]
    fn limit_descending_nulls_first() {
        // nulls
        test::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            SortOptions {
                descending: true,
                nulls_first: true,
            },
            Some(2),
            &[0, 5],
        );

        // nulls and values
        test::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            SortOptions {
                descending: true,
                nulls_first: true,
            },
            Some(4),
            &[0, 5, 2, 1],
        );
    }

    #[test]
    fn limit_descending_nulls_last() {
        // values
        test::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
            Some(2),
            &[2, 1],
        );

        // values and nulls
        test::<i8>(
            &[None, Some(3), Some(5), Some(2), Some(3), None],
            DataType::Int8,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
            Some(5),
            &[2, 1, 4, 3, 0],
        );
    }
}
