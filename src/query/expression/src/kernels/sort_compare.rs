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

use std::cmp::Ordering;
use std::ops::Range;

use databend_common_column::bitmap::Bitmap;
use databend_common_column::buffer::Buffer;
use databend_common_exception::Result;
use memchr::memchr;

use crate::LimitType;
use crate::SortColumnDescription;
use crate::types::AnyType;
use crate::types::DataType;
use crate::types::NullableColumn;
use crate::types::Number;
use crate::types::StringColumn;
use crate::types::ValueType;
use crate::visitor::ValueVisitor;

pub struct SortCompare {
    rows: usize,
    limit: LimitType,
    permutation: Vec<u32>,
    ordering_descs: Vec<SortColumnDescription>,
    current_column_index: usize,
    validity: Option<Bitmap>,
    equality_index: Vec<u8>,
    force_equality: bool,
}

macro_rules! do_sorter {
    ($self: expr, $value:expr, $validity:expr, $g:expr, $c:expr, $ordering_desc:expr, $range: expr) => {
        if let Some(valids) = &$validity {
            $self.do_inner_sort(
                |&a, &b| match (valids.get_bit(a as _), valids.get_bit(b as _)) {
                    (true, true) => {
                        let left = $g($value, a);
                        let right = $g($value, b);

                        if $ordering_desc.asc {
                            $c(left, right)
                        } else {
                            $c(right, left)
                        }
                    }
                    (true, false) => {
                        if $ordering_desc.nulls_first {
                            Ordering::Greater
                        } else {
                            Ordering::Less
                        }
                    }
                    (false, true) => {
                        if $ordering_desc.nulls_first {
                            Ordering::Less
                        } else {
                            Ordering::Greater
                        }
                    }
                    (false, false) => Ordering::Equal,
                },
                $range,
            );
        } else {
            if $ordering_desc.asc {
                $self.do_inner_sort(
                    |&a, &b| {
                        let left = $g($value, a);
                        let right = $g($value, b);
                        $c(left, right)
                    },
                    $range,
                );
            } else {
                $self.do_inner_sort(
                    |&a, &b| {
                        let left = $g($value, a);
                        let right = $g($value, b);
                        $c(right, left)
                    },
                    $range,
                );
            }
        }
    };
}

impl SortCompare {
    pub fn new(ordering_descs: Vec<SortColumnDescription>, rows: usize, limit: LimitType) -> Self {
        let equality_index =
            if ordering_descs.len() == 1 && !matches!(limit, LimitType::LimitRank(_)) {
                vec![]
            } else {
                vec![1; rows as _]
            };
        Self {
            rows,
            limit,
            permutation: (0..rows as u32).collect(),
            ordering_descs,
            current_column_index: 0,
            validity: None,
            equality_index,
            force_equality: matches!(limit, LimitType::LimitRank(_)),
        }
    }

    pub fn with_force_equality(ordering_descs: Vec<SortColumnDescription>, rows: usize) -> Self {
        Self {
            rows,
            limit: LimitType::None,
            permutation: (0..rows as u32).collect(),
            ordering_descs,
            current_column_index: 0,
            validity: None,
            equality_index: vec![1; rows as _],
            force_equality: true,
        }
    }

    fn need_update_equality_index(&self) -> bool {
        self.force_equality || self.current_column_index != self.ordering_descs.len() - 1
    }

    pub fn increment_column_index(&mut self) {
        self.current_column_index += 1;
    }

    pub fn take_permutation(mut self) -> Vec<u32> {
        match self.limit {
            LimitType::None => self.permutation,
            LimitType::LimitRows(rows) => {
                self.permutation.truncate(rows);
                self.permutation
            }
            LimitType::LimitRank(rank_number) => {
                let mut unique_count = 0;

                let mut start = 0;
                // the index of last zero sign
                let mut zero_index: isize = -1;
                while start < self.rows {
                    // Find the first occurrence of 1 in the equality_index using memchr
                    if let Some(pos) = memchr(1, &self.equality_index[start..self.rows]) {
                        start += pos;
                    } else {
                        start = self.rows;
                    }
                    unique_count += (start as isize - zero_index) as usize;

                    if unique_count > rank_number {
                        start -= unique_count - rank_number;
                        break;
                    }

                    if start == self.rows {
                        break;
                    }

                    // Find the first occurrence of 0 after the start position using memchr
                    if let Some(pos) = memchr(0, &self.equality_index[start..self.rows]) {
                        start += pos;
                    } else {
                        start = self.rows;
                    }
                    if unique_count == rank_number {
                        break;
                    }
                    zero_index = start as _;
                }

                self.permutation.truncate(start);
                self.permutation
            }
        }
    }

    fn do_inner_sort<C>(&mut self, c: C, range: Range<usize>)
    where C: FnMut(&u32, &u32) -> Ordering + Copy {
        let permutations = &mut self.permutation[range.start..range.end];

        let limit = self.limit.limit_rows(self.rows);
        if limit > range.start && limit < range.end {
            let (p, _, _) = permutations.select_nth_unstable_by(limit - range.start, c);
            p.sort_unstable_by(c);
        } else {
            permutations.sort_unstable_by(c);
        }
    }

    // sort the value using generic G and C
    fn generic_sort<T, V, G, C>(&mut self, value: V, g: G, c: C)
    where
        G: Fn(V, u32) -> T + Copy,
        V: Copy,
        C: Fn(T, T) -> Ordering + Copy,
    {
        let validity = self.validity.take();
        let ordering_desc = self.ordering_descs[self.current_column_index].clone();

        // faster path for only one sort column
        if self.ordering_descs.len() == 1 {
            do_sorter!(self, value, validity, g, c, ordering_desc, 0..self.rows);
        } else {
            let mut current = 1;
            let len = self.rows;
            let need_update_equality_index = self.need_update_equality_index();

            while current < len {
                // Find the start of the next range of equal elements
                let start = if let Some(pos) = memchr(1, &self.equality_index[current..len]) {
                    current + pos
                } else {
                    len
                };

                if start == len {
                    break;
                }

                // Find the end of the range of equal elements
                let end = if let Some(pos) = memchr(0, &self.equality_index[start..len]) {
                    start + pos
                } else {
                    len
                };

                let range = start - 1..end;
                // Perform the inner sort on the found range
                do_sorter!(self, value, validity, g, c, ordering_desc, range);
                if need_update_equality_index {
                    // Update equality_index
                    for i in start..end {
                        let is_equal = if let Some(ref v) = validity {
                            let va = v.get_bit(self.permutation[i] as _);
                            let vb = v.get_bit(self.permutation[i - 1] as _);
                            if va && vb {
                                c(
                                    g(value, self.permutation[i]),
                                    g(value, self.permutation[i - 1]),
                                ) == Ordering::Equal
                            } else {
                                !va && !vb
                            }
                        } else {
                            c(
                                g(value, self.permutation[i]),
                                g(value, self.permutation[i - 1]),
                            ) == Ordering::Equal
                        };
                        self.equality_index[i] &= u8::from(is_equal);
                    }
                }

                current = end;
            }
        }
    }

    pub fn equality_index(&self) -> &[u8] {
        debug_assert!(self.force_equality);
        &self.equality_index
    }
}

impl ValueVisitor for SortCompare {
    fn visit_scalar(&mut self, _scalar: crate::Scalar) -> Result<()> {
        Ok(())
    }

    // faster path for numeric
    fn visit_number<T: Number>(&mut self, column: Buffer<T>) -> Result<()> {
        let values = column.as_slice();
        assert!(values.len() == self.rows);
        self.generic_sort(values, |c, idx| c[idx as usize], |a: T, b: T| a.cmp(&b));
        Ok(())
    }

    fn visit_timestamp(&mut self, buffer: Buffer<i64>) -> Result<()> {
        self.visit_number(buffer)
    }

    fn visit_date(&mut self, buffer: Buffer<i32>) -> Result<()> {
        self.visit_number(buffer)
    }

    fn visit_string(&mut self, column: StringColumn) -> Result<()> {
        assert!(column.len() == self.rows);
        self.generic_sort(
            &column,
            |col, idx| (col, idx as usize),
            |(col1, idx1), (col2, idx2)| StringColumn::compare(col1, idx1, col2, idx2),
        );
        Ok(())
    }

    fn visit_typed_column<T: ValueType>(&mut self, col: T::Column, _: &DataType) -> Result<()> {
        assert!(T::column_len(&col) == self.rows);
        self.generic_sort(
            &col,
            |c, idx| unsafe { T::index_column_unchecked(c, idx as _) },
            |a, b| T::compare(a, b),
        );
        Ok(())
    }

    fn visit_nullable(&mut self, column: Box<NullableColumn<AnyType>>) -> Result<()> {
        if column.validity.null_count() > 0 {
            self.validity = Some(column.validity.clone());
        }
        self.visit_column(column.column.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_take_permutation() {
        let test_cases1 = vec![
            (12, LimitType::None, 0..12),
            (12, LimitType::LimitRows(5), 0..5),
        ];

        let test_cases2 = vec![
            (12, LimitType::LimitRank(5), 0..11),
            (12, LimitType::LimitRank(3), 0..6),
            (12, LimitType::LimitRank(4), 0..7),
            (12, LimitType::LimitRank(5), 0..11),
        ];

        for (c, limit, range) in test_cases1 {
            let sort_compare = SortCompare::new(vec![], c, limit);

            let permutation = sort_compare.take_permutation();
            let result: Vec<u32> = range.map(|c| c as u32).collect();
            assert_eq!(permutation, result);
        }

        for (c, limit, range) in test_cases2 {
            let mut sort_compare = SortCompare::new(vec![], c, limit);
            sort_compare.equality_index = vec![1, 0, 0, 1, 1, 1, 0, 0, 1, 1, 1, 0];
            let permutation = sort_compare.take_permutation();
            let result: Vec<u32> = range.map(|c| c as u32).collect();
            assert_eq!(permutation, result);
        }
    }
}
