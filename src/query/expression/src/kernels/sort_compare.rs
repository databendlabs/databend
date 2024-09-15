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

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_exception::Result;
use memchr::memchr;

use crate::types::AnyType;
use crate::types::NullableColumn;
use crate::types::Number;
use crate::types::ValueType;
use crate::visitor::ValueVisitor;
use crate::SortColumnDescription;

pub struct SortCompare {
    rows: usize,
    limit: Option<usize>,
    permutation: Vec<u32>,
    ordering_descs: Vec<SortColumnDescription>,
    current_column_index: usize,
    validity: Option<Bitmap>,
    equality_index: Vec<u8>,
}

macro_rules! generate_comparator {
    ($value:expr, $validity:expr, $g:expr, $c:expr, $ordering_desc:expr) => {
        |&a, &b| {
            let ord = if let Some(valids) = &$validity {
                match (valids.get_bit(a as _), valids.get_bit(b as _)) {
                    (true, true) => {
                        let left = $g($value, a);
                        let right = $g($value, b);
                        $c(left, right)
                    }
                    (true, false) => Ordering::Less,
                    (false, true) => Ordering::Greater,
                    (false, false) => Ordering::Equal,
                }
            } else {
                let left = $g($value, a);
                let right = $g($value, b);
                $c(left, right)
            };

            if $ordering_desc.asc {
                ord
            } else {
                ord.reverse()
            }
        }
    };
}

impl SortCompare {
    pub fn new(
        ordering_descs: Vec<SortColumnDescription>,
        rows: usize,
        limit: Option<usize>,
    ) -> Self {
        let equality_index = if ordering_descs.len() == 1 {
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
        }
    }

    pub fn increment_column_index(&mut self) {
        self.current_column_index += 1;
    }

    pub fn take_permutation(mut self) -> Vec<u32> {
        let limit = self.limit.unwrap_or(self.rows);
        self.permutation.truncate(limit);
        self.permutation
    }

    fn do_inner_sort<C>(&mut self, c: C, range: Range<usize>)
    where C: FnMut(&u32, &u32) -> Ordering + Copy {
        let permutations = &mut self.permutation[range.start..range.end];

        let limit = self.limit.unwrap_or(self.rows);
        if limit > range.start && limit < range.end {
            let (p, _, _) = permutations.select_nth_unstable_by(limit - range.start, c);
            p.sort_unstable_by(c);
        } else {
            permutations.sort_unstable_by(c);
        }
    }

    fn common_sort<T, V, G, C>(&mut self, value: V, g: G, c: C)
    where
        G: Fn(V, u32) -> T + Copy,
        V: Copy,
        C: Fn(T, T) -> Ordering + Copy,
    {
        let mut validity = self.validity.take();
        let ordering_desc = self.ordering_descs[self.current_column_index].clone();

        // faster path for only one sort column
        if self.ordering_descs.len() == 1 {
            self.do_inner_sort(
                generate_comparator!(value, validity, g, c, ordering_desc),
                0..self.rows,
            );
        } else {
            let mut current = 1;
            let len = self.rows;
            let need_update_equality_index =
                self.current_column_index != self.ordering_descs.len() - 1;

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

                if let Some(v) = validity.as_mut() {
                    v.slice(range.start, range.end - range.start);
                    if v.unset_bits() == 0 {
                        validity = None;
                    }
                }
                // Perform the inner sort on the found range
                self.do_inner_sort(
                    generate_comparator!(value, validity, g, c, ordering_desc),
                    range,
                );

                if need_update_equality_index {
                    // Update equality_index
                    for i in start..end {
                        let is_equal = u8::from(
                            c(
                                g(value, self.permutation[i]),
                                g(value, self.permutation[i - 1]),
                            ) == Ordering::Equal,
                        );
                        self.equality_index[i] &= is_equal;
                    }
                }

                current = end;
            }
        }
    }
}

impl ValueVisitor for SortCompare {
    fn visit_scalar(&mut self, _scalar: crate::Scalar) -> Result<()> {
        Ok(())
    }

    // faster path for numeric
    fn visit_number<T: Number>(&mut self, column: Buffer<T>) -> Result<()> {
        let values = column.as_slice();
        self.common_sort(values, |c, idx| c[idx as usize], |a: T, b: T| a.cmp(&b));
        Ok(())
    }

    fn visit_timestamp(&mut self, buffer: Buffer<i64>) -> Result<()> {
        self.visit_number(buffer)
    }

    fn visit_date(&mut self, buffer: Buffer<i32>) -> Result<()> {
        self.visit_number(buffer)
    }

    fn visit_typed_column<T: ValueType>(&mut self, col: T::Column) -> Result<()> {
        self.common_sort(
            &col,
            |c, idx| -> T::ScalarRef<'_> { unsafe { T::index_column_unchecked(&c, idx as _) } },
            |a, b| T::compare(a, b),
        );
        Ok(())
    }

    fn visit_nullable(&mut self, column: Box<NullableColumn<AnyType>>) -> Result<()> {
        if column.validity.unset_bits() > 0 {
            self.validity = Some(column.validity.clone());
        }
        self.visit_column(column.column.clone())
    }
}
