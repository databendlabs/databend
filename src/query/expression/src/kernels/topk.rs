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
use std::cmp::Ordering::Less;
use std::intrinsics::assume;
use std::mem;
use std::ptr;

use databend_common_arrow::arrow::bitmap::MutableBitmap;
use databend_common_base::runtime::drop_guard;

use crate::types::*;
use crate::with_number_mapped_type;
use crate::Column;
use crate::Scalar;

#[derive(Clone)]
pub struct TopKSorter {
    data: Vec<Scalar>,
    limit: usize,
    asc: bool,
}

impl TopKSorter {
    pub fn new(limit: usize, asc: bool) -> Self {
        Self {
            data: Vec::with_capacity(limit),
            limit,
            asc,
        }
    }

    // Push the column into this sorted and update the bitmap
    // The bitmap could be used in filter
    pub fn push_column(&mut self, col: &Column, bitmap: &mut MutableBitmap) {
        with_number_mapped_type!(|NUM_TYPE| match col.data_type() {
            DataType::Number(NumberDataType::NUM_TYPE) =>
                self.push_column_internal::<NumberType::<NUM_TYPE>>(col, bitmap),
            DataType::String => self.push_column_internal::<StringType>(col, bitmap),
            DataType::Timestamp => self.push_column_internal::<TimestampType>(col, bitmap),
            DataType::Date => self.push_column_internal::<DateType>(col, bitmap),
            _ => {}
        });
    }

    fn push_column_internal<T: ValueType>(&mut self, col: &Column, bitmap: &mut MutableBitmap)
    where for<'a> T::ScalarRef<'a>: Ord {
        let col = T::try_downcast_column(col).unwrap();
        for (i, value) in T::iter_column(&col).enumerate() {
            if !bitmap.get(i) {
                continue;
            }

            if self.data.len() < self.limit {
                self.data.push(T::upcast_scalar(T::to_owned_scalar(value)));
                if self.data.len() == self.limit {
                    self.make_heap();
                }
            } else if !self.push_value::<T>(value) {
                bitmap.set(i, false);
            }
        }
    }

    // Push the column into this sorted and update the selection
    // The selection could be used in filter
    pub fn push_column_with_selection<const SELECT_ALL: bool>(
        &mut self,
        col: &Column,
        selection: &mut [u32],
        count: usize,
    ) -> usize {
        with_number_mapped_type!(|NUM_TYPE| match col.data_type() {
            DataType::Number(NumberDataType::NUM_TYPE) => self
                .push_column_with_selection_internal::<NumberType::<NUM_TYPE>, SELECT_ALL>(
                    col, selection, count
                ),
            DataType::String => self.push_column_with_selection_internal::<StringType, SELECT_ALL>(
                col, selection, count
            ),
            DataType::Timestamp => self
                .push_column_with_selection_internal::<TimestampType, SELECT_ALL>(
                    col, selection, count
                ),
            DataType::Date => self
                .push_column_with_selection_internal::<DateType, SELECT_ALL>(col, selection, count),
            _ => count,
        })
    }

    fn push_column_with_selection_internal<T: ValueType, const SELECT_ALL: bool>(
        &mut self,
        col: &Column,
        selection: &mut [u32],
        count: usize,
    ) -> usize
    where
        for<'a> T::ScalarRef<'a>: Ord,
    {
        let col = T::try_downcast_column(col).unwrap();
        let mut result_count = 0;
        for i in 0..count {
            let idx = if SELECT_ALL { i as u32 } else { selection[i] };
            let value = unsafe { T::index_column_unchecked(&col, idx as usize) };
            if self.data.len() < self.limit {
                self.data.push(T::upcast_scalar(T::to_owned_scalar(value)));
                if self.data.len() == self.limit {
                    self.make_heap();
                }
                selection[result_count] = idx;
                result_count += 1;
            } else if self.push_value::<T>(value) {
                selection[result_count] = idx;
                result_count += 1;
            }
        }
        result_count
    }

    #[inline]
    fn push_value<T: ValueType>(&mut self, value: T::ScalarRef<'_>) -> bool
    where for<'a> T::ScalarRef<'a>: Ord {
        let order = self.ordering();
        unsafe {
            assume(self.data.len() == self.limit);
        }
        let data = self.data[0].as_ref();
        let data = T::try_downcast_scalar(&data).unwrap();

        let value = T::upcast_gat(value);

        if Ord::cmp(&data, &value) != order {
            drop(data);
            self.data[0] = T::upcast_scalar(T::to_owned_scalar(value));
            self.adjust();
            true
        } else {
            false
        }
    }

    #[inline]
    pub fn never_match(&self, (min, max): &(Scalar, Scalar)) -> bool {
        if self.data.len() != self.limit {
            return false;
        }
        (self.asc && &self.data[0] < min) || (!self.asc && &self.data[0] > max)
    }

    #[inline]
    pub fn never_match_value(&self, val: &Scalar) -> bool {
        if self.data.len() != self.limit {
            return false;
        }
        (self.asc && &self.data[0] < val) || (!self.asc && &self.data[0] > val)
    }

    #[inline]
    pub fn never_match_any(&self, col: &Column) -> bool {
        if self.data.len() != self.limit {
            return false;
        }
        with_number_mapped_type!(|NUM_TYPE| match col.data_type() {
            DataType::Number(NumberDataType::NUM_TYPE) =>
                self.never_match_any_internal::<NumberType::<NUM_TYPE>>(col),
            DataType::String => self.never_match_any_internal::<StringType>(col),
            DataType::Timestamp => self.never_match_any_internal::<TimestampType>(col),
            DataType::Date => self.never_match_any_internal::<DateType>(col),
            _ => false,
        })
    }

    fn never_match_any_internal<T: ValueType>(&self, col: &Column) -> bool
    where for<'a> T::ScalarRef<'a>: Ord {
        let col = T::try_downcast_column(col).unwrap();
        let data = self.data[0].as_ref();

        for val in T::iter_column(&col) {
            let data = T::try_downcast_scalar(&data).unwrap();
            if (self.asc && data >= val) || (!self.asc && data <= val) {
                return false;
            }
        }
        true
    }

    fn make_heap(&mut self) {
        let ordering = self.ordering();
        let data = self.data.as_mut_slice();
        make_heap(data, &mut |a, b| a.cmp(b) == ordering);
    }

    fn adjust(&mut self) {
        let ordering = self.ordering();
        let data = self.data.as_mut_slice();
        adjust_heap(data, 0, data.len(), &mut |a, b| a.cmp(b) == ordering);
    }

    fn ordering(&self) -> Ordering {
        if self.asc { Less } else { Less.reverse() }
    }
}

#[inline]
fn make_heap<T, F>(v: &mut [T], is_less: &mut F)
where F: FnMut(&T, &T) -> bool {
    let len = v.len();

    if len < 2 {
        // no need to adjust heap
        return;
    }

    let mut parent = (len - 2) / 2;

    loop {
        adjust_heap(v, parent, len, is_less);
        if parent == 0 {
            return;
        }
        parent -= 1;
    }
}

/// adjust_heap is a shift up adjust op for the heap
#[inline]
fn adjust_heap<T, F>(v: &mut [T], hole_index: usize, len: usize, is_less: &mut F)
where F: FnMut(&T, &T) -> bool {
    let mut left_child = hole_index * 2 + 1;

    // SAFETY: we ensure hole_index point to a properly initialized value of type T
    let mut tmp = unsafe { mem::ManuallyDrop::new(ptr::read(&v[hole_index])) };
    let mut hole = InsertionHole {
        src: &mut *tmp,
        dest: &mut v[hole_index],
    };
    // Panic safety:
    //
    // If `is_less` panics at any point during the process, `hole` will get dropped and fill the
    // hole in `v` with the unconsumed range in `buf`, thus ensuring that `v` still holds every
    // object it initially held exactly once.

    // SAFETY:
    // we ensure src/dest point to a properly initialized value of type T
    // src is valid for reads of `count * size_of::()` bytes.
    // dest is valid for reads of `count * size_of::()` bytes.
    // Both `src` and `dst` are properly aligned.

    unsafe {
        while left_child < len {
            // SAFETY:
            // we ensure left_child and left_child + 1 are between [0, len)
            if left_child + 1 < len {
                left_child +=
                    is_less(v.get_unchecked(left_child), v.get_unchecked(left_child + 1)) as usize;
            }

            // SAFETY:
            // left_child and hole.dest point to a properly initialized value of type T
            if is_less(&*tmp, v.get_unchecked(left_child)) {
                ptr::copy_nonoverlapping(&v[left_child], hole.dest, 1);
                hole.dest = &mut v[left_child];
            } else {
                break;
            }

            left_child = left_child * 2 + 1;
        }
    }

    // These codes is from std::sort_by
    // When dropped, copies from `src` into `dest`.
    struct InsertionHole<T> {
        src: *mut T,
        dest: *mut T,
    }

    impl<T> Drop for InsertionHole<T> {
        fn drop(&mut self) {
            drop_guard(move || {
                // SAFETY:
                // we ensure src/dest point to a properly initialized value of type T
                // src is valid for reads of `count * size_of::()` bytes.
                // dest is valid for reads of `count * size_of::()` bytes.
                // Both `src` and `dst` are properly aligned.
                unsafe {
                    ptr::copy_nonoverlapping(self.src, self.dest, 1);
                }
            })
        }
    }
}
