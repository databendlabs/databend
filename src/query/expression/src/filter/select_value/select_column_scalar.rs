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

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_exception::Result;

use crate::filter::SelectStrategy;
use crate::filter::Selector;
use crate::types::string::StringColumn;
use crate::types::ValueType;
use crate::LikePattern;

impl<'a> Selector<'a> {
    // Select indices by comparing scalar and column.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn select_column_scalar<
        T: ValueType,
        C: Fn(T::ScalarRef<'_>, T::ScalarRef<'_>) -> bool,
        const FALSE: bool,
    >(
        &self,
        cmp: C,
        column: T::Column,
        scalar: T::ScalarRef<'a>,
        validity: Option<Bitmap>,
        true_selection: &mut [u32],
        false_selection: &mut [u32],
        mutable_true_idx: &mut usize,
        mutable_false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> Result<usize> {
        let mut true_idx = *mutable_true_idx;
        let mut false_idx = *mutable_false_idx;

        let mut update_index = unsafe {
            |ret: bool, idx: u32, true_selection: &mut [u32], false_selection: &mut [u32]| {
                *true_selection.get_unchecked_mut(true_idx) = idx;
                true_idx += ret as usize;
                if FALSE {
                    *false_selection.get_unchecked_mut(false_idx) = idx;
                    false_idx += !ret as usize;
                }
            }
        };

        match select_strategy {
            SelectStrategy::True => unsafe {
                let start = *mutable_true_idx;
                let end = *mutable_true_idx + count;
                match validity {
                    Some(validity) => {
                        for i in start..end {
                            let idx = *true_selection.get_unchecked(i);
                            let ret = validity.get_bit_unchecked(idx as usize)
                                && cmp(
                                    T::index_column_unchecked(&column, idx as usize),
                                    scalar.clone(),
                                );
                            update_index(ret, idx, true_selection, false_selection);
                        }
                    }
                    None => {
                        for i in start..end {
                            let idx = *true_selection.get_unchecked(i);
                            let ret = cmp(
                                T::index_column_unchecked(&column, idx as usize),
                                scalar.clone(),
                            );
                            update_index(ret, idx, true_selection, false_selection);
                        }
                    }
                }
            },
            SelectStrategy::False => unsafe {
                let start = *mutable_false_idx;
                let end = *mutable_false_idx + count;
                match validity {
                    Some(validity) => {
                        for i in start..end {
                            let idx = *false_selection.get_unchecked(i);
                            let ret = validity.get_bit_unchecked(idx as usize)
                                && cmp(
                                    T::index_column_unchecked(&column, idx as usize),
                                    scalar.clone(),
                                );
                            update_index(ret, idx, true_selection, false_selection);
                        }
                    }
                    None => {
                        for i in start..end {
                            let idx = *false_selection.get_unchecked(i);
                            let ret = cmp(
                                T::index_column_unchecked(&column, idx as usize),
                                scalar.clone(),
                            );
                            update_index(ret, idx, true_selection, false_selection);
                        }
                    }
                }
            },
            SelectStrategy::All => unsafe {
                match validity {
                    Some(validity) => {
                        for idx in 0u32..count as u32 {
                            let ret = validity.get_bit_unchecked(idx as usize)
                                && cmp(
                                    T::index_column_unchecked(&column, idx as usize),
                                    scalar.clone(),
                                );
                            update_index(ret, idx, true_selection, false_selection);
                        }
                    }
                    None => {
                        for idx in 0u32..count as u32 {
                            let ret = cmp(
                                T::index_column_unchecked(&column, idx as usize),
                                scalar.clone(),
                            );
                            update_index(ret, idx, true_selection, false_selection);
                        }
                    }
                }
            },
        }

        let true_count = true_idx - *mutable_true_idx;
        *mutable_true_idx = true_idx;
        *mutable_false_idx = false_idx;
        Ok(true_count)
    }

    // Select indices by like pattern.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn select_column_like<const FALSE: bool, const NOT: bool>(
        &self,
        column: StringColumn,
        like_pattern: &LikePattern,
        validity: Option<Bitmap>,
        true_selection: &mut [u32],
        false_selection: &mut [u32],
        mutable_true_idx: &mut usize,
        mutable_false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> Result<usize> {
        let mut true_idx = *mutable_true_idx;
        let mut false_idx = *mutable_false_idx;

        let mut update_index = unsafe {
            |ret: bool, idx: u32, true_selection: &mut [u32], false_selection: &mut [u32]| {
                *true_selection.get_unchecked_mut(true_idx) = idx;
                true_idx += ret as usize;
                if FALSE {
                    *false_selection.get_unchecked_mut(false_idx) = idx;
                    false_idx += !ret as usize;
                }
            }
        };

        match select_strategy {
            SelectStrategy::True => unsafe {
                let start = *mutable_true_idx;
                let end = *mutable_true_idx + count;
                match validity {
                    Some(validity) => {
                        for i in start..end {
                            let idx = *true_selection.get_unchecked(i);
                            let ret = if NOT {
                                validity.get_bit_unchecked(idx as usize)
                                    && !like_pattern
                                        .compare(column.index_unchecked_bytes(idx as usize))
                            } else {
                                validity.get_bit_unchecked(idx as usize)
                                    && like_pattern
                                        .compare(column.index_unchecked_bytes(idx as usize))
                            };
                            update_index(ret, idx, true_selection, false_selection);
                        }
                    }
                    None => {
                        for i in start..end {
                            let idx = *true_selection.get_unchecked(i);
                            let ret = if NOT {
                                !like_pattern.compare(column.index_unchecked_bytes(idx as usize))
                            } else {
                                like_pattern.compare(column.index_unchecked_bytes(idx as usize))
                            };
                            update_index(ret, idx, true_selection, false_selection);
                        }
                    }
                }
            },
            SelectStrategy::False => unsafe {
                let start = *mutable_false_idx;
                let end = *mutable_false_idx + count;
                match validity {
                    Some(validity) => {
                        for i in start..end {
                            let idx = *false_selection.get_unchecked(i);
                            let ret = if NOT {
                                validity.get_bit_unchecked(idx as usize)
                                    && !like_pattern
                                        .compare(column.index_unchecked_bytes(idx as usize))
                            } else {
                                validity.get_bit_unchecked(idx as usize)
                                    && like_pattern
                                        .compare(column.index_unchecked_bytes(idx as usize))
                            };
                            update_index(ret, idx, true_selection, false_selection);
                        }
                    }
                    None => {
                        for i in start..end {
                            let idx = *false_selection.get_unchecked(i);
                            let ret = if NOT {
                                !like_pattern.compare(column.index_unchecked_bytes(idx as usize))
                            } else {
                                like_pattern.compare(column.index_unchecked_bytes(idx as usize))
                            };
                            update_index(ret, idx, true_selection, false_selection);
                        }
                    }
                }
            },
            SelectStrategy::All => unsafe {
                match validity {
                    Some(validity) => {
                        // search the whole string buffer
                        if let LikePattern::SurroundByPercent(searcher) = like_pattern {
                            let needle = searcher.needle();
                            let needle_byte_len = needle.len();
                            let data = column.data().as_slice();
                            let offsets = column.offsets().as_slice();
                            let mut idx = 0;
                            let mut pos = (*offsets.first().unwrap()) as usize;
                            let end = (*offsets.last().unwrap()) as usize;

                            while pos < end && idx < count {
                                if let Some(p) = searcher.search(&data[pos..end]) {
                                    while offsets[idx + 1] as usize <= pos + p {
                                        let ret = NOT && validity.get_bit_unchecked(idx);
                                        update_index(
                                            ret,
                                            idx as u32,
                                            true_selection,
                                            false_selection,
                                        );
                                        idx += 1;
                                    }

                                    // check if the substring is in bound
                                    let ret =
                                        pos + p + needle_byte_len <= offsets[idx + 1] as usize;

                                    let ret = if NOT {
                                        validity.get_bit_unchecked(idx) && !ret
                                    } else {
                                        validity.get_bit_unchecked(idx) && ret
                                    };
                                    update_index(ret, idx as u32, true_selection, false_selection);

                                    pos = offsets[idx + 1] as usize;
                                    idx += 1;
                                } else {
                                    break;
                                }
                            }
                            while idx < count {
                                let ret = NOT && validity.get_bit_unchecked(idx);
                                update_index(ret, idx as u32, true_selection, false_selection);
                                idx += 1;
                            }
                        } else {
                            for idx in 0u32..count as u32 {
                                let ret = if NOT {
                                    validity.get_bit_unchecked(idx as usize)
                                        && !like_pattern
                                            .compare(column.index_unchecked_bytes(idx as usize))
                                } else {
                                    validity.get_bit_unchecked(idx as usize)
                                        && like_pattern
                                            .compare(column.index_unchecked_bytes(idx as usize))
                                };
                                update_index(ret, idx, true_selection, false_selection);
                            }
                        }
                    }
                    None => {
                        // search the whole string buffer
                        if let LikePattern::SurroundByPercent(searcher) = like_pattern {
                            let needle = searcher.needle();
                            let needle_byte_len = needle.len();
                            let data = column.data().as_slice();
                            let offsets = column.offsets().as_slice();
                            let mut idx = 0;
                            let mut pos = (*offsets.first().unwrap()) as usize;
                            let end = (*offsets.last().unwrap()) as usize;

                            while pos < end && idx < count {
                                if let Some(p) = searcher.search(&data[pos..end]) {
                                    while offsets[idx + 1] as usize <= pos + p {
                                        update_index(
                                            NOT,
                                            idx as u32,
                                            true_selection,
                                            false_selection,
                                        );
                                        idx += 1;
                                    }
                                    // check if the substring is in bound
                                    let ret =
                                        pos + p + needle_byte_len <= offsets[idx + 1] as usize;
                                    let ret = if NOT { !ret } else { ret };
                                    update_index(ret, idx as u32, true_selection, false_selection);

                                    pos = offsets[idx + 1] as usize;
                                    idx += 1;
                                } else {
                                    break;
                                }
                            }
                            while idx < count {
                                update_index(NOT, idx as u32, true_selection, false_selection);
                                idx += 1;
                            }
                        } else {
                            for idx in 0u32..count as u32 {
                                let ret = if NOT {
                                    !like_pattern
                                        .compare(column.index_unchecked_bytes(idx as usize))
                                } else {
                                    like_pattern.compare(column.index_unchecked_bytes(idx as usize))
                                };
                                update_index(ret, idx, true_selection, false_selection);
                            }
                        }
                    }
                }
            },
        }

        let true_count = true_idx - *mutable_true_idx;
        *mutable_true_idx = true_idx;
        *mutable_false_idx = false_idx;
        Ok(true_count)
    }
}
