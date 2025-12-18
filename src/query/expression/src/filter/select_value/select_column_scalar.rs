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

use databend_common_column::bitmap::Bitmap;
use databend_common_exception::Result;

use super::SelectionBuffers;
use crate::LikePattern;
use crate::filter::SelectStrategy;
use crate::filter::Selector;
use crate::types::AccessType;
use crate::types::string::StringColumn;

impl<'a> Selector<'a> {
    // Select indices by comparing scalar and column.
    pub(super) fn select_column_scalar<const FALSE: bool, L, R, C>(
        &self,
        cmp: C,
        column: L::Column,
        scalar: R::ScalarRef<'a>,
        validity: Option<Bitmap>,
        buffers: SelectionBuffers,
    ) -> Result<usize>
    where
        L: AccessType,
        R: AccessType,
        C: Fn(L::ScalarRef<'_>, R::ScalarRef<'_>) -> bool,
    {
        let SelectionBuffers {
            true_selection,
            false_selection,
            mutable_true_idx,
            mutable_false_idx,
            select_strategy,
            count,
        } = buffers;

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
                                    L::index_column_unchecked(&column, idx as usize),
                                    scalar.clone(),
                                );
                            update_index(ret, idx, true_selection, false_selection);
                        }
                    }
                    None => {
                        for i in start..end {
                            let idx = *true_selection.get_unchecked(i);
                            let ret = cmp(
                                L::index_column_unchecked(&column, idx as usize),
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
                                    L::index_column_unchecked(&column, idx as usize),
                                    scalar.clone(),
                                );
                            update_index(ret, idx, true_selection, false_selection);
                        }
                    }
                    None => {
                        for i in start..end {
                            let idx = *false_selection.get_unchecked(i);
                            let ret = cmp(
                                L::index_column_unchecked(&column, idx as usize),
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
                                    L::index_column_unchecked(&column, idx as usize),
                                    scalar.clone(),
                                );
                            update_index(ret, idx, true_selection, false_selection);
                        }
                    }
                    None => {
                        for idx in 0u32..count as u32 {
                            let ret = cmp(
                                L::index_column_unchecked(&column, idx as usize),
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
    pub(super) fn select_column_like<const FALSE: bool, const NOT: bool>(
        &self,
        column: StringColumn,
        like_pattern: &LikePattern,
        validity: Option<Bitmap>,
        buffers: SelectionBuffers,
    ) -> Result<usize> {
        let SelectionBuffers {
            true_selection,
            false_selection,
            mutable_true_idx,
            mutable_false_idx,
            select_strategy,
            count,
        } = buffers;

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
                            for idx in 0u32..count as u32 {
                                let ret = if NOT {
                                    validity.get_bit_unchecked(idx as usize)
                                        && searcher
                                            .search(column.index_unchecked_bytes(idx as usize))
                                            .is_none()
                                } else {
                                    validity.get_bit_unchecked(idx as usize)
                                        && searcher
                                            .search(column.index_unchecked_bytes(idx as usize))
                                            .is_some()
                                };
                                update_index(ret, idx, true_selection, false_selection);
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
                            for idx in 0u32..count as u32 {
                                let ret = if NOT {
                                    searcher
                                        .search(column.index_unchecked_bytes(idx as usize))
                                        .is_none()
                                } else {
                                    searcher
                                        .search(column.index_unchecked_bytes(idx as usize))
                                        .is_some()
                                };
                                update_index(ret, idx, true_selection, false_selection);
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
