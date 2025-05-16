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
use crate::filter::SelectStrategy;
use crate::filter::Selector;
use crate::types::AccessType;

impl Selector<'_> {
    // Select indices by comparing two columns.
    pub(super) fn select_columns<const FALSE: bool, L, R, C>(
        &self,
        cmp: C,
        left: L::Column,
        right: R::Column,
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
                                    L::index_column_unchecked(&left, idx as usize),
                                    R::index_column_unchecked(&right, idx as usize),
                                );
                            *true_selection.get_unchecked_mut(true_idx) = idx;
                            true_idx += ret as usize;
                            if FALSE {
                                *false_selection.get_unchecked_mut(false_idx) = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                    None => {
                        for i in start..end {
                            let idx = *true_selection.get_unchecked(i);
                            let ret = cmp(
                                L::index_column_unchecked(&left, idx as usize),
                                R::index_column_unchecked(&right, idx as usize),
                            );
                            *true_selection.get_unchecked_mut(true_idx) = idx;
                            true_idx += ret as usize;
                            if FALSE {
                                *false_selection.get_unchecked_mut(false_idx) = idx;
                                false_idx += !ret as usize;
                            }
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
                                    L::index_column_unchecked(&left, idx as usize),
                                    R::index_column_unchecked(&right, idx as usize),
                                );
                            *true_selection.get_unchecked_mut(true_idx) = idx;
                            true_idx += ret as usize;
                            if FALSE {
                                *false_selection.get_unchecked_mut(false_idx) = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                    None => {
                        for i in start..end {
                            let idx = *false_selection.get_unchecked(i);
                            let ret = cmp(
                                L::index_column_unchecked(&left, idx as usize),
                                R::index_column_unchecked(&right, idx as usize),
                            );
                            *true_selection.get_unchecked_mut(true_idx) = idx;
                            true_idx += ret as usize;
                            if FALSE {
                                *false_selection.get_unchecked_mut(false_idx) = idx;
                                false_idx += !ret as usize;
                            }
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
                                    L::index_column_unchecked(&left, idx as usize),
                                    R::index_column_unchecked(&right, idx as usize),
                                );
                            *true_selection.get_unchecked_mut(true_idx) = idx;
                            true_idx += ret as usize;
                            if FALSE {
                                *false_selection.get_unchecked_mut(false_idx) = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                    None => {
                        for idx in 0u32..count as u32 {
                            let ret = cmp(
                                L::index_column_unchecked(&left, idx as usize),
                                R::index_column_unchecked(&right, idx as usize),
                            );
                            *true_selection.get_unchecked_mut(true_idx) = idx;
                            true_idx += ret as usize;
                            if FALSE {
                                *false_selection.get_unchecked_mut(false_idx) = idx;
                                false_idx += !ret as usize;
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

    pub(super) fn select_boolean_column<const FALSE: bool>(
        &self,
        column: Bitmap,
        buffers: SelectionBuffers,
    ) -> usize {
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
        match select_strategy {
            SelectStrategy::True => unsafe {
                let start = *mutable_true_idx;
                let end = *mutable_true_idx + count;
                for i in start..end {
                    let idx = *true_selection.get_unchecked(i);
                    let ret = column.get_bit_unchecked(idx as usize);
                    *true_selection.get_unchecked_mut(true_idx) = idx;
                    true_idx += ret as usize;
                    if FALSE {
                        *false_selection.get_unchecked_mut(false_idx) = idx;
                        false_idx += !ret as usize;
                    }
                }
            },
            SelectStrategy::False => unsafe {
                let start = *mutable_false_idx;
                let end = *mutable_false_idx + count;
                for i in start..end {
                    let idx = *false_selection.get_unchecked(i);
                    let ret = column.get_bit_unchecked(idx as usize);
                    *true_selection.get_unchecked_mut(true_idx) = idx;
                    true_idx += ret as usize;
                    if FALSE {
                        *false_selection.get_unchecked_mut(false_idx) = idx;
                        false_idx += !ret as usize;
                    }
                }
            },
            SelectStrategy::All => unsafe {
                for idx in 0u32..count as u32 {
                    let ret = column.get_bit_unchecked(idx as usize);
                    *true_selection.get_unchecked_mut(true_idx) = idx;
                    true_idx += ret as usize;
                    if FALSE {
                        *false_selection.get_unchecked_mut(false_idx) = idx;
                        false_idx += !ret as usize;
                    }
                }
            },
        }
        let true_count = true_idx - *mutable_true_idx;
        *mutable_true_idx = true_idx;
        *mutable_false_idx = false_idx;
        true_count
    }
}
