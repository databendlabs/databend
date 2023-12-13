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

use common_arrow::arrow::bitmap::Bitmap;

use crate::filter::empty_array_compare_value;
use crate::filter::selection_op;
use crate::filter::SelectOp;
use crate::filter::SelectStrategy;
use crate::Scalar;

#[allow(clippy::too_many_arguments)]
pub fn select_scalars(
    op: SelectOp,
    left: Scalar,
    right: Scalar,
    true_selection: &mut [u32],
    false_selection: (&mut [u32], bool),
    true_idx: &mut usize,
    false_idx: &mut usize,
    select_strategy: SelectStrategy,
    count: usize,
) -> usize {
    let result = selection_op(op)(left, right);
    select_boolean_scalar_adapt(
        result,
        true_selection,
        false_selection,
        true_idx,
        false_idx,
        select_strategy,
        count,
    )
}

pub fn select_boolean_scalar_adapt(
    scalar: bool,
    true_selection: &mut [u32],
    false_selection: (&mut [u32], bool),
    true_idx: &mut usize,
    false_idx: &mut usize,
    select_strategy: SelectStrategy,
    count: usize,
) -> usize {
    let has_true = !true_selection.is_empty();
    let has_false = false_selection.1;
    if has_true && has_false {
        select_boolean_scalar::<true, true>(
            scalar,
            true_selection,
            false_selection.0,
            true_idx,
            false_idx,
            select_strategy,
            count,
        )
    } else if has_true {
        select_boolean_scalar::<true, false>(
            scalar,
            true_selection,
            false_selection.0,
            true_idx,
            false_idx,
            select_strategy,
            count,
        )
    } else {
        select_boolean_scalar::<false, true>(
            scalar,
            true_selection,
            false_selection.0,
            true_idx,
            false_idx,
            select_strategy,
            count,
        )
    }
}

fn select_boolean_scalar<const TRUE: bool, const FALSE: bool>(
    scalar: bool,
    true_selection: &mut [u32],
    false_selection: &mut [u32],
    true_start_idx: &mut usize,
    false_start_idx: &mut usize,
    select_strategy: SelectStrategy,
    count: usize,
) -> usize {
    let mut true_idx = *true_start_idx;
    let mut false_idx = *false_start_idx;
    match select_strategy {
        SelectStrategy::True => unsafe {
            let start = *true_start_idx;
            let end = *true_start_idx + count;
            if scalar {
                if TRUE {
                    for i in start..end {
                        let idx = *true_selection.get_unchecked(i);
                        true_selection[true_idx] = idx;
                        true_idx += 1;
                    }
                }
            } else if FALSE {
                for i in start..end {
                    let idx = *true_selection.get_unchecked(i);
                    false_selection[false_idx] = idx;
                    false_idx += 1;
                }
            }
        },
        SelectStrategy::False => unsafe {
            let start = *false_start_idx;
            let end = *false_start_idx + count;
            if scalar {
                if TRUE {
                    for i in start..end {
                        let idx = *false_selection.get_unchecked(i);
                        true_selection[true_idx] = idx;
                        true_idx += 1;
                    }
                }
            } else if FALSE {
                for i in start..end {
                    let idx = *false_selection.get_unchecked(i);
                    false_selection[false_idx] = idx;
                    false_idx += 1;
                }
            }
        },
        SelectStrategy::All => {
            if scalar {
                if TRUE {
                    for idx in 0u32..count as u32 {
                        true_selection[true_idx] = idx;
                        true_idx += 1;
                    }
                }
            } else if FALSE {
                for idx in 0u32..count as u32 {
                    false_selection[false_idx] = idx;
                    false_idx += 1;
                }
            }
        }
    }
    let true_count = true_idx - *true_start_idx;
    let false_count = false_idx - *false_start_idx;
    *true_start_idx = true_idx;
    *false_start_idx = false_idx;
    if TRUE {
        true_count
    } else {
        count - false_count
    }
}

#[allow(clippy::too_many_arguments)]
pub fn select_empty_array_adapt(
    op: SelectOp,
    validity: Option<Bitmap>,
    true_selection: &mut [u32],
    false_selection: (&mut [u32], bool),
    true_idx: &mut usize,
    false_idx: &mut usize,
    select_strategy: SelectStrategy,
    count: usize,
) -> usize {
    let has_true = !true_selection.is_empty();
    let has_false = false_selection.1;
    if has_true && has_false {
        select_empty_array::<true, true>(
            op,
            validity,
            true_selection,
            false_selection.0,
            true_idx,
            false_idx,
            select_strategy,
            count,
        )
    } else if has_true {
        select_empty_array::<true, false>(
            op,
            validity,
            true_selection,
            false_selection.0,
            true_idx,
            false_idx,
            select_strategy,
            count,
        )
    } else {
        select_empty_array::<false, true>(
            op,
            validity,
            true_selection,
            false_selection.0,
            true_idx,
            false_idx,
            select_strategy,
            count,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn select_empty_array<const TRUE: bool, const FALSE: bool>(
    op: SelectOp,
    validity: Option<Bitmap>,
    true_selection: &mut [u32],
    false_selection: &mut [u32],
    true_start_idx: &mut usize,
    false_start_idx: &mut usize,
    select_strategy: SelectStrategy,
    count: usize,
) -> usize {
    let ret = empty_array_compare_value(&op);
    let mut true_idx = *true_start_idx;
    let mut false_idx = *false_start_idx;
    match select_strategy {
        SelectStrategy::True => unsafe {
            let start = *true_start_idx;
            let end = *true_start_idx + count;
            match validity {
                Some(validity) => {
                    for i in start..end {
                        let idx = *true_selection.get_unchecked(i);
                        let ret = ret & validity.get_bit_unchecked(idx as usize);
                        if TRUE {
                            *true_selection.get_unchecked_mut(true_idx) = idx;
                            true_idx += ret as usize;
                        }
                        if FALSE {
                            *false_selection.get_unchecked_mut(false_idx) = idx;
                            false_idx += !ret as usize;
                        }
                    }
                }
                None => {
                    for i in start..end {
                        let idx = *true_selection.get_unchecked(i);
                        if TRUE {
                            *true_selection.get_unchecked_mut(true_idx) = idx;
                            true_idx += ret as usize;
                        }
                        if FALSE {
                            *false_selection.get_unchecked_mut(false_idx) = idx;
                            false_idx += !ret as usize;
                        }
                    }
                }
            }
        },
        SelectStrategy::False => unsafe {
            let start = *false_start_idx;
            let end = *false_start_idx + count;
            match validity {
                Some(validity) => {
                    for i in start..end {
                        let idx = *false_selection.get_unchecked(i);
                        let ret = ret & validity.get_bit_unchecked(idx as usize);
                        if TRUE {
                            *true_selection.get_unchecked_mut(true_idx) = idx;
                            true_idx += ret as usize;
                        }
                        if FALSE {
                            *false_selection.get_unchecked_mut(false_idx) = idx;
                            false_idx += !ret as usize;
                        }
                    }
                }
                None => {
                    for i in start..end {
                        let idx = *false_selection.get_unchecked(i);
                        if TRUE {
                            *true_selection.get_unchecked_mut(true_idx) = idx;
                            true_idx += ret as usize;
                        }
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
                        let ret = ret & validity.get_bit_unchecked(idx as usize);
                        if TRUE {
                            *true_selection.get_unchecked_mut(true_idx) = idx;
                            true_idx += ret as usize;
                        }
                        if FALSE {
                            *false_selection.get_unchecked_mut(false_idx) = idx;
                            false_idx += !ret as usize;
                        }
                    }
                }
                None => {
                    for idx in 0u32..count as u32 {
                        if TRUE {
                            *true_selection.get_unchecked_mut(true_idx) = idx;
                            true_idx += ret as usize;
                        }
                        if FALSE {
                            *false_selection.get_unchecked_mut(false_idx) = idx;
                            false_idx += !ret as usize;
                        }
                    }
                }
            }
        },
    }
    let true_count = true_idx - *true_start_idx;
    let false_count = false_idx - *false_start_idx;
    *true_start_idx = true_idx;
    *false_start_idx = false_idx;
    if TRUE {
        true_count
    } else {
        count - false_count
    }
}
