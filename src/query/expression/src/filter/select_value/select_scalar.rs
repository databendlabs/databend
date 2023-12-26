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

use crate::filter::SelectStrategy;
use crate::filter::Selector;

impl<'a> Selector<'a> {
    pub(crate) fn select_boolean_scalar<const TRUE: bool, const FALSE: bool>(
        &self,
        scalar: bool,
        true_selection: &mut [u32],
        false_selection: &mut [u32],
        mutable_true_idx: &mut usize,
        mutable_false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> usize {
        let mut true_idx = *mutable_true_idx;
        let mut false_idx = *mutable_false_idx;
        match select_strategy {
            SelectStrategy::True => unsafe {
                let start = *mutable_true_idx;
                let end = *mutable_true_idx + count;
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
                let start = *mutable_false_idx;
                let end = *mutable_false_idx + count;
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
        let true_count = true_idx - *mutable_true_idx;
        let false_count = false_idx - *mutable_false_idx;
        *mutable_true_idx = true_idx;
        *mutable_false_idx = false_idx;
        if TRUE {
            true_count
        } else {
            count - false_count
        }
    }
}
