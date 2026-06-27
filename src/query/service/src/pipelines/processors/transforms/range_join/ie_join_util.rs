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
use std::cmp::min;

use databend_common_expression::Column;
use databend_common_expression::ScalarRef;

pub(crate) fn order_match(op: &str, v1: &ScalarRef, v2: &ScalarRef) -> bool {
    if v1.is_null() || v2.is_null() {
        return false;
    }
    let order = v1.cmp(v2);
    match op {
        "gt" => order == Ordering::Greater,
        "gte" => order == Ordering::Equal || order == Ordering::Greater,
        "lt" => order == Ordering::Less,
        "lte" => order == Ordering::Less || order == Ordering::Equal,
        _ => unreachable!(),
    }
}

// Exponential search
pub fn probe_l1(l1: &Column, pos: usize, op1: &str) -> usize {
    let mut step = 1;
    let n = l1.len() - 1;
    let mut hi = pos;
    let mut lo = pos;
    let mut off1;
    if matches!(op1, "gte" | "lte") {
        lo -= min(step, lo);
        step *= 2;
        off1 = lo;
        let pos_val = unsafe { l1.index_unchecked(pos) };
        let mut off1_val = unsafe { l1.index_unchecked(off1) };
        while lo > 0 && order_match(op1, &pos_val, &off1_val) {
            hi = lo;
            lo -= min(step, lo);
            step *= 2;
            off1_val = unsafe { l1.index_unchecked(lo) };
        }
    } else {
        hi += min(step, n - hi);
        step *= 2;
        off1 = hi;
        let pos_val = unsafe { l1.index_unchecked(pos) };
        let mut off1_val = unsafe { l1.index_unchecked(off1) };
        while hi < n && !order_match(op1, &pos_val, &off1_val) {
            lo = hi;
            hi += min(step, n - hi);
            step *= 2;
            off1_val = unsafe { l1.index_unchecked(hi) };
        }
    }
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        off1 = mid;
        let pos_val = unsafe { l1.index_unchecked(pos) };
        let off1_val = unsafe { l1.index_unchecked(off1) };

        if order_match(op1, &pos_val, &off1_val) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    lo
}
