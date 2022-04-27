// Copyright 2021 Datafuse Labs.
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

use std::alloc::Layout;
use std::borrow::BorrowMut;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use bumpalo::Bump;
use common_exception::Result;
use common_functions::aggregates::StateAddr;
use float_cmp::approx_eq;
use pretty_assertions::assert_eq;

static NUM_DROPPED: AtomicUsize = AtomicUsize::new(0);
static NUM_DROPPED2: AtomicUsize = AtomicUsize::new(0);
struct CountDrops {
    id: usize,
    c: CountDrops2,
}

struct CountDrops2 {
    value: Vec<u8>,
}

impl Drop for CountDrops {
    fn drop(&mut self) {
        NUM_DROPPED.fetch_add(1, Ordering::SeqCst);
    }
}

impl Drop for CountDrops2 {
    fn drop(&mut self) {
        NUM_DROPPED2.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn test_aggregate_state_drop() -> Result<()> {
    let place: StateAddr;
    {
        let bump = Bump::new();
        let layout = Layout::new::<CountDrops>();

        let ptr = bump.alloc_layout(layout);
        place = ptr.into();

        place.write(|| CountDrops {
            id: 1,
            c: CountDrops2 {
                value: vec![1, 2, 3],
            },
        });

        // Leap happens here, the  `CountDrops.value` is never dropped
        place.write(|| CountDrops {
            id: 1,
            c: CountDrops2 {
                value: vec![4, 5, 6],
            },
        });
    }
    assert_eq!(NUM_DROPPED.load(Ordering::SeqCst), 0);
    assert_eq!(NUM_DROPPED2.load(Ordering::SeqCst), 0);

    let count_drops = place.get::<CountDrops>();

    count_drops.c = CountDrops2 {
        value: vec![7, 8, 9],
    };
    assert_eq!(NUM_DROPPED2.load(Ordering::SeqCst), 1);

    unsafe {
        std::ptr::drop_in_place(count_drops);
    }
    assert_eq!(NUM_DROPPED.load(Ordering::SeqCst), 1);
    assert_eq!(NUM_DROPPED2.load(Ordering::SeqCst), 2);
    Ok(())
}
