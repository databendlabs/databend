// Copyright 2020 Parity Technologies
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Estimation for heapsize calculation. Usable to replace call to allocator method (for some
//! allocators or simply because we just need a deterministic cunsumption measurement).

use std::mem::size_of;
use std::mem::size_of_val;
use std::sync::Arc;

use crate::malloc_size::MallocShallowSizeOf;
use crate::malloc_size::MallocSizeOf;
use crate::malloc_size::MallocSizeOfOps;
use crate::malloc_size::MallocUnconditionalShallowSizeOf;

impl<T: ?Sized> MallocShallowSizeOf for Box<T> {
    fn shallow_size_of(&self, _ops: &mut MallocSizeOfOps) -> usize {
        size_of_val(&**self)
    }
}

impl MallocSizeOf for String {
    fn size_of(&self, _ops: &mut MallocSizeOfOps) -> usize {
        self.capacity() * size_of::<u8>()
    }
}

impl<T> MallocShallowSizeOf for Vec<T> {
    fn shallow_size_of(&self, _ops: &mut MallocSizeOfOps) -> usize {
        self.capacity() * size_of::<T>()
    }
}

impl<T> MallocUnconditionalShallowSizeOf for Arc<T> {
    fn unconditional_shallow_size_of(&self, _ops: &mut MallocSizeOfOps) -> usize {
        size_of::<T>()
    }
}
