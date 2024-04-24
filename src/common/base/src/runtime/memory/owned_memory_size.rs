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

use std::sync::Arc;

// Calculate the memory usage in heap
pub trait OwnedMemoryUsageSize {
    fn owned_memory_usage(&mut self) -> usize;
}

impl<T: OwnedMemoryUsageSize> OwnedMemoryUsageSize for Arc<T> {
    fn owned_memory_usage(&mut self) -> usize {
        // ArcInner { strong:8 bytes, weak:8 bytes, T }
        match Arc::get_mut(self) {
            None => 16,
            Some(v) => 16 + v.owned_memory_usage(),
        }
    }
}

impl<T: OwnedMemoryUsageSize> OwnedMemoryUsageSize for Box<T> {
    fn owned_memory_usage(&mut self) -> usize {
        self.as_mut().owned_memory_usage() + std::mem::size_of::<T>()
    }
}

impl<T: OwnedMemoryUsageSize> OwnedMemoryUsageSize for Vec<T> {
    fn owned_memory_usage(&mut self) -> usize {
        std::mem::size_of::<T>() * self.capacity()
            + (self
            .iter_mut()
            .map(|x| x.owned_memory_usage())
            .sum::<usize>())
    }
}

// impl<T: OwnedMemoryUsageSize> OwnedMemoryUsageSize for dyn T {
//     fn owned_memory_usage(&mut self) -> usize {
//         T::owned_memory_usage(self)
//     }
// }
