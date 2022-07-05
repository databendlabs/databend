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

pub trait HashTableGrower: Default + Clone {
    fn max_size(&self) -> isize;
    fn overflow(&self, size: usize) -> bool;
    fn place(&self, hash_value: u64) -> isize;
    fn next_place(&self, old_place: isize) -> isize;
    fn increase_size(&mut self);
}

#[derive(Clone)]
pub struct SingleLevelGrower {
    size_degree: u8,
    max_size: isize,
}

impl Default for SingleLevelGrower {
    fn default() -> Self {
        SingleLevelGrower {
            size_degree: 8,
            max_size: 1_isize << 8,
        }
    }
}

impl HashTableGrower for SingleLevelGrower {
    #[inline(always)]
    fn max_size(&self) -> isize {
        self.max_size
    }

    #[inline(always)]
    fn overflow(&self, size: usize) -> bool {
        size > ((1_usize) << (self.size_degree - 1))
    }

    #[inline(always)]
    fn place(&self, hash_value: u64) -> isize {
        hash_value as isize & (self.max_size() - 1)
    }

    #[inline(always)]
    fn next_place(&self, old_place: isize) -> isize {
        (old_place + 1) & (self.max_size() - 1)
    }

    #[inline(always)]
    fn increase_size(&mut self) {
        self.size_degree += if self.size_degree >= 23 { 1 } else { 2 };
        self.max_size = 1_isize << self.size_degree;
    }
}

#[derive(Clone)]
pub struct TwoLevelGrower {
    size_degree: u8,
    max_size: isize,
}

impl Default for TwoLevelGrower {
    fn default() -> Self {
        TwoLevelGrower {
            size_degree: 8,
            max_size: 1_isize << 8,
        }
    }
}

impl HashTableGrower for TwoLevelGrower {
    #[inline(always)]
    fn max_size(&self) -> isize {
        self.max_size
    }

    #[inline(always)]
    fn overflow(&self, size: usize) -> bool {
        size > ((1_usize) << (self.size_degree - 1))
    }

    #[inline(always)]
    fn place(&self, hash_value: u64) -> isize {
        hash_value as isize & (self.max_size() - 1)
    }

    #[inline(always)]
    fn next_place(&self, old_place: isize) -> isize {
        (old_place + 1) & (self.max_size() - 1)
    }

    #[inline(always)]
    fn increase_size(&mut self) {
        self.size_degree += if self.size_degree >= 15 { 1 } else { 2 };
        self.max_size = 1_isize << self.size_degree;
    }
}
