// Copyright 2020-2022 Jorge C. Leitão
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

use super::super::utils::Pushable;

/// A [`Pushable`] for fixed sized binary data
#[derive(Debug)]
pub struct FixedSizeBinary {
    pub values: Vec<u8>,
    pub size: usize,
}

impl FixedSizeBinary {
    #[inline]
    pub fn with_capacity(capacity: usize, size: usize) -> Self {
        Self {
            values: Vec::with_capacity(capacity * size),
            size,
        }
    }

    #[inline]
    pub fn push(&mut self, value: &[u8]) {
        debug_assert_eq!(value.len(), self.size);
        self.values.extend(value);
    }

    #[inline]
    pub fn extend_constant(&mut self, additional: usize) {
        self.values
            .resize(self.values.len() + additional * self.size, 0);
    }
}

impl<'a> Pushable<&'a [u8]> for FixedSizeBinary {
    #[inline]
    fn reserve(&mut self, additional: usize) {
        self.values.reserve(additional * self.size);
    }
    #[inline]
    fn push(&mut self, value: &[u8]) {
        debug_assert_eq!(value.len(), self.size);
        self.push(value);
    }

    #[inline]
    fn push_null(&mut self) {
        self.values.extend(std::iter::repeat(0).take(self.size))
    }

    #[inline]
    fn extend_constant(&mut self, additional: usize, value: &[u8]) {
        assert_eq!(value.len(), 0);
        self.extend_constant(additional)
    }

    #[inline]
    fn len(&self) -> usize {
        self.values.len() / self.size
    }
}
