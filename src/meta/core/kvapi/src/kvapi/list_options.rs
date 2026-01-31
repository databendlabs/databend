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

/// Options for listing keys with a prefix.
#[derive(Debug, Clone, Copy)]
pub struct ListOptions<'a, P: ?Sized> {
    pub prefix: &'a P,
    pub limit: Option<u64>,
}

impl<'a, P: ?Sized> ListOptions<'a, P> {
    pub fn unlimited(prefix: &'a P) -> Self {
        Self {
            prefix,
            limit: None,
        }
    }

    pub fn limited(prefix: &'a P, limit: u64) -> Self {
        Self {
            prefix,
            limit: Some(limit),
        }
    }

    /// Creates options with an optional limit.
    ///
    /// Use this when you already have an `Option<u64>` limit value.
    pub fn new(prefix: &'a P, limit: Option<u64>) -> Self {
        Self { prefix, limit }
    }

    pub fn with_limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }
}
