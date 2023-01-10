// Copyright 2023 Datafuse Labs.
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

#[derive(Clone, Debug, Default)]
pub struct ReadOptions {
    /// Prune row groups before reading. Require Chunk level statistics.
    /// Filter row groups don't need to read.
    pub prune_row_groups: bool,
    /// Prune pages before reading. Require Page level statistics.
    /// Filter rows don't need to read.
    pub prune_pages: bool,
}

impl ReadOptions {
    pub fn new() -> Self {
        ReadOptions::default()
    }

    pub fn with_prune_row_groups(mut self, prune: bool) -> Self {
        self.prune_row_groups = prune;
        self
    }

    pub fn with_prune_pages(mut self, prune: bool) -> Self {
        self.prune_pages = prune;
        self
    }

    pub fn need_prune(&self) -> bool {
        self.prune_row_groups || self.prune_pages
    }
}
