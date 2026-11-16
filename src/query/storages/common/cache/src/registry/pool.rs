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

/// Pools of the shared backend. Each cache kind belongs to one; kinds of a pool share its
/// capacity and eviction order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Pool {
    Meta,
    Index,
    Data,
}

impl Pool {
    pub const ALL: [Pool; 3] = [Pool::Meta, Pool::Index, Pool::Data];

    /// Order in which memory pressure reclaims pools.
    pub const RECLAIM_ORDER: [Pool; 3] = [Pool::Data, Pool::Index, Pool::Meta];

    pub const fn index(self) -> usize {
        self as usize
    }

    pub const fn name(self) -> &'static str {
        match self {
            Pool::Meta => "meta",
            Pool::Index => "index",
            Pool::Data => "data",
        }
    }

    pub fn from_name(name: &str) -> Option<Pool> {
        Pool::ALL.into_iter().find(|p| p.name() == name)
    }
}
