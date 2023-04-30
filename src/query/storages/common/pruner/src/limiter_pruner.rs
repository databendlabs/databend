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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub trait Limiter {
    fn exceeded(&self) -> bool;
    fn within_limit(&self, n: u64) -> bool;
}

pub struct Unlimited;

impl Limiter for Unlimited {
    fn exceeded(&self) -> bool {
        false
    }

    fn within_limit(&self, _: u64) -> bool {
        true
    }
}

struct U64Limiter {
    limit: u64,
    state: AtomicU64,
}

impl U64Limiter {
    fn new(limit: u64) -> Self {
        Self {
            limit,
            state: AtomicU64::new(0),
        }
    }
}

impl Limiter for U64Limiter {
    fn exceeded(&self) -> bool {
        self.state.load(Ordering::Relaxed) >= self.limit
    }

    fn within_limit(&self, n: u64) -> bool {
        let o = self.state.fetch_add(n, Ordering::Relaxed);
        o < self.limit
    }
}

pub type LimiterPruner = Arc<dyn Limiter + Send + Sync>;

pub struct LimiterPrunerCreator;

impl LimiterPrunerCreator {
    pub fn create(limit: Option<usize>) -> LimiterPruner {
        match limit {
            Some(size) => Arc::new(U64Limiter::new(size as u64)),
            _ => Arc::new(Unlimited),
        }
    }
}
