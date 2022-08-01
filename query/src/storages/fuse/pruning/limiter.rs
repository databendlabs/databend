//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub trait Limiter {
    fn within_limit(&self, n: usize) -> bool;
}

pub struct Unlimited;

impl Limiter for Unlimited {
    fn within_limit(&self, _: usize) -> bool {
        true
    }
}

impl Limiter for AtomicUsize {
    fn within_limit(&self, n: usize) -> bool {
        let o = self.fetch_sub(n, Ordering::Relaxed);
        o > n
    }
}

pub type LimiterPruner = Arc<dyn Limiter + Send + Sync>;
pub fn new_limiter(limit: Option<usize>) -> LimiterPruner {
    match limit {
        Some(size) => Arc::new(AtomicUsize::new(size)),
        _ => Arc::new(Unlimited),
    }
}
