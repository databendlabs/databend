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

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use databend_common_exception::Result;
use parking_lot::Mutex;
use tokio::sync::OnceCell;

type Call<V> = Arc<OnceCell<Result<V>>>;

/// Runs at most one load per key at a time; concurrent callers share the result.
/// If the running caller is cancelled, the next waiter runs its own load.
pub struct SingleFlight<V> {
    calls: Mutex<HashMap<String, Call<V>>>,
}

impl<V> Default for SingleFlight<V> {
    fn default() -> Self {
        SingleFlight {
            calls: Mutex::new(HashMap::new()),
        }
    }
}

impl<V: Clone> SingleFlight<V> {
    pub async fn run<F>(&self, key: &str, load: F) -> Result<V>
    where F: Future<Output = Result<V>> {
        let call = self
            .calls
            .lock()
            .entry(key.to_string())
            .or_default()
            .clone();
        let result = call.get_or_init(|| load).await.clone();
        let mut calls = self.calls.lock();
        if calls.get(key).is_some_and(|c| Arc::ptr_eq(c, &call)) {
            calls.remove(key);
        }
        result
    }

    pub fn inflight(&self) -> usize {
        self.calls.lock().len()
    }
}
