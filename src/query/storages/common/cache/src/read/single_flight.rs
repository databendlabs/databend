// Copyright 2026 Datafuse Labs
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
        // Removes the entry when done, or when this caller is cancelled with no one else
        // waiting; a waiter left behind takes over the load and removes it itself.
        let _cleanup = Cleanup {
            calls: &self.calls,
            key,
            call: call.clone(),
        };
        call.get_or_init(|| load).await.clone()
    }

    pub fn inflight(&self) -> usize {
        self.calls.lock().len()
    }
}

struct Cleanup<'a, V> {
    calls: &'a Mutex<HashMap<String, Call<V>>>,
    key: &'a str,
    call: Call<V>,
}

impl<V> Drop for Cleanup<'_, V> {
    fn drop(&mut self) {
        let mut calls = self.calls.lock();
        let Some(current) = calls.get(self.key) else {
            return;
        };
        // Callers clone the call under this lock: 3 counts are the map, `self.call` and the
        // caller's own clone. More means another caller still waits on it.
        let alone = Arc::strong_count(current) <= 3;
        if Arc::ptr_eq(current, &self.call) && (self.call.initialized() || alone) {
            calls.remove(self.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::time::Duration;

    use super::*;

    /// A cancelled load leaves no entry behind, unless another caller still waits on it.
    #[tokio::test]
    async fn test_cancelled_load_is_removed() {
        let flight = Arc::new(SingleFlight::<u32>::default());
        let cancelled =
            tokio::time::timeout(Duration::from_millis(10), flight.run("k", pending())).await;
        assert!(cancelled.is_err());
        assert_eq!(flight.inflight(), 0);

        // The first caller is cancelled while a second waits: the second takes over.
        let first = databend_common_base::runtime::spawn({
            let flight = flight.clone();
            async move { flight.run("k", pending()).await }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        let second = databend_common_base::runtime::spawn({
            let flight = flight.clone();
            async move { flight.run("k", async { Ok(7) }).await }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        first.abort();
        assert_eq!(second.await.unwrap().unwrap(), 7);
        assert_eq!(flight.inflight(), 0);
    }
}
