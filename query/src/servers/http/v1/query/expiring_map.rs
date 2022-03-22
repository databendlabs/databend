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

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use common_base::tokio::task;
use common_base::tokio::time::sleep;
use common_infallible::RwLock;

use crate::servers::http::v1::query::expirable::Expirable;
use crate::servers::http::v1::query::expirable::ExpiringState;

// todo(youngsofun): use ExpiringMap for HttpQuery

struct MaybeExpiring<V>
where V: Expirable
{
    task: Option<task::JoinHandle<()>>,
    pub value: V,
}

// on insert：start task
//   1. check V for expire
//   2. call remove if expire
// on remove:
//   1. call on_expire
//   2. Cancel task

pub struct ExpiringMap<K, V>
where V: Expirable
{
    inner: Arc<RwLock<Inner<K, V>>>,
}

async fn run_check<T: Expirable>(e: &T, max_idle: Duration) -> bool {
    loop {
        match e.expire_state() {
            ExpiringState::InUse => sleep(max_idle).await,
            ExpiringState::Idle { since } => {
                let now = Instant::now();
                if now - since > max_idle {
                    return true;
                } else {
                    sleep(max_idle - (now - since)).await;
                    continue;
                }
            }
            ExpiringState::Aborted { need_cleanup } => return need_cleanup,
        }
    }
}

impl<K, V> Default for ExpiringMap<K, V>
where V: Expirable
{
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner::default())),
        }
    }
}

struct Inner<K, V>
where V: Expirable
{
    map: HashMap<K, MaybeExpiring<V>>,
}

impl<K, V> Default for Inner<K, V>
where V: Expirable
{
    fn default() -> Self {
        Self {
            map: HashMap::new(),
        }
    }
}

impl<K, V> Inner<K, V>
where
    K: Hash + Eq,
    V: Expirable,
{
    pub fn remove<Q: ?Sized>(&mut self, k: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        if let Some(mut checker) = self.map.remove(k) {
            if let Some(t) = checker.task.take() {
                t.abort()
            }
            checker.value.on_expire();
        }
    }
    pub fn insert(&mut self, k: K, v: MaybeExpiring<V>) {
        self.map.insert(k, v);
    }

    pub fn get<Q: ?Sized>(&self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.map.get(k).map(|i| &i.value)
    }
}

impl<K, V> ExpiringMap<K, V>
where
    K: Hash + Eq,
    V: Expirable + Clone,
{
    pub fn insert(&mut self, k: K, v: V, max_idle_time: Option<Duration>)
    where
        K: Clone + Send + Sync + 'static,
        V: Send + Sync + 'static,
    {
        let mut inner = self.inner.write();
        let task = match max_idle_time {
            Some(d) => {
                let inner = self.inner.clone();
                let v_clone = v.clone();
                let k_clone = k.clone();
                let task = task::spawn(async move {
                    if run_check(&v_clone, d).await {
                        let mut inner = inner.write();
                        inner.remove(&k_clone);
                    }
                });
                Some(task)
            }
            None => None,
        };
        let i = MaybeExpiring { task, value: v };
        inner.insert(k, i);
    }

    pub fn get<Q: ?Sized>(&self, k: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let inner = self.inner.read();
        inner.get(k).cloned()
    }

    pub fn remove<Q: ?Sized>(&mut self, k: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let mut map = self.inner.write();
        map.remove(k)
    }
}
