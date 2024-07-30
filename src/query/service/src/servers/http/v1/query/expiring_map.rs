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

use std::borrow::Borrow;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use databend_common_base::base::tokio::task;
use databend_common_base::base::tokio::time::sleep;

use crate::servers::http::v1::query::expirable::Expirable;
use crate::servers::http::v1::query::expirable::ExpiringState;

// todo(youngsofun): use ExpiringMap for HttpQuery

struct MaybeExpiring<V>
where V: Expirable
{
    task: Option<task::JoinHandle<()>>,
    pub value: V,
}

impl<V> MaybeExpiring<V>
where V: Expirable
{
    pub fn on_expire(&mut self) {
        if let Some(t) = self.task.take() {
            t.abort()
        }
        self.value.on_expire();
    }
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
    map: Arc<DashMap<K, MaybeExpiring<V>>>,
}

async fn run_check<T: Expirable>(e: &T, max_idle: Duration) -> bool {
    loop {
        match e.expire_state() {
            ExpiringState::InUse(_) => sleep(max_idle).await,
            ExpiringState::Idle { idle_time } => {
                if idle_time > max_idle {
                    return true;
                } else {
                    sleep(max_idle - idle_time).await;
                    continue;
                }
            }
            ExpiringState::Aborted { need_cleanup } => return need_cleanup,
        }
    }
}

impl<K, V> Default for ExpiringMap<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Expirable,
{
    fn default() -> Self {
        Self {
            map: Arc::new(DashMap::new()),
        }
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
        let task = match max_idle_time {
            Some(d) => {
                let map_clone = self.map.clone();
                let v_clone = v.clone();
                let k_clone = k.clone();
                let task = databend_common_base::runtime::spawn(async move {
                    if run_check(&v_clone, d).await {
                        Self::remove_inner(&map_clone, &k_clone);
                    }
                });
                Some(task)
            }
            None => None,
        };
        let i = MaybeExpiring { task, value: v };
        self.map.insert(k, i);
    }

    pub fn get<Q>(&self, k: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.map.get(k).map(|i| i.value().value.clone())
    }

    pub fn remove<Q>(&mut self, k: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        Self::remove_inner(&self.map, k)
    }

    fn remove_inner<Q>(map: &Arc<DashMap<K, MaybeExpiring<V>>>, k: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let checker = { map.remove(k) };
        if let Some((_, mut checker)) = checker {
            checker.on_expire()
        }
    }
}
