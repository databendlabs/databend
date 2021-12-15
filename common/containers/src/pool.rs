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

use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use common_base::tokio;
use common_base::tokio::time::sleep;
use common_tracing::tracing;

pub type PoolItem<T> = Arc<tokio::sync::Mutex<Option<T>>>;

/// To build or check an item.
///
/// When an item is requested, ItemManager `build()` one for the pool.
/// When an item is reused, ItemManager `check()` if it is still valid.
#[async_trait]
pub trait ItemManager {
    type Key;
    type Item;
    type Error;

    /// Make a new item to put into the pool.
    ///
    /// An impl should hold that an item returned by `build()` is passed `check()`.
    async fn build(&self, key: &Self::Key) -> Result<Self::Item, Self::Error>;

    /// Check if an existent item still valid.
    ///
    /// E.g.: check if a tcp connection still alive.
    /// If the item is valid, `check` should return it in a Ok().
    /// Otherwise, the item should be dropped and `check` returns an Err().
    async fn check(&self, item: Self::Item) -> Result<Self::Item, Self::Error>;
}

/// Pool assumes the items in it is `Clone`, thus it keeps only one item for each key.
#[allow(clippy::type_complexity)]
pub struct Pool<Mgr>
where Mgr: ItemManager
{
    /// The first sleep time when `build()` fails.
    /// The next sleep time is 2 times of the previous one.
    pub initial_retry_interval: Duration,

    /// Pooled items indexed by key.
    pub items: Arc<Mutex<HashMap<Mgr::Key, PoolItem<Mgr::Item>>>>,

    manager: Mgr,

    err_type: PhantomData<Mgr::Error>,
}

impl<Mgr> Pool<Mgr>
where
    Mgr: ItemManager,
    Mgr::Key: Clone + Eq + Hash + Send + Debug,
    Mgr::Item: Clone + Sync + Send + Debug,
    Mgr::Error: Sync + Debug,
{
    pub fn new(manager: Mgr, initial_retry_interval: Duration) -> Self {
        Pool {
            initial_retry_interval,
            items: Default::default(),
            manager,
            err_type: Default::default(),
        }
    }

    /// Return an raw pool item.
    ///
    /// The returned one may be an uninitialized one, i.e., it contains a None.
    /// The lock for `items` should not be held for long, e.g. when `build()` a new connection, it takes dozen ms.
    fn get_pool_item(&self, key: &Mgr::Key) -> PoolItem<Mgr::Item> {
        let mut items = self.items.lock().unwrap();

        if let Some(item) = items.get(key) {
            item.clone()
        } else {
            let item = PoolItem::default();
            items.insert(key.clone(), item.clone());
            item
        }
    }

    /// Return a item, by cloning an existent one or making a new one.
    ///
    /// When returning an existent one, `check()` will be called on it to ensure it is still valid.
    /// E.g., when returning a tcp connection.
    pub async fn get(&self, key: &Mgr::Key) -> Result<Mgr::Item, Mgr::Error> {
        let pool_item = self.get_pool_item(key);

        let mut guard = pool_item.lock().await;
        let item_opt = (*guard).clone();

        if let Some(ref item) = item_opt {
            let check_res = self.manager.check(item.clone()).await;
            tracing::debug!("check reused item res: {:?}", check_res);

            if let Ok(itm) = check_res {
                return Ok(itm);
            } else {
                // mark broken conn as deleted
                *guard = None;
            }
        }

        let mut interval = self.initial_retry_interval;

        let n_retry = 3;

        for i in 0..n_retry {
            tracing::info!("build new item of key: {:?}", key);

            let new_item = self.manager.build(key).await;

            tracing::info!("build new item of key res: {:?}", new_item);

            match new_item {
                Ok(x) => {
                    *guard = Some(x.clone());
                    return Ok(x);
                }
                Err(err) => {
                    if i == n_retry - 1 {
                        return Err(err);
                    }
                }
            }

            sleep(interval).await;
            interval *= 2;
        }

        unreachable!("the loop should always return!");
    }
}
