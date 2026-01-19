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
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use log::debug;
use log::warn;
use tokio::time::sleep;
use tonic::async_trait;

pub type PoolItem<T> = Arc<tokio::sync::Mutex<Option<T>>>;

/// To build or check an item.
///
/// When an item is requested, ItemManager `build()` one for the pool.
/// When an item is reused, ItemManager `check()` if it is still valid.
#[async_trait]
pub trait ItemManager {
    type Key: Debug;
    type Item;
    type Error: Debug;

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
#[derive(Debug)]
pub struct Pool<Mgr>
where Mgr: ItemManager + Debug
{
    /// The first sleep time when `build()` fails.
    /// The next sleep time is 2 times of the previous one.
    pub initial_retry_interval: Duration,

    /// Pooled items indexed by key.
    pub items: Arc<Mutex<HashMap<Mgr::Key, PoolItem<Mgr::Item>>>>,

    manager: Mgr,

    n_retries: u32,
}

impl<Mgr> Pool<Mgr>
where
    Mgr: ItemManager + Debug,
    Mgr::Key: Clone + Eq + Hash + Send + Debug,
    Mgr::Item: Clone + Sync + Send + Debug,
    Mgr::Error: Sync + Debug,
{
    pub fn new(manager: Mgr, initial_retry_interval: Duration) -> Self {
        Pool {
            initial_retry_interval,
            items: Default::default(),
            manager,
            n_retries: 3,
        }
    }

    #[allow(dead_code)]
    pub fn with_retries(mut self, retries: u32) -> Self {
        self.n_retries = retries;
        self
    }

    /// Return a raw pool item.
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
    #[logcall::logcall(err = "debug")]
    #[fastrace::trace]
    pub async fn get(&self, key: &Mgr::Key) -> Result<Mgr::Item, Mgr::Error> {
        let pool_item = self.get_pool_item(key);

        let mut guard = pool_item.lock().await;
        let item_opt = (*guard).clone();

        if let Some(ref item) = item_opt {
            let check_res = self.manager.check(item.clone()).await;
            debug!("check reused item res: {:?}", check_res);

            if let Ok(itm) = check_res {
                return Ok(itm);
            } else {
                warn!("Pool check reused item failed: {:?}", key);
                // mark broken conn as deleted
                *guard = None;
            }
        }

        let mut interval = self.initial_retry_interval;
        let mut last_err = None;

        for i in 0..self.n_retries {
            if i > 0 {
                sleep(interval).await;
                interval *= 2;
            }

            debug!("build new item of key: {:?}", key);
            let new_item = self.manager.build(key).await;
            debug!("build new item of key res: {:?}", new_item);

            match new_item {
                Ok(x) => {
                    *guard = Some(x.clone());
                    return Ok(x);
                }
                Err(err) => {
                    warn!("Pool build new item failed: {:?}", err);
                    last_err = Some(err);
                }
            }
        }

        Err(last_err.unwrap())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use log::info;
    use tonic::async_trait;

    use super::ItemManager;
    use super::Pool;

    /// A simple sequence generator for tests
    struct TestSequence;

    impl TestSequence {
        fn next() -> usize {
            static SEQ: AtomicUsize = AtomicUsize::new(0);
            SEQ.fetch_add(1, Ordering::SeqCst)
        }

        fn reset() {
            static SEQ: AtomicUsize = AtomicUsize::new(0);
            SEQ.store(0, Ordering::SeqCst);
        }
    }

    #[derive(Clone, Debug)]
    struct Item {
        pub key: u32,
        pub seq: usize,
    }

    // Test from pool.rs: basic pool functionality
    #[derive(Debug)]
    struct FooMgr {}

    #[async_trait]
    impl ItemManager for FooMgr {
        type Key = u32;
        type Item = Item;
        type Error = anyhow::Error;

        async fn build(&self, key: &Self::Key) -> Result<Self::Item, Self::Error> {
            let seq = TestSequence::next();
            if seq > 4 {
                return Err(anyhow::anyhow!("invalid seq: {}", seq));
            }
            Ok(Item { key: *key, seq })
        }

        async fn check(&self, item: Self::Item) -> Result<Self::Item, Self::Error> {
            if item.seq == 2 || item.seq > 3 {
                Err(anyhow::anyhow!("invalid seq: {}", item.seq))
            } else {
                Ok(item)
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_pool() -> anyhow::Result<()> {
        TestSequence::reset();
        let p = Pool::new(FooMgr {}, Duration::from_millis(10));

        let i3_1 = p.get(&3).await?;
        assert_eq!(3, i3_1.key, "make a new item(3)");

        let i3_2 = p.get(&3).await?;
        assert_eq!(i3_1.seq, i3_2.seq, "item(3) is reused");

        let _i4 = p.get(&4).await?;

        let i5_1 = p.get(&5).await?;
        assert_eq!(2, i5_1.seq, "seq=2 valid for make");

        info!("--- check() is called when reusing it. re-build() an new one");
        let i5_2 = p.get(&5).await?;
        assert_eq!(
            3, i5_2.seq,
            "seq=2 is dropped by check(), then make() a new item with seq=3"
        );

        info!("--- check() is not called for new item");
        let i6_1 = p.get(&6).await?;
        assert_eq!(4, i6_1.seq, "seq=4 is valid for make()");

        info!("--- check() is called when reusing it. re-build() does not succeed");
        let i6_2 = p.get(&6).await;
        assert!(i6_2.is_err(), "seq>=4 can not reuse or make()");

        Ok(())
    }

    // Test from pool_retry.rs: retry functionality
    struct LocalRetrySequence;

    impl LocalRetrySequence {
        fn next() -> usize {
            static LOCAL_SEQ: AtomicUsize = AtomicUsize::new(0);
            LOCAL_SEQ.fetch_add(1, Ordering::SeqCst)
        }

        fn reset() {
            static LOCAL_SEQ: AtomicUsize = AtomicUsize::new(0);
            LOCAL_SEQ.store(0, Ordering::SeqCst);
        }
    }

    #[derive(Debug)]
    struct BarMgr {}

    #[async_trait]
    impl ItemManager for BarMgr {
        type Key = u32;
        type Item = Item;
        type Error = anyhow::Error;

        async fn build(&self, key: &Self::Key) -> Result<Self::Item, Self::Error> {
            let retry_times = LocalRetrySequence::next();
            if retry_times < 5 {
                return Err(anyhow::anyhow!("Keep retry!"));
            }
            Ok(Item {
                key: *key,
                seq: retry_times,
            })
        }

        async fn check(&self, item: Self::Item) -> Result<Self::Item, Self::Error> {
            Ok(item)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_pool_retry() -> anyhow::Result<()> {
        LocalRetrySequence::reset();
        let p = Pool::new(BarMgr {}, Duration::from_millis(10)).with_retries(6);
        let r1 = p.get(&3).await?;
        assert_eq!(3, r1.key, "make a new item(3)");
        let r2 = p.get(&3).await?;
        assert_eq!(r1.seq, r2.seq, "item(3) is reused");
        Ok(())
    }
}
