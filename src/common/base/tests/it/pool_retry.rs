// Copyright 2022 Datafuse Labs.
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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_trait::async_trait;
use databend_common_base::base::tokio;
use databend_common_base::containers::ItemManager;
use databend_common_base::containers::Pool;

pub struct LocalSequence;

impl LocalSequence {
    pub fn next() -> usize {
        static LOCAL_SEQ: AtomicUsize = AtomicUsize::new(0);

        LOCAL_SEQ.fetch_add(1, Ordering::SeqCst)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_pool() -> anyhow::Result<()> {
    let p = Pool::new(BarMgr {}, Duration::from_millis(10)).with_retries(6);
    let r1 = p.get(&3).await?;
    assert_eq!(3, r1.key, "make a new item(3)");
    let r2 = p.get(&3).await?;
    assert_eq!(r1.seq, r2.seq, "item(3) is reused");
    Ok(())
}

#[derive(Debug)]
struct BarMgr {}

#[derive(Clone, Debug)]
struct Item {
    pub key: u32,
    pub seq: usize,
}

#[async_trait]
impl ItemManager for BarMgr {
    type Key = u32;
    type Item = Item;
    type Error = anyhow::Error;

    async fn build(&self, key: &Self::Key) -> Result<Self::Item, Self::Error> {
        let retry_times = LocalSequence::next();
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
