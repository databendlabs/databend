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

use std::time::Duration;

use async_trait::async_trait;
use common_base::tokio;
use common_base::GlobalSequence;
use common_containers::ItemManager;
use common_containers::Pool;
use common_tracing::tracing;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_pool() -> anyhow::Result<()> {
    let p = Pool::new(FooMgr {}, Duration::from_millis(10));

    let i3_1 = p.get(&3).await?;
    assert_eq!(3, i3_1.key, "make a new item(3)");

    let i3_2 = p.get(&3).await?;
    assert_eq!(i3_1.seq, i3_2.seq, "item(3) is reused");

    let _i4 = p.get(&4).await?;

    let i5_1 = p.get(&5).await?;
    assert_eq!(2, i5_1.seq, "seq=2 valid for make");

    tracing::info!("--- check() is called when reusing it. re-build() an new one");
    let i5_2 = p.get(&5).await?;
    assert_eq!(
        3, i5_2.seq,
        "seq=2 is dropped by check(), then make() a new item with seq=3"
    );

    tracing::info!("--- check() is not called for new item");
    let i6_1 = p.get(&6).await?;
    assert_eq!(4, i6_1.seq, "seq=4 is valid for make()");

    tracing::info!("--- check() is called when reusing it. re-build() does not succeed");
    let i6_2 = p.get(&6).await;
    assert!(i6_2.is_err(), "seq>=4 can not reuse or make()");

    Ok(())
}

struct FooMgr {}

#[derive(Clone, Debug)]
struct Item {
    pub key: u32,
    pub seq: usize,
}

#[async_trait]
impl ItemManager for FooMgr {
    type Key = u32;
    type Item = Item;
    type Error = anyhow::Error;

    async fn build(&self, key: &Self::Key) -> Result<Self::Item, Self::Error> {
        let seq = GlobalSequence::next();
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
