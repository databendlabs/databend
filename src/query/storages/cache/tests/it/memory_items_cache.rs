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

use std::sync::Arc;

use common_base::base::tokio;
use common_base::base::uuid;
use common_exception::Result;
use common_storages_cache::CacheSettings;
use common_storages_cache::CachedObject;
use common_storages_cache::CachedObjectAccessor;
use common_storages_cache::MemoryItemsCache;
use opendal::services::fs;
use opendal::services::fs::Builder;
use opendal::Operator;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug, Clone)]
struct TestMeta {
    a: u64,
    b: u64,
}

impl CachedObject for TestMeta {
    fn from_bytes(bs: Vec<u8>) -> Result<Arc<Self>> {
        let t = serde_json::from_slice(&bs).unwrap();
        Ok(Arc::new(t))
    }

    fn to_bytes(self: Arc<Self>) -> Result<Vec<u8>> {
        let data = serde_json::to_vec(&self).unwrap();
        Ok(data)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_memory_items_cache() -> Result<()> {
    let mut builder: Builder = fs::Builder::default();
    builder.root("/tmp");
    let op: Operator = Operator::new(builder.build()?);

    let path = uuid::Uuid::new_v4().to_string();
    let object = op.object(&path);

    // Cache.
    let settings = CacheSettings::default();
    let cache = Arc::new(MemoryItemsCache::<TestMeta>::create(&settings));

    let expect = Arc::from(TestMeta { a: 0, b: 0 });

    // Cached Object Accessor.
    let accessor = CachedObjectAccessor::create(cache.clone());
    accessor.write(&object, expect.clone()).await?;

    let actual = accessor.read(&object, 0, 16).await?;

    assert_eq!(actual, expect);

    // Remove.
    accessor.remove(&object).await?;

    Ok(())
}
