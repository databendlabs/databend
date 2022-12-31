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
use common_exception::Result;
use common_storages_cache::CacheSettings;
use common_storages_cache::MemoryItemCache;
use common_storages_cache::ObjectReader;
use common_storages_cache::ObjectWrite;
use opendal::services::fs;
use opendal::services::fs::Builder;
use opendal::Operator;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug)]
struct TestItem {
    a: u64,
    b: u64,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_memory_item_cache() -> Result<()> {
    let mut builder: Builder = fs::Builder::default();
    builder.root("/tmp");
    let op: Operator = Operator::new(builder.build()?);
    let path = "test.object";
    let object = op.object(path);

    // Cache.
    let settings = CacheSettings::default();
    let cache = Arc::new(MemoryItemCache::create(&settings));

    let expect = Arc::new(TestItem { a: 0, b: 0 });
    // Writer.
    let object_writer = ObjectWrite::create(cache.clone());
    object_writer.write(&object, expect.clone()).await?;

    // Reader.
    let object_reader = ObjectReader::create(cache);
    let actual: Arc<TestItem> = object_reader.read(&object, 0, 16).await?;

    assert_eq!(actual, expect);

    object_writer.remove(&object).await?;

    Ok(())
}
