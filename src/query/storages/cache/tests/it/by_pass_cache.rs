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
use common_storages_cache::ByPassCache;
use common_storages_cache::CacheSettings;
use common_storages_cache::CachedObjectAccessor;
use opendal::services::fs;
use opendal::services::fs::Builder;
use opendal::Operator;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_by_pass_cache() -> Result<()> {
    let mut builder: Builder = fs::Builder::default();
    builder.root("/tmp");
    let op: Operator = Operator::new(builder.build()?);

    let path = uuid::Uuid::new_v4().to_string();
    let object = op.object(&path);

    // Cache.
    let settings = CacheSettings::default();
    let cache = Arc::new(ByPassCache::create(&settings));

    let expect: Arc<Vec<u8>> = Arc::new("hello, by pass".into());

    // Cached Object Accessor.
    let accessor = CachedObjectAccessor::create(cache.clone());
    accessor.write(&object, expect.clone()).await?;

    // Reader.
    let actual: Arc<Vec<u8>> = accessor.read(&object, 0, expect.len() as u64).await?;

    assert_eq!(actual, expect);

    // Remove.
    accessor.remove(&object).await?;

    Ok(())
}
