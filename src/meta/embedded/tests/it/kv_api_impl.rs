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

use databend_common_base::base::tokio;
use databend_common_meta_embedded::MetaEmbedded;
use databend_common_meta_kvapi::kvapi;

#[tokio::test(flavor = "multi_thread")]
async fn test_kv_write_read() -> anyhow::Result<()> {
    let kv = MetaEmbedded::new_temp().await?;
    kvapi::TestSuite {}.kv_write_read(&kv).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_kv_delete() -> anyhow::Result<()> {
    let kv = MetaEmbedded::new_temp().await?;
    kvapi::TestSuite {}.kv_delete(&kv).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_kv_update() -> anyhow::Result<()> {
    let kv = MetaEmbedded::new_temp().await?;
    kvapi::TestSuite {}.kv_update(&kv).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_kv_timeout() -> anyhow::Result<()> {
    let kv = MetaEmbedded::new_temp().await?;
    kvapi::TestSuite {}.kv_timeout(&kv).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_kv_meta() -> anyhow::Result<()> {
    let kv = MetaEmbedded::new_temp().await?;
    kvapi::TestSuite {}.kv_meta(&kv).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_kv_list() -> anyhow::Result<()> {
    let kv = MetaEmbedded::new_temp().await?;
    kvapi::TestSuite {}.kv_list(&kv).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_kv_mget() -> anyhow::Result<()> {
    let kv = MetaEmbedded::new_temp().await?;
    kvapi::TestSuite {}.kv_mget(&kv).await
}
