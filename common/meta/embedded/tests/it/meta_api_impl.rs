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

use common_base::tokio;
use common_meta_api::MetaApiTestSuite;
use common_meta_embedded::MetaEmbedded;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_embedded_database_create_get_drop() -> anyhow::Result<()> {
    let mt = MetaEmbedded::new_temp().await?;
    MetaApiTestSuite {}.database_create_get_drop(&mt).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_embedded_database_create_get_drop_in_diff_tenant() -> anyhow::Result<()> {
    let mt = MetaEmbedded::new_temp().await?;
    MetaApiTestSuite {}
        .database_create_get_drop_in_diff_tenant(&mt)
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_embedded_database_list() -> anyhow::Result<()> {
    let mt = MetaEmbedded::new_temp().await?;
    MetaApiTestSuite {}.database_list(&mt).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_embedded_database_list_in_diff_tenant() -> anyhow::Result<()> {
    let mt = MetaEmbedded::new_temp().await?;
    MetaApiTestSuite {}.database_list_in_diff_tenant(&mt).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_embedded_table_create_get_drop() -> anyhow::Result<()> {
    let mt = MetaEmbedded::new_temp().await?;
    MetaApiTestSuite {}.table_create_get_drop(&mt).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_embedded_table_list() -> anyhow::Result<()> {
    let mt = MetaEmbedded::new_temp().await?;
    MetaApiTestSuite {}.table_list(&mt).await
}
