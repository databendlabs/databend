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
use common_meta_api::KVApiTestSuite;
use common_meta_grpc::MetaGrpcClient;

use crate::init_meta_ut;

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_kv_api_mget() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_tc, addr) = crate::tests::start_metasrv().await?;

    let client = MetaGrpcClient::try_create(addr.as_str(), "root", "xxx").await?;

    KVApiTestSuite {}.kv_mget(&client).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_kv_api_list() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_tc, addr) = crate::tests::start_metasrv().await?;

    let client = MetaGrpcClient::try_create(addr.as_str(), "root", "xxx").await?;

    KVApiTestSuite {}.kv_list(&client).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_kv_api_delete() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_tc, addr) = crate::tests::start_metasrv().await?;

    let client = MetaGrpcClient::try_create(addr.as_str(), "root", "xxx").await?;

    KVApiTestSuite {}.kv_delete(&client).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_kv_api_update() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_tc, addr) = crate::tests::start_metasrv().await?;

    let client = MetaGrpcClient::try_create(addr.as_str(), "root", "xxx").await?;

    KVApiTestSuite {}.kv_update(&client).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_kv_api_update_meta() -> anyhow::Result<()> {
    // Only update meta, do not touch the value part.

    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_tc, addr) = crate::tests::start_metasrv().await?;

    let client = MetaGrpcClient::try_create(addr.as_str(), "root", "xxx").await?;

    KVApiTestSuite {}.kv_meta(&client).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_kv_api_timeout() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_tc, addr) = crate::tests::start_metasrv().await?;

    let client = MetaGrpcClient::try_create(addr.as_str(), "root", "xxx").await?;

    KVApiTestSuite {}.kv_timeout(&client).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_kv_api_write_read() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_tc, addr) = crate::tests::start_metasrv().await?;

    let client = MetaGrpcClient::try_create(addr.as_str(), "root", "xxx").await?;

    KVApiTestSuite {}.kv_write_read(&client).await
}
