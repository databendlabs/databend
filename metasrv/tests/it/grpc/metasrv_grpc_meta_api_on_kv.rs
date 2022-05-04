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

//! Test metasrv MetaApi on a single node.

use common_base::tokio;
use common_meta_api::MetaApiTestSuite;
use common_meta_grpc::MetaClientOnKV;

use crate::init_meta_ut;
use crate::tests::start_metasrv;

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_meta_api_on_kv_database_create_get_drop() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_tc, addr) = start_metasrv().await?;

    let client = MetaClientOnKV::try_create(addr.as_str(), "root", "xxx", None, None).await?;

    MetaApiTestSuite {}.database_create_get_drop(&client).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_meta_api_on_kv_database_create_get_drop_in_diff_tenant() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_tc, addr) = start_metasrv().await?;

    let client = MetaClientOnKV::try_create(addr.as_str(), "root", "xxx", None, None).await?;

    MetaApiTestSuite {}
        .database_create_get_drop_in_diff_tenant(&client)
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_meta_api_on_kv_database_list() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_tc, addr) = start_metasrv().await?;

    let client = MetaClientOnKV::try_create(addr.as_str(), "root", "xxx", None, None).await?;

    MetaApiTestSuite {}.database_list(&client).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_meta_api_on_kv_database_list_in_diff_tenant() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_tc, addr) = start_metasrv().await?;

    let client = MetaClientOnKV::try_create(addr.as_str(), "root", "xxx", None, None).await?;

    MetaApiTestSuite {}
        .database_list_in_diff_tenant(&client)
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_meta_api_on_kv_table_create_get_drop() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_tc, addr) = start_metasrv().await?;

    let client = MetaClientOnKV::try_create(addr.as_str(), "root", "xxx", None, None).await?;

    MetaApiTestSuite {}.table_create_get_drop(&client).await
}

// Under developing:
/*
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_meta_api_on_kv_table_rename() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_tc, addr) = start_metasrv().await?;

    let client = MetaGrpcClient::try_create(addr.as_str(), "root", "xxx", None, None).await?;

    MetaApiTestSuite {}.table_rename(&client).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_meta_api_on_kv_table_list() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_tc, addr) = start_metasrv().await?;

    let client = MetaGrpcClient::try_create(addr.as_str(), "root", "xxx", None, None).await?;

    MetaApiTestSuite {}.table_list(&client).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_meta_api_on_kv_share_create_get_drop() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_tc, addr) = start_metasrv().await?;

    let client = MetaGrpcClient::try_create(addr.as_str(), "root", "xxx", None, None).await?;

    MetaApiTestSuite {}.share_create_get_drop(&client).await
}
*/
