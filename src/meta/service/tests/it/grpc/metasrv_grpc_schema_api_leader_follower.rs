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

//! Test metasrv SchemaApi by writing to leader and then reading from a follower.

use databend_common_meta_api::SchemaApiTestSuite;
use test_harness::test;

use crate::testing::meta_service_test_harness;
use crate::tests::service::start_metasrv_cluster;

#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_meta_api_database_create_get_drop() -> anyhow::Result<()> {
    let tcs = start_metasrv_cluster(&[0, 1]).await?;

    let client0 = tcs[0].grpc_client().await?;
    let client1 = tcs[1].grpc_client().await?;

    SchemaApiTestSuite {}
        .database_get_diff_nodes(client0.as_ref(), client1.as_ref())
        .await
}

#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_meta_api_list_database() -> anyhow::Result<()> {
    let tcs = start_metasrv_cluster(&[0, 1]).await?;

    let client0 = tcs[0].grpc_client().await?;
    let client1 = tcs[1].grpc_client().await?;

    SchemaApiTestSuite {}
        .list_database_diff_nodes(client0.as_ref(), client1.as_ref())
        .await
}

#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_meta_api_table_create_get_drop() -> anyhow::Result<()> {
    let tcs = start_metasrv_cluster(&[0, 1]).await?;

    let client0 = tcs[0].grpc_client().await?;
    let client1 = tcs[1].grpc_client().await?;
    SchemaApiTestSuite {}
        .table_get_diff_nodes(client0.as_ref(), client1.as_ref())
        .await
}

#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_meta_api_list_table() -> anyhow::Result<()> {
    let tcs = start_metasrv_cluster(&[0, 1]).await?;

    let client0 = tcs[0].grpc_client().await?;
    let client1 = tcs[1].grpc_client().await?;

    SchemaApiTestSuite {}
        .list_table_diff_nodes(client0.as_ref(), client1.as_ref())
        .await
}
