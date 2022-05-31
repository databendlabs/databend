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

use common_base::base::tokio;
use common_meta_api::SchemaApiTestSuite;
use common_meta_grpc::MetaGrpcClient;

use crate::init_meta_ut;
use crate::tests::start_metasrv;

#[cfg(feature = "create_with_drop_time")]
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_meta_gpc_client_table_drop_out_of_retention_time_history() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_tc, addr) = start_metasrv().await?;

    let client = MetaGrpcClient::try_create(vec![addr], "root", "xxx", None, None)?;

    SchemaApiTestSuite {}
        .table_drop_out_of_retention_time_history(client.as_ref())
        .await
}

#[cfg(feature = "create_with_drop_time")]
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_meta_gpc_client_database_drop_out_of_retention_time_history() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let (_tc, addr) = start_metasrv().await?;

    let client = MetaGrpcClient::try_create(vec![addr], "root", "xxx", None, None)?;

    SchemaApiTestSuite {}
        .database_drop_out_of_retention_time_history(client.as_ref())
        .await
}
