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
use common_meta_flight::MetaFlightClient;

use crate::init_meta_ut;
use crate::tests::service::new_metasrv_test_context;
use crate::tests::start_metasrv_with_context;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_api_database_create_get_drop() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let mut tc0 = new_metasrv_test_context(0);
    let mut tc1 = new_metasrv_test_context(1);

    tc1.config.raft_config.single = false;
    tc1.config.raft_config.join = vec![tc0.config.raft_config.raft_api_addr()];

    start_metasrv_with_context(&mut tc0).await?;
    start_metasrv_with_context(&mut tc1).await?;

    let addr0 = tc0.config.flight_api_address.clone();
    let addr1 = tc1.config.flight_api_address.clone();

    let client0 = MetaFlightClient::try_create(addr0.as_str(), "root", "xxx").await?;
    let client1 = MetaFlightClient::try_create(addr1.as_str(), "root", "xxx").await?;

    MetaApiTestSuite {}
        .database_create_get_drop_leader_follower(&client0, &client1)
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_api_list_table() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let mut tc0 = new_metasrv_test_context(0);
    let mut tc1 = new_metasrv_test_context(1);

    tc1.config.raft_config.single = false;
    tc1.config.raft_config.join = vec![tc0.config.raft_config.raft_api_addr()];

    start_metasrv_with_context(&mut tc0).await?;
    start_metasrv_with_context(&mut tc1).await?;

    let addr0 = tc0.config.flight_api_address.clone();
    let addr1 = tc1.config.flight_api_address.clone();

    let client0 = MetaFlightClient::try_create(addr0.as_str(), "root", "xxx").await?;
    let client1 = MetaFlightClient::try_create(addr1.as_str(), "root", "xxx").await?;

    MetaApiTestSuite {}
        .list_table_leader_follower(&client0, &client1)
        .await
}
