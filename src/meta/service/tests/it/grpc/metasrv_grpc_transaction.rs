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

//! Test special cases of grpc API: transaction().

use databend_common_meta_types::TxnRequest;
use test_harness::test;

use crate::testing::meta_service_test_harness;
use crate::tests::service::make_grpc_client;

/// When invoke transaction() on a follower, the leader endpoint is responded in the response header.
#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_transaction_follower_responds_leader_endpoint() -> anyhow::Result<()> {
    let tcs = crate::tests::start_metasrv_cluster(&[0, 1, 2]).await?;

    let addresses = tcs
        .iter()
        .map(|tc| tc.config.grpc_api_address.clone())
        .collect::<Vec<_>>();

    let a0 = || addresses[0].clone();
    let a1 = || addresses[1].clone();
    let a2 = || addresses[2].clone();

    let client = make_grpc_client(vec![a1(), a2(), a0()])?;
    {
        let eclient = client.make_established_client().await?;
        assert_eq!(a0(), eclient.target_endpoint(),);

        // Start using a1(), a follower, for next RPC
        eclient.endpoints().lock().choose_next();
    }

    // Make client again, connect to a1.
    {
        let eclient = client.make_established_client().await?;
        assert_eq!(a1(), eclient.target_endpoint(),);
    }

    let _res = client
        .request(TxnRequest {
            condition: vec![],
            if_then: vec![],
            else_then: vec![],
        })
        .await?;

    // Current leader endpoint updated, will connect to a0.
    {
        let eclient = client.make_established_client().await?;
        assert_eq!(a0(), eclient.target_endpoint(),);
    }

    Ok(())
}
