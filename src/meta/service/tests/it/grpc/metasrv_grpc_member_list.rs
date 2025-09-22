// Copyright 2025 Datafuse Labs.
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

use pretty_assertions::assert_eq;
use test_harness::test;

use crate::testing::meta_service_test_harness;

#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_member_list() -> anyhow::Result<()> {
    // - Start a metasrv server.
    // - Get member list

    let (tc, _addr) = crate::tests::start_metasrv().await?;

    let client = tc.grpc_client().await?;

    let resp = client.get_member_list().await?;
    println!("member list: {:?}", resp);

    // The member list should contain at least one member (the current node)
    assert!(!resp.data.is_empty(), "member list should not be empty");

    // Each member address should be in the expected format (host:port)
    for member in &resp.data {
        assert!(
            member.contains(':'),
            "member address should contain port: {}",
            member
        );
        // Check that the address has valid format like "127.0.0.1:port" or "host:port"
        let parts: Vec<&str> = member.split(':').collect();
        assert_eq!(
            parts.len(),
            2,
            "member address should have host:port format: {}",
            member
        );

        // Check that port is a valid number
        let port = parts[1].parse::<u16>();
        assert!(
            port.is_ok(),
            "member address should have valid port number: {}",
            member
        );
    }

    Ok(())
}
