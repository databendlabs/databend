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

use pretty_assertions::assert_eq;
use regex::Regex;
use test_harness::test;

use crate::testing::meta_service_test_harness;
#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_get_client_info() -> anyhow::Result<()> {
    // - Start a metasrv server.
    // - Get client ip

    let (tc, _addr) = crate::tests::start_metasrv().await?;

    let client = tc.grpc_client().await?;

    let resp = client.get_client_info().await?;

    let client_addr = resp.client_addr;

    let masked_addr = Regex::new(r"\d+")
        .unwrap()
        .replace_all(&client_addr, "1")
        .to_string();

    assert_eq!("1.1.1.1:1", masked_addr);

    assert!(resp.server_time > Some(1), "server time is returned");
    Ok(())
}
