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

//! `seq` is a key-space in meta for generating kv-record version numbers.
//! It can also be used by apps to generate mono incremental seq numbers.

use common_base::base::tokio;
use common_meta_types::protobuf::raft_service_client::RaftServiceClient;
use common_meta_types::AppliedState;
use common_meta_types::Cmd;
use common_meta_types::LogEntry;
use common_meta_types::RetryableError;
use databend_meta::meta_service::MetaNode;

use crate::init_meta_ut;
use crate::tests::service::MetaSrvTestContext;

#[async_entry::test(worker_threads = 3, init = "init_meta_ut!()", tracing_span = "debug")]
async fn test_meta_node_incr_seq() -> anyhow::Result<()> {
    let tc = MetaSrvTestContext::new(0);
    let addr = tc.config.raft_config.raft_api_addr().await?;

    let _mn = MetaNode::boot(&tc.config).await?;
    tc.assert_raft_server_connection().await?;

    let mut client = RaftServiceClient::connect(format!("http://{}", addr)).await?;

    let cases = common_meta_raft_store::state_machine::testing::cases_incr_seq();

    for (name, txid, k, want) in cases.iter() {
        let req = LogEntry {
            txid: txid.clone(),
            time_ms: None,
            cmd: Cmd::IncrSeq { key: k.to_string() },
        };
        let raft_reply = client.write(req).await?.into_inner();

        let res: Result<AppliedState, RetryableError> = raft_reply.into();
        let resp: AppliedState = res?;
        match resp {
            AppliedState::Seq { seq } => {
                assert_eq!(*want, seq, "{}", name);
            }
            _ => {
                panic!("not Seq")
            }
        }
    }

    Ok(())
}
