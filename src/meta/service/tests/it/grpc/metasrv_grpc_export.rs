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

use std::time::Duration;

use common_base::base::tokio;
use common_meta_api::KVApi;
use common_meta_grpc::MetaGrpcClient;
use common_meta_types::protobuf::Empty;
use common_meta_types::MatchSeq;
use common_meta_types::Operation;
use common_meta_types::UpsertKVReq;
use pretty_assertions::assert_eq;
use regex::Regex;
use tokio_stream::StreamExt;
use tracing::info;

use crate::init_meta_ut;

#[async_entry::test(worker_threads = 3, init = "init_meta_ut!()", tracing_span = "debug")]
async fn test_export() -> anyhow::Result<()> {
    // - Start a metasrv server.
    // - Write some data
    // - Export all data in json and check it.

    let (_tc, addr) = crate::tests::start_metasrv().await?;

    let client = MetaGrpcClient::try_create(
        vec![addr],
        "root",
        "xxx",
        None,
        Some(Duration::from_secs(10)),
        None,
    )?;

    info!("--- upsert kv");
    {
        for k in ["foo", "bar", "wow"] {
            client
                .upsert_kv(UpsertKVReq::new(
                    k,
                    MatchSeq::Any,
                    Operation::Update(k.as_bytes().to_vec()),
                    None,
                ))
                .await?;
        }
    }

    let mut grpc_client = client.make_client().await?;

    let exported = grpc_client.export(tonic::Request::new(Empty {})).await?;

    let mut stream = exported.into_inner();

    let mut lines = vec![];
    while let Some(chunk_res) = stream.next().await {
        let chunk = chunk_res?;

        lines.extend_from_slice(&chunk.data);
    }

    let want = vec![
        r#"["test-29000-raft_state",{"RaftStateKV":{"key":"Id","value":{"NodeId":0}}}]"#,
        r#"["test-29000-raft_state",{"RaftStateKV":{"key":"HardState","value":{"HardState":{"current_term":1,"voted_for":0}}}}]"#,
        r#"["test-29000-raft_log",{"Logs":{"key":0,"value":{"log_id":{"term":0,"index":0},"payload":{"Membership":{"configs":[[0]],"all_nodes":[0]}}}}}]"#,
        r#"["test-29000-raft_log",{"Logs":{"key":1,"value":{"log_id":{"term":1,"index":1},"payload":"Blank"}}}]"#,
        r#"["test-29000-raft_log",{"Logs":{"key":2,"value":{"log_id":{"term":1,"index":2},"payload":{"Normal":{"txid":null,"time_ms":1111111111111,"cmd":{"AddNode":{"node_id":0,"node":{"name":"0","endpoint":{"addr":"localhost","port":29000},"grpc_api_addr":"127.0.0.1:29000"}}}}}}}}]"#,
        r#"["test-29000-raft_log",{"Logs":{"key":3,"value":{"log_id":{"term":1,"index":3},"payload":{"Normal":{"txid":null,"time_ms":1111111111111,"cmd":{"UpsertKV":{"key":"foo","seq":"Any","value":{"Update":[102,111,111]},"value_meta":null}}}}}}}]"#,
        r#"["test-29000-raft_log",{"Logs":{"key":4,"value":{"log_id":{"term":1,"index":4},"payload":{"Normal":{"txid":null,"time_ms":1111111111111,"cmd":{"UpsertKV":{"key":"bar","seq":"Any","value":{"Update":[98,97,114]},"value_meta":null}}}}}}}]"#,
        r#"["test-29000-raft_log",{"Logs":{"key":5,"value":{"log_id":{"term":1,"index":5},"payload":{"Normal":{"txid":null,"time_ms":1111111111111,"cmd":{"UpsertKV":{"key":"wow","seq":"Any","value":{"Update":[119,111,119]},"value_meta":null}}}}}}}]"#,
        r#"["test-29000-state_machine/0",{"Nodes":{"key":0,"value":{"name":"0","endpoint":{"addr":"localhost","port":29000},"grpc_api_addr":"127.0.0.1:29000"}}}]"#,
        r#"["test-29000-state_machine/0",{"StateMachineMeta":{"key":"LastApplied","value":{"LogId":{"term":1,"index":5}}}}]"#,
        r#"["test-29000-state_machine/0",{"StateMachineMeta":{"key":"Initialized","value":{"Bool":true}}}]"#,
        r#"["test-29000-state_machine/0",{"StateMachineMeta":{"key":"LastMembership","value":{"Membership":{"log_id":{"term":0,"index":0},"membership":{"configs":[[0]],"all_nodes":[0]}}}}}]"#,
        r#"["test-29000-state_machine/0",{"GenericKV":{"key":"bar","value":{"seq":2,"meta":null,"data":[98,97,114]}}}]"#,
        r#"["test-29000-state_machine/0",{"GenericKV":{"key":"foo","value":{"seq":1,"meta":null,"data":[102,111,111]}}}]"#,
        r#"["test-29000-state_machine/0",{"GenericKV":{"key":"wow","value":{"seq":3,"meta":null,"data":[119,111,119]}}}]"#,
        r#"["test-29000-state_machine/0",{"Sequences":{"key":"generic-kv","value":3}}]"#,
    ];

    // The addresses are built from random number.
    // Wash them.
    let lines = lines
        .iter()
        .map(|x| {
            Regex::new(r"29\d\d\d")
                .unwrap()
                .replace_all(x, "29000")
                .to_string()
        })
        .map(|x| {
            Regex::new(r"test-29\d\d\d")
                .unwrap()
                .replace_all(&x, "test-29000")
                .to_string()
        })
        .map(|x| {
            Regex::new(r"\d{13}")
                .unwrap()
                .replace_all(&x, "1111111111111")
                .to_string()
        })
        .collect::<Vec<_>>();

    assert_eq!(want, lines);

    Ok(())
}
