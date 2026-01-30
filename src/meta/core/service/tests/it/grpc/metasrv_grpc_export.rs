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

use std::time::Duration;

use databend_common_meta_runtime_api::TokioRuntime;
use databend_common_meta_types::UpsertKV;
use databend_common_meta_types::protobuf as pb;
use log::info;
use pretty_assertions::assert_eq;
use regex::Regex;
use test_harness::test;
use tokio::time::sleep;
use tokio_stream::StreamExt;

use crate::testing::meta_service_test_harness;
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_export() -> anyhow::Result<()> {
    // - Start a metasrv server.
    // - Write some data
    // - Export all data in json and check it.
    //
    // State machine will not be exported, only snapshot will.
    // Thus in this test we'll gonna trigger a snapshot manually.

    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;

    let client = tc.grpc_client().await?;

    info!("--- upsert kv");
    {
        for k in ["foo", "bar", "wow"] {
            client.upsert_kv(UpsertKV::update(k, &b(k))).await?;
        }
    }

    let meta_handle = tc
        .grpc_srv
        .as_ref()
        .map(|grpc_server| grpc_server.get_meta_handle())
        .unwrap();

    meta_handle.handle_trigger_snapshot().await??;

    // Wait for snapshot to be ready
    sleep(Duration::from_secs(2)).await;

    let mut grpc_client = client.make_established_client().await?;

    let exported = grpc_client
        .export_v1(tonic::Request::new(pb::ExportRequest {
            chunk_size: Some(1),
        }))
        .await?;

    let mut stream = exported.into_inner();

    let mut lines = vec![];
    while let Some(chunk_res) = stream.next().await {
        let chunk = chunk_res?;

        assert_eq!(chunk.data.len(), 1);

        lines.extend_from_slice(&chunk.data);
    }

    let want = vec![
        r#"["header",{"DataHeader":{"key":"header","value":{"version":"V004"}}}]"#,
        r#"["raft_log",{"NodeId":0}]"#,
        r#"["raft_log",{"Vote":{"leader_id":{"term":1,"node_id":0},"committed":true}}]"#,
        r#"["raft_log",{"Committed":{"leader_id":{"term":1,"node_id":0},"index":6}}]"#,
        r#"["raft_log",{"Purged":null}]"#,
        r#"["raft_log",{"LogEntry":{"log_id":{"leader_id":{"term":0,"node_id":0},"index":0},"payload":{"Membership":{"configs":[[0]],"nodes":{"0":{}}}}}}]"#,
        r#"["raft_log",{"LogEntry":{"log_id":{"leader_id":{"term":1,"node_id":0},"index":1},"payload":"Blank"}}]"#,
        r#"["raft_log",{"LogEntry":{"log_id":{"leader_id":{"term":1,"node_id":0},"index":2},"payload":{"Normal":{"time_ms":1111111111111,"cmd":{"AddNode":{"node_id":0,"node":{"name":"0","endpoint":{"addr":"localhost","port":29000},"grpc_api_advertise_address":"127.0.0.1:29000"},"overriding":false}}}}}}]"#,
        r#"["raft_log",{"LogEntry":{"log_id":{"leader_id":{"term":1,"node_id":0},"index":3},"payload":{"Membership":{"configs":[[0]],"nodes":{"0":{}}}}}}]"#,
        r#"["raft_log",{"LogEntry":{"log_id":{"leader_id":{"term":1,"node_id":0},"index":4},"payload":{"Normal":{"time_ms":1111111111111,"cmd":{"Transaction":{"condition":[{"key":"foo","expected":2,"target":{"Seq":0}}],"if_then":[{"request":{"Put":{"key":"foo","value":[102,111,111],"prev_value":true,"expire_at":null}}}],"else_then":[{"request":{"Get":{"key":"foo"}}}]}}}}}}]"#,
        r#"["raft_log",{"LogEntry":{"log_id":{"leader_id":{"term":1,"node_id":0},"index":5},"payload":{"Normal":{"time_ms":1111111111111,"cmd":{"Transaction":{"condition":[{"key":"bar","expected":2,"target":{"Seq":0}}],"if_then":[{"request":{"Put":{"key":"bar","value":[98,97,114],"prev_value":true,"expire_at":null}}}],"else_then":[{"request":{"Get":{"key":"bar"}}}]}}}}}}]"#,
        r#"["raft_log",{"LogEntry":{"log_id":{"leader_id":{"term":1,"node_id":0},"index":6},"payload":{"Normal":{"time_ms":1111111111111,"cmd":{"Transaction":{"condition":[{"key":"wow","expected":2,"target":{"Seq":0}}],"if_then":[{"request":{"Put":{"key":"wow","value":[119,111,119],"prev_value":true,"expire_at":null}}}],"else_then":[{"request":{"Get":{"key":"wow"}}}]}}}}}}]"#,
        r#"["state_machine/0",{"Sequences":{"key":"generic-kv","value":3}}]"#,
        r#"["state_machine/0",{"StateMachineMeta":{"key":"LastApplied","value":{"LogId":{"leader_id":{"term":1,"node_id":0},"index":6}}}}]"#,
        r#"["state_machine/0",{"StateMachineMeta":{"key":"LastMembership","value":{"Membership":{"log_id":{"leader_id":{"term":1,"node_id":0},"index":3},"membership":{"configs":[[0]],"nodes":{"0":{}}}}}}}]"#,
        r#"["state_machine/0",{"Nodes":{"key":0,"value":{"name":"0","endpoint":{"addr":"localhost","port":29000},"grpc_api_advertise_address":"127.0.0.1:29000"}}}]"#,
        r#"["state_machine/0",{"GenericKV":{"key":"bar","value":{"seq":2,"meta":{"proposed_at_ms":1111111111111},"data":[98,97,114]}}}]"#,
        r#"["state_machine/0",{"GenericKV":{"key":"foo","value":{"seq":1,"meta":{"proposed_at_ms":1111111111111},"data":[102,111,111]}}}]"#,
        r#"["state_machine/0",{"GenericKV":{"key":"wow","value":{"seq":3,"meta":{"proposed_at_ms":1111111111111},"data":[119,111,119]}}}]"#,
    ];

    // The addresses are built from OS-assigned ports.
    // Normalize them for comparison.
    let lines = lines
        .iter()
        .map(|x| {
            // Normalize port numbers in JSON "port":XXXXX format
            Regex::new(r#""port":\d+"#)
                .unwrap()
                .replace_all(x, r#""port":29000"#)
                .to_string()
        })
        .map(|x| {
            // Normalize port numbers in address 127.0.0.1:XXXXX format
            Regex::new(r"127\.0\.0\.1:\d+")
                .unwrap()
                .replace_all(&x, "127.0.0.1:29000")
                .to_string()
        })
        .map(|x| {
            // Normalize port numbers in address localhost:XXXXX format
            Regex::new(r"localhost:\d+")
                .unwrap()
                .replace_all(&x, "localhost:29000")
                .to_string()
        })
        .map(|x| {
            // Normalize timestamps
            Regex::new(r"\d{13}")
                .unwrap()
                .replace_all(&x, "1111111111111")
                .to_string()
        })
        .collect::<Vec<_>>();

    assert_eq!(want, lines);

    Ok(())
}

fn b(s: impl ToString) -> Vec<u8> {
    s.to_string().into_bytes()
}
