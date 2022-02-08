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
use common_meta_api::KVApi;
use common_meta_grpc::MetaGrpcClient;
use common_meta_types::protobuf::Empty;
use common_meta_types::MatchSeq;
use common_meta_types::Operation;
use common_meta_types::UpsertKVAction;
use common_tracing::tracing;
use common_tracing::tracing::Instrument;
use regex::Regex;
use tokio_stream::StreamExt;

use crate::init_meta_ut;

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_export() -> anyhow::Result<()> {
    //
    // - Start a metasrv server.
    // - Write some data
    // - Export all data in json and check it.

    let (_log_guards, ut_span) = init_meta_ut!();

    async {
        let (_tc, addr) = crate::tests::start_metasrv().await?;

        let client = MetaGrpcClient::try_create(addr.as_str(), "root", "xxx", None, None).await?;

        tracing::info!("--- upsert kv");
        {
            for k in ["foo", "bar", "wow"] {
                client
                    .upsert_kv(UpsertKVAction::new(
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
            r#"["state",4,"Id",{"NodeId":0}]"#, //
            r#"["state",4,"HardState",{"HardState":{"current_term":1,"voted_for":0}}]"#, //
            r#"["log",1,0,{"log_id":{"term":0,"index":0},"payload":{"Membership":{"configs":[[0]],"all_nodes":[0]}}}]"#, //
            r#"["log",1,1,{"log_id":{"term":1,"index":1},"payload":"Blank"}]"#, //
            r#"["log",1,2,{"log_id":{"term":1,"index":2},"payload":{"Normal":{"txid":null,"cmd":{"AddNode":{"node_id":0,"node":{"name":"","address":"127.0.0.1:29000"}}}}}}]"#, //
            r#"["log",1,3,{"log_id":{"term":1,"index":3},"payload":{"Normal":{"txid":null,"cmd":{"UpsertKV":{"key":"foo","seq":"Any","value":{"Update":[102,111,111]},"value_meta":null}}}}}]"#, //
            r#"["log",1,4,{"log_id":{"term":1,"index":4},"payload":{"Normal":{"txid":null,"cmd":{"UpsertKV":{"key":"bar","seq":"Any","value":{"Update":[98,97,114]},"value_meta":null}}}}}]"#, //
            r#"["log",1,5,{"log_id":{"term":1,"index":5},"payload":{"Normal":{"txid":null,"cmd":{"UpsertKV":{"key":"wow","seq":"Any","value":{"Update":[119,111,119]},"value_meta":null}}}}}]"#, //
            r#"["sm",2,0,{"name":"","address":"127.0.0.1:29000"}]"#, //
            r#"["sm",3,"LastApplied",{"LogId":{"term":1,"index":5}}]"#, //
            r#"["sm",3,"Initialized",{"Bool":true}]"#,               //
            r#"["sm",3,"LastMembership",{"Membership":{"log_id":{"term":0,"index":0},"membership":{"configs":[[0]],"all_nodes":[0]}}}]"#, //
            r#"["sm",6,"bar",{"seq":2,"meta":null,"data":[98,97,114]}]"#, //
            r#"["sm",6,"foo",{"seq":1,"meta":null,"data":[102,111,111]}]"#, //
            r#"["sm",6,"wow",{"seq":3,"meta":null,"data":[119,111,119]}]"#, //
            r#"["sm",7,"generic-kv",3]"#,                                 //
        ];

        // The addresses are built from random number.
        // Wash them.
        let lines = lines
            .iter()
            .map(|x| {
                Regex::new(r"127.0.0.1:29\d\d\d")
                    .unwrap()
                    .replace(x, "127.0.0.1:29000")
                    .to_string()
            })
            .collect::<Vec<_>>();

        assert_eq!(want, lines);

        Ok(())
    }
    .instrument(ut_span)
    .await
}
