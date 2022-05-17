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
use common_meta_api::KVApi;
use common_meta_grpc::MetaGrpcClient;
use common_meta_types::protobuf::watch_request::FilterType;
use common_meta_types::protobuf::WatchRequest;
use common_meta_types::MatchSeq;
use common_meta_types::Operation;
use common_meta_types::UpsertKVAction;
use databend_meta::metrics::init_meta_metrics_recorder;
use databend_meta::metrics::meta_metrics_to_json;

use crate::init_meta_ut;

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_metrics() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();
    let (_tc, addr) = crate::tests::start_metasrv().await?;

    init_meta_metrics_recorder();

    {
        let json = meta_metrics_to_json();

        let expected = serde_json::json!({
            "has_leader": 1,
            "is_leader": 1,
            "leader_changes": 1,
            "applying_snapshot": 0,
            "proposals_applied": 2,
            "proposals_pending": 0,
            "proposals_failed": 0,
            "read_failed": 0,
            "watchers": 0,
        });

        assert_eq!(json, expected);
    }

    // write a msg
    let client = MetaGrpcClient::try_create(vec![addr.clone()], "root", "xxx", None, None).await?;

    let _res = client
        .upsert_kv(UpsertKVAction::new(
            "foo",
            MatchSeq::Any,
            Operation::Update(b"bar".to_vec()),
            None,
        ))
        .await;

    // test that proposals_applied + 1
    {
        let json = meta_metrics_to_json();
        let expected = serde_json::json!({
            "has_leader": 1,
            "is_leader": 1,
            "leader_changes": 1,
            "applying_snapshot": 0,
            "proposals_applied": 3,
            "proposals_pending": 0,
            "proposals_failed": 0,
            "read_failed": 0,
            "watchers": 0,
        });
        assert_eq!(json, expected);
    }

    // add a watcher
    let mut grpc_client = client.make_conn().await?;

    let watch = WatchRequest {
        key: "a".to_string(),
        key_end: Some("z".to_string()),
        filter_type: FilterType::All.into(),
    };
    let request = tonic::Request::new(watch);

    let mut _client_stream = grpc_client.watch(request).await?.into_inner();

    // test that watchers + 1
    {
        let json = meta_metrics_to_json();
        let expected = serde_json::json!({
            "has_leader": 1,
            "is_leader": 1,
            "leader_changes": 1,
            "applying_snapshot": 0,
            "proposals_applied": 3,
            "proposals_pending": 0,
            "proposals_failed": 0,
            "read_failed": 0,
            "watchers": 1,
        });
        assert_eq!(json, expected);
    }

    Ok(())
}
