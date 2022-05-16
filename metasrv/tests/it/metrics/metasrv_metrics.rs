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

/*
use common_base::base::tokio;
use common_meta_api::KVApi;
use common_meta_grpc::MetaGrpcClient;
use common_meta_types::protobuf::watch_request::FilterType;
use common_meta_types::protobuf::Event;
use common_meta_types::protobuf::SeqV;
use common_meta_types::protobuf::WatchRequest;
use common_meta_types::MatchSeq;
use common_meta_types::Operation;
use common_meta_types::UpsertKVAction;
use databend_meta::metrics::init_meta_metrics_recorder;

use crate::init_meta_ut;

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_metrics() -> anyhow::Result<()> {
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();
    let (_tc, addr) = crate::tests::start_metasrv().await?;

    init_meta_metrics_recorder();

    Ok(())
}
*/
