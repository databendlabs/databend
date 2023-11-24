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

//! Test kv_read_v1() API, which handles kv-read request and return result in a stream.

use std::sync::Arc;

use common_meta_client::ClientHandle;
use common_meta_client::Streamed;
use common_meta_kvapi::kvapi::GetKVReq;
use common_meta_kvapi::kvapi::KVApi;
use common_meta_kvapi::kvapi::ListKVReq;
use common_meta_kvapi::kvapi::MGetKVReq;
use common_meta_kvapi::kvapi::UpsertKVReq;
use common_meta_types::protobuf as pb;
use common_meta_types::protobuf::KvMeta;
use common_meta_types::KVMeta;
use common_meta_types::SeqV;
use common_meta_types::With;
use futures::stream::StreamExt;
use futures::TryStreamExt;
use log::info;
use pretty_assertions::assert_eq;
use test_harness::test;

use crate::testing::meta_service_test_harness;

#[test(harness = meta_service_test_harness)]
#[minitrace::trace]
async fn test_kv_read_v1_on_leader() -> anyhow::Result<()> {
    let now_sec = SeqV::<()>::now_sec();

    let (tc, _addr) = crate::tests::start_metasrv().await?;

    let client = tc.grpc_client().await?;

    initialize_kvs(&client, now_sec).await?;
    test_streamed_get(&client, now_sec).await?;
    test_streamed_mget(&client, now_sec).await?;
    test_streamed_list(&client, now_sec).await?;

    Ok(())
}

#[test(harness = meta_service_test_harness)]
#[minitrace::trace]
async fn test_kv_read_v1_on_follower() -> anyhow::Result<()> {
    let now_sec = SeqV::<()>::now_sec();

    let tcs = crate::tests::start_metasrv_cluster(&[0, 1, 2]).await?;

    let client = tcs[0].grpc_client().await?;

    initialize_kvs(&client, now_sec).await?;

    let client = tcs[1].grpc_client().await?;
    test_streamed_get(&client, now_sec).await?;
    test_streamed_mget(&client, now_sec).await?;
    test_streamed_list(&client, now_sec).await?;

    Ok(())
}

/// Initialize kv store for test.
///
/// Insert keys:
/// a(meta), c, c1, c2
async fn initialize_kvs(client: &Arc<ClientHandle>, now_sec: u64) -> anyhow::Result<()> {
    info!("--- prepare keys: a(meta),c,c1,c2");

    let updates = vec![
        UpsertKVReq::insert("a", &b("a")).with(KVMeta::new_expire(now_sec + 10)),
        UpsertKVReq::insert("c", &b("c")),
        UpsertKVReq::insert("c1", &b("c1")),
        UpsertKVReq::insert("c2", &b("c2")),
    ];

    for update in updates {
        client.upsert_kv(update).await?;
    }

    Ok(())
}

/// Test streamed mget on a grpc meta-service client
async fn test_streamed_get(client: &Arc<ClientHandle>, now_sec: u64) -> anyhow::Result<()> {
    info!("--- test streamed get");

    let strm = client.request(Streamed(GetKVReq { key: s("a") })).await?;

    let got = strm.map_err(|e| e.to_string()).collect::<Vec<_>>().await;
    assert_eq!(
        vec![Ok(pb::StreamItem::new(
            s("a"),
            Some(pb::SeqV::with_meta(
                1,
                Some(KvMeta::new_expire(now_sec + 10)),
                b("a")
            ))
        )),],
        got
    );
    Ok(())
}

/// Test streamed mget on a grpc meta-service client
async fn test_streamed_mget(client: &Arc<ClientHandle>, now_sec: u64) -> anyhow::Result<()> {
    info!("--- test streamed mget");

    let strm = client
        .request(Streamed(MGetKVReq {
            keys: vec![s("a"), s("b")],
        }))
        .await?;

    let got = strm.map_err(|e| e.to_string()).collect::<Vec<_>>().await;
    assert_eq!(
        vec![
            Ok(pb::StreamItem::new(
                s("a"),
                Some(pb::SeqV::with_meta(
                    1,
                    Some(KvMeta::new_expire(now_sec + 10)),
                    b("a")
                ))
            )),
            Ok(pb::StreamItem::new(s("b"), None)),
        ],
        got
    );
    Ok(())
}

/// Test streamed list on a grpc meta-service client
async fn test_streamed_list(client: &Arc<ClientHandle>, _now_sec: u64) -> anyhow::Result<()> {
    info!("--- test streamed list");

    let strm = client
        .request(Streamed(ListKVReq { prefix: s("c") }))
        .await?;

    let got = strm.map_err(|e| e.to_string()).collect::<Vec<_>>().await;
    assert_eq!(
        vec![
            Ok(pb::StreamItem::new(s("c"), Some(pb::SeqV::new(2, b("c"))))),
            Ok(pb::StreamItem::new(
                s("c1"),
                Some(pb::SeqV::new(3, b("c1")))
            )),
            Ok(pb::StreamItem::new(
                s("c2"),
                Some(pb::SeqV::new(4, b("c2")))
            )),
        ],
        got
    );
    Ok(())
}

fn s(x: &str) -> String {
    x.to_string()
}

fn b(x: &str) -> Vec<u8> {
    x.as_bytes().to_vec()
}
