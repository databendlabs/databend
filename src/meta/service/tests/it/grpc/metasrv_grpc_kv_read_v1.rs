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

//! Test kv_read_v1() API, which handles kv-read request and return result in a stream.

use std::sync::Arc;
use std::time::Duration;

use databend_common_meta_client::ClientHandle;
use databend_common_meta_client::Streamed;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_kvapi::kvapi::ListKVReq;
use databend_common_meta_kvapi::kvapi::MGetKVReq;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_types::protobuf as pb;
use databend_common_meta_types::protobuf::KvMeta;
use databend_common_meta_types::MetaSpec;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::With;
use futures::stream::StreamExt;
use futures::TryStreamExt;
use log::info;
use pretty_assertions::assert_eq;
use test_harness::test;

use crate::testing::meta_service_test_harness;
use crate::tests::service::make_grpc_client;

#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_kv_read_v1_on_leader() -> anyhow::Result<()> {
    let now_sec = SeqV::<()>::now_sec();

    let (tc, _addr) = crate::tests::start_metasrv().await?;

    let client = tc.grpc_client().await?;

    initialize_kvs(&client).await?;
    test_streamed_mget(&client, now_sec).await?;
    test_streamed_list(&client, now_sec).await?;

    Ok(())
}

#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_kv_read_v1_on_follower() -> anyhow::Result<()> {
    let now_sec = SeqV::<()>::now_sec();

    let tcs = crate::tests::start_metasrv_cluster(&[0, 1, 2]).await?;

    let client = tcs[0].grpc_client().await?;

    initialize_kvs(&client).await?;

    let client = tcs[1].grpc_client().await?;
    test_streamed_mget(&client, now_sec).await?;
    test_streamed_list(&client, now_sec).await?;

    Ok(())
}

/// When invoke kv_read_v1() on a follower, the leader endpoint is responded in the response header.
#[test(harness = meta_service_test_harness)]
#[fastrace::trace]
async fn test_kv_read_v1_follower_responds_leader_endpoint() -> anyhow::Result<()> {
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
    {
        let eclient = client.make_established_client().await?;
        assert_eq!(a1(), eclient.target_endpoint(), "using a1()");
    }
    {
        let eclient = client.make_established_client().await?;
        assert_eq!(
            a1(),
            eclient.target_endpoint(),
            "make client again, still using a1()"
        );
    }

    let _strm = client
        .request(Streamed(MGetKVReq {
            keys: vec![s("a"), s("b")],
        }))
        .await?;

    // Current leader endpoint updated, will connect to a0.
    {
        let eclient = client.make_established_client().await?;
        assert_eq!(a0(), eclient.target_endpoint(),);
    }

    Ok(())
}

/// Initialize kv store for test.
///
/// Insert keys:
/// a(meta), c, c1, c2
async fn initialize_kvs(client: &Arc<ClientHandle>) -> anyhow::Result<()> {
    info!("--- prepare keys: a(meta),c,c1,c2");

    let updates = vec![
        UpsertKVReq::insert("a", &b("a")).with(MetaSpec::new_ttl(Duration::from_secs(10))),
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
async fn test_streamed_mget(client: &Arc<ClientHandle>, now_sec: u64) -> anyhow::Result<()> {
    info!("--- test streamed mget");

    let strm = client
        .request(Streamed(MGetKVReq {
            keys: vec![s("a"), s("b")],
        }))
        .await?;

    let mut got = strm.map_err(|e| e.to_string()).collect::<Vec<_>>().await;
    assert_eq!(2, got.len());

    let v1 = got.remove(0);
    let v2 = got.remove(0);

    // check v1
    {
        let Ok(pb::StreamItem {
            key,
            value: Some(seq_v),
        }) = v1
        else {
            panic!("expecting Some(seq_v): but: {v1:?}");
        };

        assert_eq!(s("a"), key);
        assert_eq!(1, seq_v.seq);
        assert_eq!(b("a"), seq_v.data);
        // check meta
        {
            let KvMeta { expire_at } = seq_v.meta.unwrap();
            let want = now_sec + 10;
            assert!((want..want + 3).contains(&expire_at.unwrap()));
        }
    }

    // check v2
    assert_eq!(v2, Ok(pb::StreamItem::new(s("b"), None)));

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
