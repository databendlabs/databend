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

use databend_common_meta_types::node::Node;
use databend_common_meta_types::raft_types::Membership;
use databend_common_meta_types::raft_types::StoredMembership;
use databend_common_meta_types::seq_value::KVMeta;
use databend_common_meta_types::Endpoint;
use databend_common_meta_types::UpsertKV;
use futures_util::TryStreamExt;
use map_api::map_api::MapApi;
use map_api::map_api_ro::MapApiRO;
use maplit::btreemap;
use openraft::testing::log_id;
use pretty_assertions::assert_eq;
use seq_marked::SeqMarked;

use crate::leveled_store::leveled_map::compacting_data::CompactingData;
use crate::leveled_store::leveled_map::LeveledMap;
use crate::leveled_store::map_api::AsMap;
use crate::leveled_store::sys_data_api::SysDataApiRO;
use crate::sm_v003::sm_v003::SMV003;
use crate::state_machine::ExpireKey;
use crate::state_machine::UserKey;

#[tokio::test]
async fn test_compact_copied_value_and_kv() -> anyhow::Result<()> {
    let mut lm = build_3_levels().await?;

    let mut immutable_levels = lm.freeze_writable().clone();

    {
        let mut compacting_data = CompactingData::new(&mut immutable_levels, None);
        compacting_data.compact_immutable_in_place().await?;
    }

    let compacted = immutable_levels;

    let d = compacted.newest().unwrap().as_ref();

    assert_eq!(compacted.iter_immutable_levels().count(), 1);
    assert_eq!(
        d.last_membership_ref(),
        &StoredMembership::new(
            Some(log_id(3, 3, 3)),
            Membership::new_with_defaults(vec![], [])
        )
    );
    assert_eq!(d.last_applied_ref(), &Some(log_id(3, 3, 3)));
    assert_eq!(
        d.nodes_ref(),
        &btreemap! {3=>Node::new("3", Endpoint::new("3", 3))}
    );

    let got = d
        .as_user_map()
        .range(..)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(got, vec![
        //
        (user_key("a"), SeqMarked::new_normal(1, (None, b("a0")))),
        (user_key("b"), SeqMarked::new_tombstone(4)),
        (user_key("c"), SeqMarked::new_tombstone(6)),
        (user_key("d"), SeqMarked::new_normal(7, (None, b("d2")))),
        (user_key("e"), SeqMarked::new_normal(6, (None, b("e1")))),
    ]);

    let got = d
        .as_expire_map()
        .range(..)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(got, vec![]);

    Ok(())
}

#[tokio::test]
async fn test_compact_expire_index() -> anyhow::Result<()> {
    let mut sm = build_sm_with_expire().await?;

    let compacted = {
        sm.freeze_writable();
        let mut compactor = sm.acquire_compactor().await;
        compactor.compact_immutable_in_place().await?;
        compactor.immutable_levels().clone()
    };

    let d = compacted.newest().unwrap().as_ref();

    let got = d
        .as_user_map()
        .range(..)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(got, vec![
        //
        (
            user_key("a"),
            SeqMarked::new_normal(4, (Some(KVMeta::new_expires_at(15)), b("a1")))
        ),
        (
            user_key("b"),
            SeqMarked::new_normal(2, (Some(KVMeta::new_expires_at(5)), b("b0")))
        ),
        (
            user_key("c"),
            SeqMarked::new_normal(3, (Some(KVMeta::new_expires_at(20)), b("c0")))
        ),
    ]);

    let got = d
        .as_expire_map()
        .range(..)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(got, vec![
        //
        (ExpireKey::new(5_000, 2), SeqMarked::new_normal(2, s("b"))),
        (ExpireKey::new(10_000, 1), SeqMarked::new_tombstone(4)),
        (ExpireKey::new(15_000, 4), SeqMarked::new_normal(4, s("a"))),
        (ExpireKey::new(20_000, 3), SeqMarked::new_normal(3, s("c"))),
    ]);

    Ok(())
}

#[tokio::test]
async fn test_compact_3_level() -> anyhow::Result<()> {
    let mut lm = build_3_levels().await?;

    lm.freeze_writable();

    let mut compactor = lm.acquire_compactor().await;

    let (sys_data, strm) = compactor.compact_into_stream().await?;
    assert_eq!(
        r#"{"last_applied":{"leader_id":{"term":3,"node_id":3},"index":3},"last_membership":{"log_id":{"leader_id":{"term":3,"node_id":3},"index":3},"membership":{"configs":[],"nodes":{}}},"nodes":{"3":{"name":"3","endpoint":{"addr":"3","port":3},"grpc_api_advertise_address":null}},"sequence":7}"#,
        serde_json::to_string(&sys_data).unwrap()
    );

    let got = strm
        .map_ok(|x| serde_json::to_string(&x).unwrap())
        .try_collect::<Vec<_>>()
        .await?;

    assert_eq!(got, vec![
        r#"["kv--/a",{"seq":1,"marked":{"Normal":[1,4,110,117,108,108,2,97,48]}}]"#,
        r#"["kv--/d",{"seq":7,"marked":{"Normal":[1,4,110,117,108,108,2,100,50]}}]"#,
        r#"["kv--/e",{"seq":6,"marked":{"Normal":[1,4,110,117,108,108,2,101,49]}}]"#,
    ]);

    Ok(())
}

#[tokio::test]
async fn test_export_2_level_with_meta() -> anyhow::Result<()> {
    let mut sm = build_sm_with_expire().await?;
    sm.freeze_writable();

    let mut compactor = sm.acquire_compactor().await;

    let (sys_data, strm) = compactor.compact_into_stream().await?;
    let got = strm
        .map_ok(|x| serde_json::to_string(&x).unwrap())
        .try_collect::<Vec<_>>()
        .await?;

    assert_eq!(
        r#"{"last_applied":null,"last_membership":{"log_id":null,"membership":{"configs":[],"nodes":{}}},"nodes":{},"sequence":4}"#,
        serde_json::to_string(&sys_data).unwrap()
    );

    assert_eq!(got, vec![
        r#"["exp-/00000000000000005000/00000000000000000002",{"seq":2,"marked":{"Normal":[1,4,110,117,108,108,1,98]}}]"#,
        r#"["exp-/00000000000000015000/00000000000000000004",{"seq":4,"marked":{"Normal":[1,4,110,117,108,108,1,97]}}]"#,
        r#"["exp-/00000000000000020000/00000000000000000003",{"seq":3,"marked":{"Normal":[1,4,110,117,108,108,1,99]}}]"#,
        r#"["kv--/a",{"seq":4,"marked":{"Normal":[1,16,123,34,101,120,112,105,114,101,95,97,116,34,58,49,53,125,2,97,49]}}]"#,
        r#"["kv--/b",{"seq":2,"marked":{"Normal":[1,15,123,34,101,120,112,105,114,101,95,97,116,34,58,53,125,2,98,48]}}]"#,
        r#"["kv--/c",{"seq":3,"marked":{"Normal":[1,16,123,34,101,120,112,105,114,101,95,97,116,34,58,50,48,125,2,99,48]}}]"#,
    ]);

    Ok(())
}

/// Create multi levels store:
///
/// l2 |         c(D) d
/// l1 |    b(D) c        e
/// l0 | a  b    c    d              // db
async fn build_3_levels() -> anyhow::Result<LeveledMap> {
    let mut lm = LeveledMap::default();
    let sd = lm.writable_mut().sys_data_mut();

    *sd.last_membership_mut() = StoredMembership::new(
        Some(log_id(1, 1, 1)),
        Membership::new_with_defaults(vec![], []),
    );
    *sd.last_applied_mut() = Some(log_id(1, 1, 1));
    *sd.nodes_mut() = btreemap! {1=>Node::new("1", Endpoint::new("1", 1))};

    // internal_seq: 0
    lm.as_user_map_mut()
        .set(user_key("a"), Some((None, b("a0"))))
        .await?;
    lm.as_user_map_mut()
        .set(user_key("b"), Some((None, b("b0"))))
        .await?;
    lm.as_user_map_mut()
        .set(user_key("c"), Some((None, b("c0"))))
        .await?;
    lm.as_user_map_mut()
        .set(user_key("d"), Some((None, b("d0"))))
        .await?;

    lm.freeze_writable();
    let sd = lm.writable_mut().sys_data_mut();

    *sd.last_membership_mut() = StoredMembership::new(
        Some(log_id(2, 2, 2)),
        Membership::new_with_defaults(vec![], []),
    );
    *sd.last_applied_mut() = Some(log_id(2, 2, 2));
    *sd.nodes_mut() = btreemap! {2=>Node::new("2", Endpoint::new("2", 2))};

    // internal_seq: 4
    lm.as_user_map_mut().set(user_key("b"), None).await?;
    lm.as_user_map_mut()
        .set(user_key("c"), Some((None, b("c1"))))
        .await?;
    lm.as_user_map_mut()
        .set(user_key("e"), Some((None, b("e1"))))
        .await?;

    lm.freeze_writable();
    let sd = lm.writable_mut().sys_data_mut();

    *sd.last_membership_mut() = StoredMembership::new(
        Some(log_id(3, 3, 3)),
        Membership::new_with_defaults(vec![], []),
    );
    *sd.last_applied_mut() = Some(log_id(3, 3, 3));
    *sd.nodes_mut() = btreemap! {3=>Node::new("3", Endpoint::new("3", 3))};

    // internal_seq: 6
    lm.as_user_map_mut().set(user_key("c"), None).await?;
    lm.as_user_map_mut()
        .set(user_key("d"), Some((None, b("d2"))))
        .await?;

    Ok(lm)
}

/// The subscript is internal_seq:
///
///    | kv             | expire
///    | ---            | ---
/// l1 | a₄       c₃    |               10,1₄ -> ø    15,4₄ -> a  20,3₃ -> c
/// ------------------------------------------------------------
/// l0 | a₁  b₂         |  5,2₂ -> b    10,1₁ -> a
async fn build_sm_with_expire() -> anyhow::Result<SMV003> {
    let mut sm = SMV003::default();

    let mut a = sm.new_applier();
    a.upsert_kv(&UpsertKV::update("a", b"a0").with_expire_sec(10))
        .await?;
    a.upsert_kv(&UpsertKV::update("b", b"b0").with_expire_sec(5))
        .await?;

    sm.map_mut().freeze_writable();

    let mut a = sm.new_applier();
    a.upsert_kv(&UpsertKV::update("c", b"c0").with_expire_sec(20))
        .await?;
    a.upsert_kv(&UpsertKV::update("a", b"a1").with_expire_sec(15))
        .await?;

    Ok(sm)
}

fn s(x: impl ToString) -> String {
    x.to_string()
}

fn b(x: impl ToString) -> Vec<u8> {
    x.to_string().as_bytes().to_vec()
}

fn user_key(s: impl ToString) -> UserKey {
    UserKey::new(s)
}
