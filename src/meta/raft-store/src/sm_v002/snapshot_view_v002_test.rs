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

use databend_common_meta_types::Endpoint;
use databend_common_meta_types::KVMeta;
use databend_common_meta_types::Membership;
use databend_common_meta_types::Node;
use databend_common_meta_types::StoredMembership;
use databend_common_meta_types::UpsertKV;
use futures_util::TryStreamExt;
use maplit::btreemap;
use openraft::testing::log_id;
use pretty_assertions::assert_eq;

use crate::key_spaces::SMEntry;
use crate::leveled_store::immutable::Immutable;
use crate::leveled_store::immutable_levels::ImmutableLevels;
use crate::leveled_store::leveled_map::LeveledMap;
use crate::leveled_store::map_api::AsMap;
use crate::leveled_store::map_api::MapApi;
use crate::leveled_store::map_api::MapApiRO;
use crate::leveled_store::sys_data_api::SysDataApiRO;
use crate::marked::Marked;
use crate::sm_v002::sm_v002::SMV002;
use crate::sm_v002::SnapshotViewV002;
use crate::state_machine::ExpireKey;

#[tokio::test]
async fn test_compact_copied_value_and_kv() -> anyhow::Result<()> {
    let mut l = build_3_levels().await?;

    let frozen = l.freeze_writable().clone();

    let mut snapshot = SnapshotViewV002::new(frozen);

    snapshot.compact_mem_levels().await?;

    let top_level = snapshot.compacted();

    let d = top_level.newest().unwrap().as_ref();

    assert_eq!(top_level.iter_immutable_levels().count(), 1);
    assert_eq!(
        d.last_membership_ref(),
        &StoredMembership::new(Some(log_id(3, 3, 3)), Membership::new(vec![], ()))
    );
    assert_eq!(d.last_applied_ref(), &Some(log_id(3, 3, 3)));
    assert_eq!(
        d.nodes_ref(),
        &btreemap! {3=>Node::new("3", Endpoint::new("3", 3))}
    );

    let got = d.str_map().range(..).await?.try_collect::<Vec<_>>().await?;
    assert_eq!(got, vec![
        //
        (s("a"), Marked::new_with_meta(1, b("a0"), None)),
        (s("d"), Marked::new_with_meta(7, b("d2"), None)),
        (s("e"), Marked::new_with_meta(6, b("e1"), None)),
    ]);

    let got = d
        .expire_map()
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

    let mut snapshot = sm.full_snapshot_view();

    snapshot.compact_mem_levels().await?;

    let compacted = snapshot.compacted();

    let d = compacted.newest().unwrap().as_ref();

    let got = d.str_map().range(..).await?.try_collect::<Vec<_>>().await?;
    assert_eq!(got, vec![
        //
        (
            s("a"),
            Marked::new_with_meta(4, b("a1"), Some(KVMeta::new_expire(15)))
        ),
        (
            s("b"),
            Marked::new_with_meta(2, b("b0"), Some(KVMeta::new_expire(5)))
        ),
        (
            s("c"),
            Marked::new_with_meta(3, b("c0"), Some(KVMeta::new_expire(20)))
        ),
    ]);

    let got = d
        .expire_map()
        .range(..)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(got, vec![
        //
        (
            ExpireKey::new(5_000, 2),
            Marked::new_with_meta(2, s("b"), None)
        ),
        (
            ExpireKey::new(15_000, 4),
            Marked::new_with_meta(4, s("a"), None)
        ),
        (
            ExpireKey::new(20_000, 3),
            Marked::new_with_meta(3, s("c"), None)
        ),
    ]);

    Ok(())
}

#[tokio::test]
async fn test_export_3_level() -> anyhow::Result<()> {
    let mut l = build_3_levels().await?;

    let frozen = l.freeze_writable().clone();

    let snapshot = SnapshotViewV002::new(frozen);
    let got = snapshot
        .export()
        .await?
        .map_ok(|x| serde_json::to_string(&x).unwrap())
        .try_collect::<Vec<_>>()
        .await?;

    // TODO(1): add tree name: ["state_machine/0",{"Sequences":{"key":"generic-kv","value":159}}]

    assert_eq!(got, vec![
        r#"{"DataHeader":{"key":"header","value":{"version":"V002","upgrading":null}}}"#,
        r#"{"StateMachineMeta":{"key":"LastApplied","value":{"LogId":{"leader_id":{"term":3,"node_id":3},"index":3}}}}"#,
        r#"{"StateMachineMeta":{"key":"LastMembership","value":{"Membership":{"log_id":{"leader_id":{"term":3,"node_id":3},"index":3},"membership":{"configs":[],"nodes":{}}}}}}"#,
        r#"{"Sequences":{"key":"generic-kv","value":7}}"#,
        r#"{"Nodes":{"key":3,"value":{"name":"3","endpoint":{"addr":"3","port":3},"grpc_api_advertise_address":null}}}"#,
        r#"{"GenericKV":{"key":"a","value":{"seq":1,"meta":null,"data":[97,48]}}}"#,
        r#"{"GenericKV":{"key":"d","value":{"seq":7,"meta":null,"data":[100,50]}}}"#,
        r#"{"GenericKV":{"key":"e","value":{"seq":6,"meta":null,"data":[101,49]}}}"#,
    ]);

    Ok(())
}

#[tokio::test]
async fn test_export_2_level_with_meta() -> anyhow::Result<()> {
    let mut sm = build_sm_with_expire().await?;

    let snapshot = sm.full_snapshot_view();

    let got = snapshot
        .export()
        .await?
        .map_ok(|x| serde_json::to_string(&x).unwrap())
        .try_collect::<Vec<_>>()
        .await?;

    assert_eq!(got, vec![
        r#"{"DataHeader":{"key":"header","value":{"version":"V002","upgrading":null}}}"#,
        r#"{"StateMachineMeta":{"key":"LastMembership","value":{"Membership":{"log_id":null,"membership":{"configs":[],"nodes":{}}}}}}"#,
        r#"{"Sequences":{"key":"generic-kv","value":4}}"#,
        r#"{"GenericKV":{"key":"a","value":{"seq":4,"meta":{"expire_at":15},"data":[97,49]}}}"#,
        r#"{"GenericKV":{"key":"b","value":{"seq":2,"meta":{"expire_at":5},"data":[98,48]}}}"#,
        r#"{"GenericKV":{"key":"c","value":{"seq":3,"meta":{"expire_at":20},"data":[99,48]}}}"#,
        r#"{"Expire":{"key":{"time_ms":5000,"seq":2},"value":{"seq":2,"key":"b"}}}"#,
        r#"{"Expire":{"key":{"time_ms":15000,"seq":4},"value":{"seq":4,"key":"a"}}}"#,
        r#"{"Expire":{"key":{"time_ms":20000,"seq":3},"value":{"seq":3,"key":"c"}}}"#,
    ]);

    Ok(())
}

#[tokio::test]
async fn test_import() -> anyhow::Result<()> {
    let exported = vec![
        r#"{"DataHeader":{"key":"header","value":{"version":"V002","upgrading":null}}}"#,
        r#"{"StateMachineMeta":{"key":"LastApplied","value":{"LogId":{"leader_id":{"term":3,"node_id":3},"index":3}}}}"#,
        r#"{"StateMachineMeta":{"key":"LastMembership","value":{"Membership":{"log_id":{"leader_id":{"term":3,"node_id":3},"index":3},"membership":{"configs":[],"nodes":{}}}}}}"#,
        r#"{"Sequences":{"key":"generic-kv","value":9}}"#,
        r#"{"Nodes":{"key":3,"value":{"name":"3","endpoint":{"addr":"3","port":3},"grpc_api_advertise_address":null}}}"#,
        r#"{"GenericKV":{"key":"a","value":{"seq":7,"meta":{"expire_at":15},"data":[97,49]}}}"#,
        r#"{"GenericKV":{"key":"b","value":{"seq":3,"meta":{"expire_at":5},"data":[98,48]}}}"#,
        r#"{"GenericKV":{"key":"c","value":{"seq":5,"meta":{"expire_at":20},"data":[99,48]}}}"#,
        r#"{"Expire":{"key":{"time_ms":5000,"seq":3},"value":{"seq":3,"key":"b"}}}"#,
        r#"{"Expire":{"key":{"time_ms":15000,"seq":7},"value":{"seq":7,"key":"a"}}}"#,
        r#"{"Expire":{"key":{"time_ms":20000,"seq":5},"value":{"seq":5,"key":"c"}}}"#,
    ];
    let data = exported
        .iter()
        .map(|x| serde_json::from_str::<SMEntry>(x).unwrap());

    let d = {
        let mut importer = SMV002::new_importer();

        for ent in data {
            importer.import(ent)?;
        }

        importer.commit()
    };

    let snapshot = SnapshotViewV002::new(ImmutableLevels::new([Immutable::new_from_level(d)]));

    let got = snapshot
        .export()
        .await?
        .map_ok(|x| serde_json::to_string(&x).unwrap())
        .try_collect::<Vec<_>>()
        .await?;

    assert_eq!(got, exported);

    Ok(())
}

/// Create multi levels store:
///
/// l2 |         c(D) d
/// l1 |    b(D) c        e
/// l0 | a  b    c    d
async fn build_3_levels() -> anyhow::Result<LeveledMap> {
    let mut l = LeveledMap::default();
    let sd = l.writable_mut().sys_data_mut();

    *sd.last_membership_mut() =
        StoredMembership::new(Some(log_id(1, 1, 1)), Membership::new(vec![], ()));
    *sd.last_applied_mut() = Some(log_id(1, 1, 1));
    *sd.nodes_mut() = btreemap! {1=>Node::new("1", Endpoint::new("1", 1))};

    // internal_seq: 0
    l.str_map_mut().set(s("a"), Some((b("a0"), None))).await?;
    l.str_map_mut().set(s("b"), Some((b("b0"), None))).await?;
    l.str_map_mut().set(s("c"), Some((b("c0"), None))).await?;
    l.str_map_mut().set(s("d"), Some((b("d0"), None))).await?;

    l.freeze_writable();
    let sd = l.writable_mut().sys_data_mut();

    *sd.last_membership_mut() =
        StoredMembership::new(Some(log_id(2, 2, 2)), Membership::new(vec![], ()));
    *sd.last_applied_mut() = Some(log_id(2, 2, 2));
    *sd.nodes_mut() = btreemap! {2=>Node::new("2", Endpoint::new("2", 2))};

    // internal_seq: 4
    l.str_map_mut().set(s("b"), None).await?;
    l.str_map_mut().set(s("c"), Some((b("c1"), None))).await?;
    l.str_map_mut().set(s("e"), Some((b("e1"), None))).await?;

    l.freeze_writable();
    let sd = l.writable_mut().sys_data_mut();

    *sd.last_membership_mut() =
        StoredMembership::new(Some(log_id(3, 3, 3)), Membership::new(vec![], ()));
    *sd.last_applied_mut() = Some(log_id(3, 3, 3));
    *sd.nodes_mut() = btreemap! {3=>Node::new("3", Endpoint::new("3", 3))};

    // internal_seq: 6
    l.str_map_mut().set(s("c"), None).await?;
    l.str_map_mut().set(s("d"), Some((b("d2"), None))).await?;

    Ok(l)
}

/// The subscript is internal_seq:
///
///    | kv             | expire
///    | ---            | ---
/// l1 | a₄       c₃    |               10,1₄ -> ø    15,4₄ -> a  20,3₃ -> c
/// ------------------------------------------------------------
/// l0 | a₁  b₂         |  5,2₂ -> b    10,1₁ -> a
async fn build_sm_with_expire() -> anyhow::Result<SMV002> {
    let mut sm = SMV002::default();

    let mut a = sm.new_applier();
    a.upsert_kv(&UpsertKV::update("a", b"a0").with_expire_sec(10))
        .await?;
    a.upsert_kv(&UpsertKV::update("b", b"b0").with_expire_sec(5))
        .await?;

    sm.levels.freeze_writable();

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
