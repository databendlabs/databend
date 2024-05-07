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

use std::io;
use std::sync::Arc;

use databend_common_meta_types::snapshot_db::DB;
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

use crate::leveled_store::db_builder::DBBuilder;
use crate::leveled_store::immutable_levels::ImmutableLevels;
use crate::leveled_store::leveled_map::LeveledMap;
use crate::leveled_store::map_api::AsMap;
use crate::leveled_store::map_api::MapApi;
use crate::leveled_store::map_api::MapApiRO;
use crate::leveled_store::sys_data_api::SysDataApiRO;
use crate::marked::Marked;
use crate::sm_v003::sm_v002::SMV003;
use crate::state_machine::ExpireKey;

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_leveled_query_with_db() -> anyhow::Result<()> {
    let (lm, _g) = build_3_levels().await?;

    assert_eq!(lm.curr_seq(), 7);
    assert_eq!(
        lm.last_membership_ref(),
        &StoredMembership::new(Some(log_id(3, 3, 3)), Membership::new(vec![], ()))
    );
    assert_eq!(lm.last_applied_ref(), &Some(log_id(3, 3, 3)));
    assert_eq!(
        lm.nodes_ref(),
        &btreemap! {3=>Node::new("3", Endpoint::new("3", 3))}
    );

    let got = lm
        .str_map()
        .range(..)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(got, vec![
        //
        (s("a"), Marked::new_with_meta(1, b("a0"), None)),
        (s("b"), Marked::new_tombstone(4)),
        (s("c"), Marked::new_tombstone(6)),
        (s("d"), Marked::new_with_meta(7, b("d2"), None)),
        (s("e"), Marked::new_with_meta(6, b("e1"), None)),
    ]);

    assert_eq!(
        lm.str_map().get("a").await?,
        Marked::new_with_meta(1, b("a0"), None)
    );
    assert_eq!(lm.str_map().get("b").await?, Marked::new_tombstone(4));

    let got = lm
        .expire_map()
        .range(..)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(got, vec![]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_leveled_query_with_expire_index() -> anyhow::Result<()> {
    let (sm, _g) = build_sm_with_expire().await?;

    let lm = sm.into_levels();

    assert_eq!(lm.curr_seq(), 4);
    assert_eq!(
        lm.last_membership_ref(),
        &StoredMembership::new(None, Membership::new(vec![], ()))
    );
    assert_eq!(lm.last_applied_ref(), &None);
    assert_eq!(lm.nodes_ref(), &btreemap! {});

    let got = lm
        .str_map()
        .range(..)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
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

    let got = lm
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
        (ExpireKey::new(10_000, 1), Marked::new_tombstone(4)),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_compact() -> anyhow::Result<()> {
    let (mut lm, _g) = build_3_levels().await?;
    lm.freeze_writable();

    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path();
    let path = path.join("temp-compacted");
    compact(&mut lm, path.as_os_str().to_str().unwrap()).await?;

    let db = lm.persisted().unwrap();

    assert_eq!(db.curr_seq(), 7);
    assert_eq!(
        db.last_membership_ref(),
        &StoredMembership::new(Some(log_id(3, 3, 3)), Membership::new(vec![], ()))
    );
    assert_eq!(db.last_applied_ref(), &Some(log_id(3, 3, 3)));
    assert_eq!(
        db.nodes_ref(),
        &btreemap! {3=>Node::new("3", Endpoint::new("3", 3))}
    );

    let got = db
        .str_map()
        .range(..)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(got, vec![
        //
        (s("a"), Marked::new_with_meta(1, b("a0"), None)),
        (s("d"), Marked::new_with_meta(7, b("d2"), None)),
        (s("e"), Marked::new_with_meta(6, b("e1"), None)),
    ]);

    let got = db
        .expire_map()
        .range(..)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(got, vec![]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_compact_expire_index() -> anyhow::Result<()> {
    let (mut sm, _g) = build_sm_with_expire().await?;
    sm.freeze_writable();

    let mut lm = sm.into_levels();

    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path();
    let path = path.join("temp-compacted");
    compact(&mut lm, path.as_os_str().to_str().unwrap()).await?;

    let db = lm.persisted().unwrap();

    assert_eq!(db.curr_seq(), 4);
    assert_eq!(
        db.last_membership_ref(),
        &StoredMembership::new(None, Membership::new(vec![], ()))
    );
    assert_eq!(db.last_applied_ref(), &None);
    assert_eq!(db.nodes_ref(), &btreemap! {});

    let got = db
        .str_map()
        .range(..)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
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

    let got = db
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

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_compact_output_3_level() -> anyhow::Result<()> {
    let (mut lm, _g) = build_3_levels().await?;
    lm.freeze_writable();

    let mut compactor = lm.acquire_compactor().await;

    let (sys_data, strm) = compactor.compact().await?;

    assert_eq!(sys_data.curr_seq(), 7);
    assert_eq!(
        sys_data.last_membership_ref(),
        &StoredMembership::new(Some(log_id(3, 3, 3)), Membership::new(vec![], ()))
    );
    assert_eq!(sys_data.last_applied_ref(), &Some(log_id(3, 3, 3)));
    assert_eq!(
        sys_data.nodes_ref(),
        &btreemap! {3=>Node::new("3", Endpoint::new("3", 3))}
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

/// Create multi levels store:
///
/// l2 |         c(D) d
/// l1 |    b(D) c        e
/// l0 | a  b    c    d              // db
async fn build_3_levels() -> anyhow::Result<(LeveledMap, impl Drop)> {
    let mut lm = LeveledMap::default();
    let sd = lm.writable_mut().sys_data_mut();

    *sd.last_membership_mut() =
        StoredMembership::new(Some(log_id(1, 1, 1)), Membership::new(vec![], ()));
    *sd.last_applied_mut() = Some(log_id(1, 1, 1));
    *sd.nodes_mut() = btreemap! {1=>Node::new("1", Endpoint::new("1", 1))};

    // internal_seq: 0
    lm.str_map_mut().set(s("a"), Some((b("a0"), None))).await?;
    lm.str_map_mut().set(s("b"), Some((b("b0"), None))).await?;
    lm.str_map_mut().set(s("c"), Some((b("c0"), None))).await?;
    lm.str_map_mut().set(s("d"), Some((b("d0"), None))).await?;

    lm.freeze_writable();
    let sd = lm.writable_mut().sys_data_mut();

    *sd.last_membership_mut() =
        StoredMembership::new(Some(log_id(2, 2, 2)), Membership::new(vec![], ()));
    *sd.last_applied_mut() = Some(log_id(2, 2, 2));
    *sd.nodes_mut() = btreemap! {2=>Node::new("2", Endpoint::new("2", 2))};

    // internal_seq: 4
    lm.str_map_mut().set(s("b"), None).await?;
    lm.str_map_mut().set(s("c"), Some((b("c1"), None))).await?;
    lm.str_map_mut().set(s("e"), Some((b("e1"), None))).await?;

    lm.freeze_writable();
    let sd = lm.writable_mut().sys_data_mut();

    *sd.last_membership_mut() =
        StoredMembership::new(Some(log_id(3, 3, 3)), Membership::new(vec![], ()));
    *sd.last_applied_mut() = Some(log_id(3, 3, 3));
    *sd.nodes_mut() = btreemap! {3=>Node::new("3", Endpoint::new("3", 3))};

    // internal_seq: 6
    lm.str_map_mut().set(s("c"), None).await?;
    lm.str_map_mut().set(s("d"), Some((b("d2"), None))).await?;

    // Move the bottom level to db
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path();
    let path = path.join("temp-db");
    move_bottom_to_db(&mut lm, path.to_str().unwrap()).await?;

    Ok((lm, temp_dir))
}

/// The subscript is internal_seq:
///
///    | kv             | expire
///    | ---            | ---
/// l1 | a₄       c₃    |               10,1₄ -> ø    15,4₄ -> a  20,3₃ -> c
/// ------------------------------------------------------------
/// l0 | a₁  b₂         |  5,2₂ -> b    10,1₁ -> a
async fn build_sm_with_expire() -> anyhow::Result<(SMV003, impl Drop)> {
    let mut sm = SMV003::default();

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

    let lm = sm.levels_mut();

    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path();
    let path = path.join("temp-db");
    move_bottom_to_db(lm, path.to_str().unwrap()).await?;
    Ok((sm, temp_dir))
}

/// Build a DB from the bottom level of the immutable levels.
async fn move_bottom_to_db(lm: &mut LeveledMap, path: &str) -> Result<(), io::Error> {
    let mut immutables = lm.immutable_levels_ref().clone();
    let bottom = immutables.levels().remove(0);
    lm.replace_immutable_levels(immutables);

    let bottom = ImmutableLevels::new([bottom]);
    let mut lm2 = LeveledMap::default();
    lm2.replace_immutable_levels(bottom);

    compact(&mut lm2, path).await?;

    *lm.persisted_mut() = lm2.persisted_mut().clone();
    Ok(())
}

async fn compact(lm: &mut LeveledMap, path: &str) -> Result<(), io::Error> {
    lm.freeze_writable();

    let mut compacter = lm.acquire_compactor().await;
    let (sys_data, strm) = compacter.compact().await?;

    let mut db_builder = DBBuilder::new_with_default_config(path)?;
    db_builder.append_kv_stream(strm).await?;

    let r = db_builder.flush(sys_data)?;
    let db = DB::new(path, "1-1-1-1".to_string(), Arc::new(r))?;

    drop(compacter);

    lm.replace_immutable_levels(ImmutableLevels::new([]));
    *lm.persisted_mut() = Some(db);
    Ok(())
}

fn s(x: impl ToString) -> String {
    x.to_string()
}

fn b(x: impl ToString) -> Vec<u8> {
    x.to_string().as_bytes().to_vec()
}
