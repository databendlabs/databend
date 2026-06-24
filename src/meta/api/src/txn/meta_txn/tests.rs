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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use chrono::DateTime;
use chrono::Utc;
use databend_common_meta_app::KeyExistsBuilder;
use databend_common_meta_app::KeyUnknownBuilder;
use databend_common_meta_app::app_error::TxnRetryMaxTimes;
use databend_common_meta_app::schema::CatalogIdIdent;
use databend_common_meta_app::schema::CatalogMeta;
use databend_common_meta_app::schema::CatalogOption;
use databend_common_meta_app::schema::DBIdTableName;
use databend_common_meta_app::schema::DatabaseId;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::schema::HiveCatalogOption;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_client::kvapi::StructKey;
use databend_meta_client::types::KVMeta;
use databend_meta_client::types::SeqV;
use databend_meta_client::types::TxnCondition;

use super::KvApiOrUserError;
use super::MetaTxn;
use super::MetaTxnManager;
use super::MoveKeyError;
use super::mem_kv::MemKV;
use crate::kv_pb_api::encode_pb;
use crate::txn::op_builder::txn_del;
use crate::txn::op_builder::txn_put_pb_with_ttl;

fn tenant() -> Tenant {
    Tenant::new_literal("test")
}

fn key(id: u64) -> CatalogIdIdent {
    CatalogIdIdent::new(tenant(), id)
}

fn meta() -> CatalogMeta {
    meta_at("127.0.0.1:10000")
}

fn meta_at(address: &str) -> CatalogMeta {
    CatalogMeta {
        catalog_option: CatalogOption::Hive(HiveCatalogOption {
            address: address.to_string(),
            storage_params: None,
        }),
        created_on: DateTime::<Utc>::MIN_UTC,
    }
}

fn encoded() -> Vec<u8> {
    encode_pb(&meta())
}

/// A re-read returns the snapshot cached on the first read, even after the
/// backend record changes underneath — the second read never reaches the backend.
#[tokio::test]
async fn test_read_is_cached() -> anyhow::Result<()> {
    let mem = MemKV::new();
    let k = key(1);
    mem.seed(&k.to_string_key(), 7, encoded());

    let txn = MetaTxn::new(&mem);

    // First read snapshots the key at seq 7.
    let first = txn.get_for_update(k.clone()).await?;
    assert_eq!(first.seq_v().map(|s| s.seq), Some(7));
    assert_eq!(first.value(), Some(&meta()));

    // Replace the backend record underneath the transaction: a fresh read would
    // now see seq 9 with a different value.
    mem.seed(
        &k.to_string_key(),
        9,
        encode_pb(&meta_at("127.0.0.1:29999")),
    );

    // The transaction serves its cached snapshot, not the new backend record.
    let second = txn.get_for_update(k.clone()).await?;
    assert_eq!(second.seq(), 7, "cached snapshot, not backend seq 9");
    assert_eq!(
        second.into_value(),
        Some(meta()),
        "cached value, not the changed one"
    );

    assert_eq!(mem.read_count(), 1, "backend read only once");
    Ok(())
}

/// Reads of one key collapse to a single `eq_seq` guard however many times, or
/// however, it is read; a plain `get` of another key arms none.
#[tokio::test]
async fn test_for_update_becomes_guard() -> anyhow::Result<()> {
    let mem = MemKV::new();
    let (k, other) = (key(1), key(2));
    mem.seed(&k.to_string_key(), 7, encoded());
    mem.seed(&other.to_string_key(), 5, encoded());

    let txn = MetaTxn::new(&mem);
    let commit = txn.clone();

    // The same key read several times — plain and for update — yields one guard.
    txn.get(k.clone()).await?;
    txn.get_for_update(k.clone()).await?;
    let fu = txn.get_for_update(k.clone()).await?;
    fu.stage_put(&meta());

    // A plain get of another key arms no guard.
    txn.get(other.clone()).await?;

    assert!(commit.execute().await?.0);

    let req = mem.last_request();
    assert_eq!(
        req.condition,
        vec![TxnCondition::eq_seq(k.to_string_key(), 7)],
        "one guard for the for-update key, none for the plain gets",
    );
    assert_eq!(req.if_then.len(), 1);
    Ok(())
}

/// A plain `get` records no guard; only the staged write reaches the request.
#[tokio::test]
async fn test_plain_get_adds_no_guard() -> anyhow::Result<()> {
    let mem = MemKV::new();
    let (k1, k2) = (key(1), key(2));
    mem.seed(&k1.to_string_key(), 7, encoded());

    let txn = MetaTxn::new(&mem);
    let commit = txn.clone();
    txn.get(k1.clone()).await?;
    txn.stage_unconditional_put(&k2, &meta());
    commit.execute().await?;

    let req = mem.last_request();
    assert!(req.condition.is_empty(), "plain get must not add a guard");
    assert_eq!(req.if_then.len(), 1);
    Ok(())
}

/// A plain `get` followed by a for-update read upgrades the key to a guard,
/// without a second backend read.
#[tokio::test]
async fn test_get_then_for_update_upgrades() -> anyhow::Result<()> {
    let mem = MemKV::new();
    let k = key(1);
    mem.seed(&k.to_string_key(), 7, encoded());

    let txn = MetaTxn::new(&mem);
    let commit = txn.clone();
    txn.get(k.clone()).await?;
    let fu = txn.get_for_update(k.clone()).await?;
    fu.stage_put(&meta());
    commit.execute().await?;

    assert_eq!(mem.read_count(), 1, "no second backend read");
    let req = mem.last_request();
    assert_eq!(req.condition, vec![TxnCondition::eq_seq(
        k.to_string_key(),
        7
    )]);
    Ok(())
}

/// `run` retries when a commit fails, re-reading from the backend each attempt.
#[tokio::test]
async fn test_run_retries_on_conflict() -> anyhow::Result<()> {
    let mem = MemKV::new();
    let k = key(1);
    mem.seed(&k.to_string_key(), 7, encoded());
    mem.script([false, true]);

    let calls = AtomicUsize::new(0);
    let calls = &calls;
    let res = MetaTxnManager::new(&mem, "test")
        .run(|txn| {
            let k = k.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                let fu = txn.get_for_update(k).await?;
                fu.stage_put(&meta());
                Ok::<_, KvApiOrUserError<_, ()>>(())
            }
        })
        .await;

    assert!(res.is_ok());
    assert_eq!(calls.load(Ordering::SeqCst), 2, "closure ran twice");
    assert_eq!(mem.read_count(), 2, "re-read from backend on retry");
    Ok(())
}

/// Several `FetchedRecord` handles can be held at once: each borrows the
/// transaction shared-ly, so a batch of keys can be read for update, inspected,
/// then written. Each for-update read still contributes its own `eq_seq` guard.
#[tokio::test]
async fn test_multiple_for_update_held() -> anyhow::Result<()> {
    let mem = MemKV::new();
    let (k1, k2) = (key(1), key(2));
    mem.seed(&k1.to_string_key(), 7, encoded());
    mem.seed(&k2.to_string_key(), 9, encoded());

    let txn = MetaTxn::new(&mem);
    let commit = txn.clone();

    let a = txn.get_for_update(k1.clone()).await?;
    let b = txn.get_for_update(k2.clone()).await?;
    assert_eq!(a.seq(), 7);
    assert_eq!(b.seq(), 9);
    a.stage_put(&meta());
    b.stage_delete();

    commit.execute().await?;

    let req = mem.last_request();
    assert_eq!(req.condition.len(), 2);
    assert!(
        req.condition
            .contains(&TxnCondition::eq_seq(k1.to_string_key(), 7))
    );
    assert!(
        req.condition
            .contains(&TxnCondition::eq_seq(k2.to_string_key(), 9))
    );
    assert_eq!(req.if_then.len(), 2);
    Ok(())
}

/// The record's `meta` is fetched and decoded with the value, and cached: a
/// re-read keeps the original meta even after the backend record loses it.
#[tokio::test]
async fn test_meta_is_cached() -> anyhow::Result<()> {
    let mem = MemKV::new();
    let k = key(1);

    // Seed a record carrying meta (expire_at + proposed_at_ms).
    let seeded_meta = KVMeta::new(Some(1_700_000_000), Some(1_700_000_000_000));
    mem.seed_seqv(
        &k.to_string_key(),
        SeqV::new_with_meta(7, Some(seeded_meta.clone()), encoded()),
    );

    let txn = MetaTxn::new(&mem);

    // First read decodes the value and carries the meta.
    let first = txn.get_for_update(k.clone()).await?;
    assert_eq!(
        first.seq_v().and_then(|s| s.meta.as_ref()),
        Some(&seeded_meta)
    );

    // Replace the backend record with one that has no meta; the cached snapshot
    // still carries the original meta.
    mem.seed_seqv(&k.to_string_key(), SeqV::new(9, encoded()));
    let second = txn.get_for_update(k.clone()).await?;
    assert_eq!(
        second.seq_v().and_then(|s| s.meta.as_ref()),
        Some(&seeded_meta),
        "meta served from cache, not the backend",
    );
    assert_eq!(second.seq(), 7);
    assert_eq!(mem.read_count(), 1);
    Ok(())
}

/// `stage_put` and `stage_delete` each stage the matching operation, keyed by
/// its target.
#[tokio::test]
async fn test_writes_generate_operations() -> anyhow::Result<()> {
    let mem = MemKV::new();
    let (k1, k2) = (key(1), key(2));

    let txn = MetaTxn::new(&mem);
    let commit = txn.clone();
    txn.stage_unconditional_put(&k1, &meta());
    txn.stage_delete(&k2);
    commit.execute().await?;

    let req = mem.last_request();
    assert_eq!(req.if_then, vec![
        txn_put_pb_with_ttl(&k1, &meta(), None),
        txn_del(&k2),
    ]);
    Ok(())
}

/// `some_or_unknown` yields a present-only handle when the key exists, and the
/// key's own unknown error (unconverted) when it is absent. `DatabaseId` keys
/// are used for their stable, concrete error `Display`.
#[tokio::test]
async fn test_some_or_unknown() -> anyhow::Result<()> {
    let mem = MemKV::new();
    let (present, absent) = (DatabaseId::new(7), DatabaseId::new(8));
    mem.seed(
        &present.to_string_key(),
        3,
        encode_pb(&DatabaseMeta::default()),
    );

    let txn = MetaTxn::new(&mem);

    let fu = txn.get_for_update(present).await?;
    let present = fu.some_or_unknown("ctx").unwrap();
    assert_eq!(present.seq(), 3);
    assert_eq!(present.seq_v().seq, 3);

    let fu = txn.get_for_update(absent).await?;
    let err = match fu.some_or_unknown("ctx") {
        Ok(_) => panic!("expected unknown key"),
        Err(err) => err,
    };
    assert_eq!(err.to_string(), "UnknownDatabaseId: `8` while `ctx`");
    Ok(())
}

/// `none_or_exist` yields an absent-only handle when the key is absent, and the
/// key's own already-exists error (unconverted) when it is present.
#[tokio::test]
async fn test_none_or_exist() -> anyhow::Result<()> {
    let mem = MemKV::new();
    let (absent, present) = (
        DBIdTableName::new(7, "absent"),
        DBIdTableName::new(7, "present"),
    );
    mem.seed(&present.to_string_key(), 3, encode_pb(&TableId::new(8)));

    let txn = MetaTxn::new(&mem);

    let fu = txn.get_for_update(absent.clone()).await?;
    let absent = fu.none_or_exists("ctx").unwrap();
    absent.stage_put(&TableId::new(7));

    let fu = txn.get_for_update(present.clone()).await?;
    let err = match fu.none_or_exists("ctx") {
        Ok(_) => panic!("expected existing key"),
        Err(err) => err,
    };
    assert_eq!(err.to_string(), "TableAlreadyExists: present while ctx");
    Ok(())
}

#[tokio::test]
async fn test_move_key_generates_rename_txn() -> anyhow::Result<()> {
    let mem = MemKV::new();
    let old = DBIdTableName::new(7, "old");
    let new = DBIdTableName::new(7, "new");
    let value = TableId::new(8);
    mem.seed(&old.to_string_key(), 3, encode_pb(&value));

    MetaTxnManager::new(&mem, "ctx")
        .move_key(old.clone(), new.clone())
        .await
        .unwrap();

    let req = mem.last_request();
    assert_eq!(req.condition.len(), 2);
    assert!(
        req.condition
            .contains(&TxnCondition::eq_seq(old.to_string_key(), 3))
    );
    assert!(
        req.condition
            .contains(&TxnCondition::eq_seq(new.to_string_key(), 0))
    );
    assert_eq!(req.if_then.len(), 2);
    assert!(
        req.if_then
            .contains(&txn_put_pb_with_ttl(&new, &value, None))
    );
    assert!(req.if_then.contains(&txn_del(&old)));
    Ok(())
}

#[tokio::test]
async fn test_move_key_returns_unknown_when_source_absent() -> anyhow::Result<()> {
    let mem = MemKV::new();
    let old = DBIdTableName::new(7, "old");
    let new = DBIdTableName::new(7, "new");

    let err = MetaTxnManager::new(&mem, "ctx")
        .move_key(old.clone(), new)
        .await
        .unwrap_err();

    assert_eq!(err, MoveKeyError::Unknown(old.unknown_error("ctx")));
    Ok(())
}

#[tokio::test]
async fn test_move_key_returns_exists_when_target_present() -> anyhow::Result<()> {
    let mem = MemKV::new();
    let old = DBIdTableName::new(7, "old");
    let new = DBIdTableName::new(7, "new");
    mem.seed(&old.to_string_key(), 3, encode_pb(&TableId::new(8)));
    mem.seed(&new.to_string_key(), 4, encode_pb(&TableId::new(9)));

    let err = MetaTxnManager::new(&mem, "ctx")
        .move_key(old, new.clone())
        .await
        .unwrap_err();

    assert_eq!(err, MoveKeyError::Exists(new.exist_error("ctx")));
    Ok(())
}

#[tokio::test]
async fn test_move_key_returns_retry_error_on_conflict() -> anyhow::Result<()> {
    let mem = MemKV::new();
    let old = DBIdTableName::new(7, "old");
    let new = DBIdTableName::new(7, "new");
    mem.seed(&old.to_string_key(), 3, encode_pb(&TableId::new(8)));
    mem.script([false]);

    let err = MetaTxnManager::new(&mem, "ctx")
        .retries(Some(1))
        .move_key(old, new)
        .await
        .unwrap_err();

    assert_eq!(
        err,
        MoveKeyError::TxnRetryMaxTimes(TxnRetryMaxTimes::new("ctx", 1))
    );
    Ok(())
}

#[tokio::test]
async fn test_remove_key_fetches_then_deletes() -> anyhow::Result<()> {
    let mem = MemKV::new();
    let key = DBIdTableName::new(7, "tag");
    mem.seed(&key.to_string_key(), 3, encode_pb(&TableId::new(8)));

    MetaTxnManager::new(&mem, "ctx")
        .remove_key(key.clone(), Some(3))
        .await
        .unwrap();

    let req = mem.last_request();
    assert_eq!(mem.read_count(), 1);
    assert_eq!(req.condition, vec![TxnCondition::eq_seq(
        key.to_string_key(),
        3
    )]);
    assert_eq!(req.if_then, vec![txn_del(&key)]);
    Ok(())
}

#[tokio::test]
async fn test_remove_key_returns_unknown_when_absent() -> anyhow::Result<()> {
    let mem = MemKV::new();
    let key = DBIdTableName::new(7, "tag");

    let err = MetaTxnManager::new(&mem, "ctx")
        .remove_key(key.clone(), Some(3))
        .await
        .unwrap_err();

    assert_eq!(mem.read_count(), 1);
    assert_eq!(err, super::RunError::User(key.unknown_error("ctx")));
    Ok(())
}

#[tokio::test]
async fn test_remove_key_returns_unknown_on_seq_mismatch() -> anyhow::Result<()> {
    let mem = MemKV::new();
    let key = DBIdTableName::new(7, "tag");
    mem.seed(&key.to_string_key(), 3, encode_pb(&TableId::new(8)));

    let err = MetaTxnManager::new(&mem, "ctx")
        .remove_key(key.clone(), Some(4))
        .await
        .unwrap_err();

    assert_eq!(mem.read_count(), 1);
    assert_eq!(
        err,
        super::RunError::User(key.unknown_error("ctx; seq mismatched: expect 4, current 3"))
    );
    Ok(())
}

#[tokio::test]
async fn test_remove_key_without_seq_uses_fetched_seq_guard() -> anyhow::Result<()> {
    let mem = MemKV::new();
    let key = DBIdTableName::new(7, "tag");
    mem.seed(&key.to_string_key(), 3, encode_pb(&TableId::new(8)));

    MetaTxnManager::new(&mem, "ctx")
        .remove_key(key.clone(), None)
        .await
        .unwrap();

    let req = mem.last_request();
    assert_eq!(mem.read_count(), 1);
    assert_eq!(req.condition, vec![TxnCondition::eq_seq(
        key.to_string_key(),
        3
    )]);
    assert_eq!(req.if_then, vec![txn_del(&key)]);
    Ok(())
}
