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

use futures_util::TryStreamExt;
use map_api::SeqMarked;
use map_api::mvcc::ScopedGet;
use map_api::mvcc::ScopedRange;
use map_api::mvcc::ScopedSeqBoundedGet;
use map_api::mvcc::ScopedSeqBoundedRange;
use map_api::mvcc::ScopedSet;
use state_machine_api::KVMeta;
use state_machine_api::UserKey;

use crate::leveled_store::leveled_map::LeveledMap;
use crate::leveled_store::map_api::MapApiHelper;

#[tokio::test]
async fn test_freeze() -> anyhow::Result<()> {
    let l = LeveledMap::default();
    let mut view = l.to_view();

    // Insert an entry at level 0
    let (prev, result) = view
        .fetch_and_set(user_key("a1"), Some((None, b("b0"))))
        .await?;
    assert_eq!(prev, SeqMarked::new_tombstone(0));
    assert_eq!(result, SeqMarked::new_normal(1, (None, b("b0"))));
    view.commit().await?;

    // Insert the same entry at level 1
    l.freeze_writable_without_permit();
    // println!("{:#?}", l);
    let mut view = l.to_view();

    let (prev, result) = view
        .fetch_and_set(user_key("a1"), Some((None, b("b1"))))
        .await?;
    assert_eq!(prev, SeqMarked::new_normal(1, (None, b("b0"))));
    assert_eq!(result, SeqMarked::new_normal(2, (None, b("b1"))));
    view.commit().await?;

    // Listing entries from all levels see the latest
    let view = l.to_view();
    let got = view
        .range(user_key("")..)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(got, vec![
        //
        (user_key("a1"), SeqMarked::new_normal(2, (None, b("b1")))),
    ]);

    // Listing from the base level sees the old value.
    let immutables = l.immutable_levels();
    // println!("{:#?}", immutables);

    let last_seq = immutables.newest().unwrap().sys_data().curr_seq();

    let tmp = LeveledMap::default();
    tmp.with_sys_data(|s| s.update_seq(last_seq));
    tmp.replace_immutable_levels(immutables);
    let view = tmp.to_view();

    let got = view
        .range(user_key("")..)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(got, vec![
        //
        (user_key("a1"), SeqMarked::new_normal(1, (None, b("b0")))),
    ]);

    Ok(())
}

#[tokio::test]
async fn test_single_level() -> anyhow::Result<()> {
    let l = LeveledMap::default();
    let mut view = l.to_view();

    // Write a1
    let (prev, result) = view
        .fetch_and_set(user_key("a1"), Some((None, b("b1"))))
        .await?;
    assert_eq!(prev, SeqMarked::new_tombstone(0));
    assert_eq!(result, SeqMarked::new_normal(1, (None, b("b1"))));

    // Write more
    let (_prev, result) = view
        .fetch_and_set(user_key("a2"), Some((None, b("b2"))))
        .await?;
    assert_eq!(result, SeqMarked::new_normal(2, (None, b("b2"))));

    let (_prev, result) = view
        .fetch_and_set(user_key("a3"), Some((None, b("b3"))))
        .await?;
    assert_eq!(result, SeqMarked::new_normal(3, (None, b("b3"))));

    let (_prev, result) = view
        .fetch_and_set(user_key("x1"), Some((None, b("y1"))))
        .await?;
    assert_eq!(result, SeqMarked::new_normal(4, (None, b("y1"))));

    let (_prev, result) = view
        .fetch_and_set(user_key("x2"), Some((None, b("y2"))))
        .await?;
    assert_eq!(result, SeqMarked::new_normal(5, (None, b("y2"))));

    // Override a1
    let (prev, result) = view
        .fetch_and_set(user_key("a1"), Some((None, b("b1"))))
        .await?;
    assert_eq!(prev, SeqMarked::new_normal(1, (None, b("b1"))));
    assert_eq!(result, SeqMarked::new_normal(6, (None, b("b1"))));

    // Delete a3
    let (prev, result) = view.fetch_and_set(user_key("a3"), None).await?;
    assert_eq!(prev, SeqMarked::new_normal(3, (None, b("b3"))));
    assert_eq!(
        result,
        SeqMarked::new_tombstone(6),
        "NOTE: single level data also creates a tombstone"
    );

    // Range
    let strm = view.range(user_key("")..).await?;
    let got = strm.try_collect::<Vec<_>>().await?;
    assert_eq!(got, vec![
        //
        (user_key("a1"), SeqMarked::new_normal(6, (None, b("b1")))),
        (user_key("a2"), SeqMarked::new_normal(2, (None, b("b2")))),
        (user_key("a3"), SeqMarked::new_tombstone(6)),
        (user_key("x1"), SeqMarked::new_normal(4, (None, b("y1")))),
        (user_key("x2"), SeqMarked::new_normal(5, (None, b("y2")))),
    ]);

    // Get
    let got = view.get(user_key("a2")).await?;
    assert_eq!(got, SeqMarked::new_normal(2, (None, b("b2"))));

    let got = view.get(user_key("a3")).await?;
    assert_eq!(got, SeqMarked::new_tombstone(6));
    Ok(())
}

#[tokio::test]
async fn test_two_levels() -> anyhow::Result<()> {
    // Create the first level

    let l = LeveledMap::default();
    let mut view = l.to_view();

    view.set(user_key("a1"), Some((None, b("b1"))));
    view.set(user_key("a2"), Some((None, b("b2"))));
    view.set(user_key("x1"), Some((None, b("y1"))));
    view.set(user_key("x2"), Some((None, b("y2"))));
    view.commit().await?;

    let view = l.to_view();
    let it = view.range(user_key("")..).await?;
    let got = it.try_collect::<Vec<_>>().await?;
    assert_eq!(got, vec![
        //
        (user_key("a1"), SeqMarked::new_normal(1, (None, b("b1")))),
        (user_key("a2"), SeqMarked::new_normal(2, (None, b("b2")))),
        (user_key("x1"), SeqMarked::new_normal(3, (None, b("y1")))),
        (user_key("x2"), SeqMarked::new_normal(4, (None, b("y2")))),
    ]);

    // Create a new level

    l.freeze_writable_without_permit();
    let mut view = l.to_view();

    // Override
    let (prev, result) = view
        .fetch_and_set(user_key("a2"), Some((None, b("b3"))))
        .await?;
    assert_eq!(prev, SeqMarked::new_normal(2, (None, b("b2"))));
    assert_eq!(result, SeqMarked::new_normal(5, (None, b("b3"))));

    // Override again
    let (prev, result) = view
        .fetch_and_set(user_key("a2"), Some((None, b("b4"))))
        .await?;
    assert_eq!(prev, SeqMarked::new_normal(5, (None, b("b3"))));
    assert_eq!(result, SeqMarked::new_normal(6, (None, b("b4"))));

    // Delete by adding a tombstone
    let (prev, result) = view.fetch_and_set(user_key("a1"), None).await?;
    assert_eq!(prev, SeqMarked::new_normal(1, (None, b("b1"))));
    assert_eq!(result, SeqMarked::new_tombstone(6));

    // Override tombstone
    let (prev, result) = view
        .fetch_and_set(user_key("a1"), Some((None, b("b5"))))
        .await?;
    assert_eq!(prev, SeqMarked::new_tombstone(6));
    assert_eq!(result, SeqMarked::new_normal(7, (None, b("b5"))));
    view.commit().await?;

    // Range
    let view = l.to_view();
    let it = view.range(user_key("")..).await?;
    let got = it.try_collect::<Vec<_>>().await?;
    assert_eq!(got, vec![
        //
        (user_key("a1"), SeqMarked::new_normal(7, (None, b("b5")))),
        (user_key("a2"), SeqMarked::new_normal(6, (None, b("b4")))),
        (user_key("x1"), SeqMarked::new_normal(3, (None, b("y1")))),
        (user_key("x2"), SeqMarked::new_normal(4, (None, b("y2")))),
    ]);

    // Get

    let got = view.get(user_key("a1")).await?;
    assert_eq!(got, SeqMarked::new_normal(7, (None, b("b5"))));

    let got = view.get(user_key("a2")).await?;
    assert_eq!(got, SeqMarked::new_normal(6, (None, b("b4"))));

    let got = view.get(user_key("w1")).await?;
    assert_eq!(got, SeqMarked::new_tombstone(0));

    // Check base level

    let immutables = l.immutable_levels();

    let tmp = LeveledMap::default();
    tmp.replace_immutable_levels(immutables);

    let strm = tmp.range(user_key("").., u64::MAX).await?;
    let got = strm.try_collect::<Vec<_>>().await?;
    assert_eq!(got, vec![
        //
        (user_key("a1"), SeqMarked::new_normal(1, (None, b("b1")))),
        (user_key("a2"), SeqMarked::new_normal(2, (None, b("b2")))),
        (user_key("x1"), SeqMarked::new_normal(3, (None, b("y1")))),
        (user_key("x2"), SeqMarked::new_normal(4, (None, b("y2")))),
    ]);

    Ok(())
}

/// Create multi levels store:
///
/// ```text
/// l2 |         c(D) d
/// l1 |    b(D) c        e
/// l0 | a  b    c    d
/// ```
async fn build_3_levels() -> anyhow::Result<LeveledMap> {
    let l = LeveledMap::default();
    let mut view = l.to_view();

    // internal_seq: 0
    view.set(user_key("a"), Some((None, b("a0"))));
    view.set(user_key("b"), Some((None, b("b0"))));
    view.set(user_key("c"), Some((None, b("c0"))));
    view.set(user_key("d"), Some((None, b("d0"))));
    view.commit().await?;

    l.freeze_writable_without_permit();
    let mut view = l.to_view();

    // internal_seq: 4
    view.set(user_key("b"), None);
    view.set(user_key("c"), Some((None, b("c1"))));
    view.set(user_key("e"), Some((None, b("e1"))));
    view.commit().await?;

    l.freeze_writable_without_permit();
    let mut view = l.to_view();

    // internal_seq: 6
    view.set(user_key("c"), None);
    view.set(user_key("d"), Some((None, b("d2"))));
    view.commit().await?;

    Ok(l)
}

#[tokio::test]
async fn test_three_levels_get_range() -> anyhow::Result<()> {
    let l = build_3_levels().await?;

    let got = l.get(user_key("a"), u64::MAX).await?;
    assert_eq!(got, SeqMarked::new_normal(1, (None, b("a0"))));

    let got = l.get(user_key("b"), u64::MAX).await?;
    assert_eq!(got, SeqMarked::new_tombstone(4));

    let got = l.get(user_key("c"), u64::MAX).await?;
    assert_eq!(got, SeqMarked::new_tombstone(6));

    let got = l.get(user_key("d"), u64::MAX).await?;
    assert_eq!(got, SeqMarked::new_normal(7, (None, b("d2"))));

    let got = l.get(user_key("e"), u64::MAX).await?;
    assert_eq!(got, SeqMarked::new_normal(6, (None, b("e1"))));

    let got = l.get(user_key("f"), u64::MAX).await?;
    assert_eq!(got, SeqMarked::new_tombstone(0));

    let got = l
        .range(user_key("").., u64::MAX)
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

    Ok(())
}

#[tokio::test]
async fn test_three_levels_override() -> anyhow::Result<()> {
    let l = build_3_levels().await?;
    let mut view = l.to_view();

    let (prev, result) = view
        .fetch_and_set(user_key("a"), Some((None, b("x"))))
        .await?;
    assert_eq!(prev, SeqMarked::new_normal(1, (None, b("a0"))));
    assert_eq!(result, SeqMarked::new_normal(8, (None, b("x"))));

    let (prev, result) = view
        .fetch_and_set(user_key("b"), Some((None, b("y"))))
        .await?;
    assert_eq!(prev, SeqMarked::new_tombstone(4));
    assert_eq!(result, SeqMarked::new_normal(9, (None, b("y"))));

    let (prev, result) = view
        .fetch_and_set(user_key("c"), Some((None, b("z"))))
        .await?;
    assert_eq!(prev, SeqMarked::new_tombstone(6));
    assert_eq!(result, SeqMarked::new_normal(10, (None, b("z"))));

    let (prev, result) = view
        .fetch_and_set(user_key("d"), Some((None, b("u"))))
        .await?;
    assert_eq!(prev, SeqMarked::new_normal(7, (None, b("d2"))));
    assert_eq!(result, SeqMarked::new_normal(11, (None, b("u"))));

    let (prev, result) = view
        .fetch_and_set(user_key("e"), Some((None, b("v"))))
        .await?;
    assert_eq!(prev, SeqMarked::new_normal(6, (None, b("e1"))));
    assert_eq!(result, SeqMarked::new_normal(12, (None, b("v"))));

    let (prev, result) = view
        .fetch_and_set(user_key("f"), Some((None, b("w"))))
        .await?;
    assert_eq!(prev, SeqMarked::new_tombstone(0));
    assert_eq!(result, SeqMarked::new_normal(13, (None, b("w"))));
    view.commit().await?;

    let view = l.to_view();

    let got = view
        .range(user_key("")..)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(got, vec![
        //
        (user_key("a"), SeqMarked::new_normal(8, (None, b("x")))),
        (user_key("b"), SeqMarked::new_normal(9, (None, b("y")))),
        (user_key("c"), SeqMarked::new_normal(10, (None, b("z")))),
        (user_key("d"), SeqMarked::new_normal(11, (None, b("u")))),
        (user_key("e"), SeqMarked::new_normal(12, (None, b("v")))),
        (user_key("f"), SeqMarked::new_normal(13, (None, b("w")))),
    ]);

    Ok(())
}

#[tokio::test]
async fn test_three_levels_delete() -> anyhow::Result<()> {
    let l = build_3_levels().await?;

    let mut view = l.to_view();

    let (prev, result) = view.fetch_and_set(user_key("a"), None).await?;
    assert_eq!(prev, SeqMarked::new_normal(1, (None, b("a0"))));
    assert_eq!(result, SeqMarked::new_tombstone(7));

    let (prev, result) = view.fetch_and_set(user_key("b"), None).await?;
    assert_eq!(prev, SeqMarked::new_tombstone(4));
    assert_eq!(result, SeqMarked::new_tombstone(7));

    let (prev, result) = view.fetch_and_set(user_key("c"), None).await?;
    assert_eq!(prev, SeqMarked::new_tombstone(6));
    assert_eq!(result, SeqMarked::new_tombstone(7));

    let (prev, result) = view.fetch_and_set(user_key("d"), None).await?;
    assert_eq!(prev, SeqMarked::new_normal(7, (None, b("d2"))));
    assert_eq!(result, SeqMarked::new_tombstone(7));

    let (prev, result) = view.fetch_and_set(user_key("e"), None).await?;
    assert_eq!(prev, SeqMarked::new_normal(6, (None, b("e1"))));
    assert_eq!(result, SeqMarked::new_tombstone(7));

    let (prev, result) = view.fetch_and_set(user_key("f"), None).await?;
    assert_eq!(prev, SeqMarked::new_tombstone(0));
    assert_eq!(result, SeqMarked::new_tombstone(0));
    view.commit().await?;

    let view = l.to_view();

    let got = view
        .range(user_key("")..)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(got, vec![
        //
        (user_key("a"), SeqMarked::new_tombstone(7)),
        (user_key("b"), SeqMarked::new_tombstone(7)),
        (user_key("c"), SeqMarked::new_tombstone(7)),
        (user_key("d"), SeqMarked::new_tombstone(7)),
        (user_key("e"), SeqMarked::new_tombstone(7)),
    ]);

    Ok(())
}

/// ```text
/// |      b(m) c
/// | a(m) b    c(m)
/// ```
async fn build_2_level_with_meta() -> anyhow::Result<LeveledMap> {
    let l = LeveledMap::default();
    let mut view = l.to_view();

    // internal_seq: 0
    view.set(
        user_key("a"),
        Some((Some(KVMeta::new_expires_at(1)), b("a0"))),
    );
    view.set(user_key("b"), Some((None, b("b0"))));
    view.set(
        user_key("c"),
        Some((Some(KVMeta::new_expires_at(2)), b("c0"))),
    );
    view.commit().await?;

    l.freeze_writable_without_permit();
    let mut view = l.to_view();

    // internal_seq: 3
    view.set(
        user_key("b"),
        Some((Some(KVMeta::new_expires_at(10)), b("b1"))),
    );
    view.set(user_key("c"), Some((None, b("c1"))));
    view.commit().await?;

    Ok(l)
}

/// Build a LeveledMap with two consecutive deletes, which produce two tombstones with same internal_seq.
/// a range that combine two levels should not panic.
#[tokio::test]
async fn test_2_level_same_tombstone() -> anyhow::Result<()> {
    let lm = build_2_level_consecutive_delete().await?;
    let strm = lm.range(UserKey::default().., u64::MAX).await?;

    let got = strm.try_collect::<Vec<_>>().await?;

    let got = got
        .into_iter()
        .map(|(k, v)| format!("{}={:?}", k, v))
        .collect::<Vec<_>>();

    assert_eq!(
        vec![
            "a=SeqMarked { seq: 1, marked: Normal((None, [97, 48])) }",
            "b=SeqMarked { seq: 3, marked: TombStone }",
            "c=SeqMarked { seq: 4, marked: Normal((None, [99, 49])) }"
        ],
        got
    );

    Ok(())
}

/// Build a LeveledMap with two consecutive deletes, which produce two tombstones with same internal_seq.
/// ```text
/// |      b(D) c
/// | a    b(D) c
/// ```
async fn build_2_level_consecutive_delete() -> anyhow::Result<LeveledMap> {
    let l = LeveledMap::default();
    let mut view = l.to_view();

    // internal_seq: 0
    view.set(user_key("a"), Some((None, b("a0"))));
    view.set(user_key("b"), Some((None, b("b0"))));
    view.set(user_key("c"), Some((None, b("c0"))));
    view.set(user_key("b"), None);
    view.commit().await?;

    l.freeze_writable_without_permit();
    let mut view = l.to_view();

    // internal_seq: 3
    view.set(user_key("b"), None);
    view.set(user_key("c"), Some((None, b("c1"))));
    view.commit().await?;

    Ok(l)
}

#[tokio::test]
async fn test_two_level_update_meta() -> anyhow::Result<()> {
    // Update meta for a.
    {
        let mut l = build_2_level_with_meta().await?;

        let (prev, result) = MapApiHelper::update_meta_on_leveled_map(
            &mut l,
            user_key("a"),
            Some(KVMeta::new_expires_at(2)),
        )
        .await?;

        assert_eq!(
            prev,
            SeqMarked::new_normal(1, (Some(KVMeta::new_expires_at(1)), b("a0")))
        );
        assert_eq!(
            result,
            SeqMarked::new_normal(6, (Some(KVMeta::new_expires_at(2)), b("a0")))
        );

        let got = l.get(user_key("a"), u64::MAX).await?;
        assert_eq!(
            got,
            SeqMarked::new_normal(6, (Some(KVMeta::new_expires_at(2)), b("a0")))
        );
    }

    // Delete meta for a.
    {
        let mut l = build_2_level_with_meta().await?;

        let (prev, result) =
            MapApiHelper::update_meta_on_leveled_map(&mut l, user_key("b"), None).await?;
        assert_eq!(
            prev,
            SeqMarked::new_normal(4, (Some(KVMeta::new_expires_at(10)), b("b1"),))
        );
        assert_eq!(result, SeqMarked::new_normal(6, (None, b("b1"))));

        let got = l.get(user_key("b"), u64::MAX).await?;
        assert_eq!(got, SeqMarked::new_normal(6, (None, b("b1"))));
    }

    // Update meta for c.
    {
        let mut l = build_2_level_with_meta().await?;

        let (prev, result) = MapApiHelper::update_meta_on_leveled_map(
            &mut l,
            user_key("c"),
            Some(KVMeta::new_expires_at(20)),
        )
        .await?;
        assert_eq!(prev, SeqMarked::new_normal(5, (None, b("c1"))));
        assert_eq!(
            result,
            SeqMarked::new_normal(6, (Some(KVMeta::new_expires_at(20)), b("c1")))
        );

        let got = l.get(user_key("c"), u64::MAX).await?;
        assert_eq!(
            got,
            SeqMarked::new_normal(6, (Some(KVMeta::new_expires_at(20)), b("c1")))
        );
    }

    // Update nonexistent.
    {
        let mut l = build_2_level_with_meta().await?;

        let (prev, result) = MapApiHelper::update_meta_on_leveled_map(
            &mut l,
            user_key("d"),
            Some(KVMeta::new_expires_at(2)),
        )
        .await?;
        assert_eq!(prev, SeqMarked::new_tombstone(0));
        assert_eq!(result, SeqMarked::new_tombstone(0));

        let got = l.get(user_key("d"), u64::MAX).await?;
        assert_eq!(got, SeqMarked::new_tombstone(0));
    }

    Ok(())
}

fn user_key(s: impl ToString) -> UserKey {
    UserKey::new(s)
}

fn b(x: impl ToString) -> Vec<u8> {
    x.to_string().as_bytes().to_vec()
}
