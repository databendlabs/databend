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

use common_meta_types::KVMeta;
use futures_util::StreamExt;

use crate::sm_v002::leveled_store::leveled_map::LeveledMap;
use crate::sm_v002::leveled_store::map_api::MapApi;
use crate::sm_v002::leveled_store::map_api::MapApiRO;
use crate::sm_v002::marked::Marked;

#[tokio::test]
async fn test_freeze() -> anyhow::Result<()> {
    let mut l = LeveledMap::default();

    // Insert an entry at level 0
    let (prev, result) = MapApi::<String>::set(&mut l, s("a1"), Some((b("b0"), None))).await;
    assert_eq!(prev, Marked::new_tomb_stone(0));
    assert_eq!(result, Marked::new_normal(1, b("b0"), None));

    // Insert the same entry at level 1
    l.freeze_writable();

    let (prev, result) = MapApi::<String>::set(&mut l, s("a1"), Some((b("b1"), None))).await;
    assert_eq!(prev, Marked::new_normal(1, b("b0"), None));
    assert_eq!(result, Marked::new_normal(2, b("b1"), None));

    // Listing entries from all levels see the latest
    let got = MapApiRO::<String>::range(&l, s("")..)
        .await
        .collect::<Vec<_>>()
        .await;
    assert_eq!(got, vec![
        //
        (s("a1"), Marked::new_normal(2, b("b1"), None)),
    ]);

    // Listing from the base level sees the old value.
    let base = l.frozen_ref();

    let got = MapApiRO::<String>::range(base, s("")..)
        .await
        .collect::<Vec<_>>()
        .await;
    assert_eq!(got, vec![
        //
        (s("a1"), Marked::new_normal(1, b("b0"), None)),
    ]);

    Ok(())
}

#[tokio::test]
async fn test_single_level() -> anyhow::Result<()> {
    let mut l = LeveledMap::default();

    // Write a1
    let (prev, result) = MapApi::<String>::set(&mut l, s("a1"), Some((b("b1"), None))).await;
    assert_eq!(prev, Marked::new_tomb_stone(0));
    assert_eq!(result, Marked::new_normal(1, b("b1"), None));

    // Write more
    let (_prev, result) = MapApi::<String>::set(&mut l, s("a2"), Some((b("b2"), None))).await;
    assert_eq!(result, Marked::new_normal(2, b("b2"), None));

    let (_prev, result) = MapApi::<String>::set(&mut l, s("a3"), Some((b("b3"), None))).await;
    assert_eq!(result, Marked::new_normal(3, b("b3"), None));

    let (_prev, result) = MapApi::<String>::set(&mut l, s("x1"), Some((b("y1"), None))).await;
    assert_eq!(result, Marked::new_normal(4, b("y1"), None));

    let (_prev, result) = MapApi::<String>::set(&mut l, s("x2"), Some((b("y2"), None))).await;
    assert_eq!(result, Marked::new_normal(5, b("y2"), None));

    // Override a1
    let (prev, result) = MapApi::<String>::set(&mut l, s("a1"), Some((b("b1"), None))).await;
    assert_eq!(prev, Marked::new_normal(1, b("b1"), None));
    assert_eq!(result, Marked::new_normal(6, b("b1"), None));

    // Delete a3
    let (prev, result) = MapApi::<String>::set(&mut l, s("a3"), None).await;
    assert_eq!(prev, Marked::new_normal(3, b("b3"), None));
    assert_eq!(
        result,
        Marked::new_tomb_stone(6),
        "NOTE: single level data also creates a tombstone"
    );

    // Range
    let it = MapApiRO::<String>::range(&l, s("")..).await;
    let got = it.collect::<Vec<_>>().await;
    assert_eq!(got, vec![
        //
        (s("a1"), Marked::new_normal(6, b("b1"), None)),
        (s("a2"), Marked::new_normal(2, b("b2"), None)),
        (s("a3"), Marked::new_tomb_stone(6)),
        (s("x1"), Marked::new_normal(4, b("y1"), None)),
        (s("x2"), Marked::new_normal(5, b("y2"), None)),
    ]);

    // Get
    let got = MapApiRO::<String>::get(&l, "a2").await;
    assert_eq!(got, Marked::new_normal(2, b("b2"), None));

    let got = MapApiRO::<String>::get(&l, "a3").await;
    assert_eq!(got, Marked::new_tomb_stone(6));
    Ok(())
}

#[tokio::test]
async fn test_two_levels() -> anyhow::Result<()> {
    // Create the first level

    let mut l = LeveledMap::default();

    MapApi::<String>::set(&mut l, s("a1"), Some((b("b1"), None))).await;
    MapApi::<String>::set(&mut l, s("a2"), Some((b("b2"), None))).await;
    MapApi::<String>::set(&mut l, s("x1"), Some((b("y1"), None))).await;
    MapApi::<String>::set(&mut l, s("x2"), Some((b("y2"), None))).await;

    let it = MapApiRO::<String>::range(&l, s("")..).await;
    let got = it.collect::<Vec<_>>().await;
    assert_eq!(got, vec![
        //
        (s("a1"), Marked::new_normal(1, b("b1"), None)),
        (s("a2"), Marked::new_normal(2, b("b2"), None)),
        (s("x1"), Marked::new_normal(3, b("y1"), None)),
        (s("x2"), Marked::new_normal(4, b("y2"), None)),
    ]);

    // Create a new level

    l.freeze_writable();

    // Override
    let (prev, result) = MapApi::<String>::set(&mut l, s("a2"), Some((b("b3"), None))).await;
    assert_eq!(prev, Marked::new_normal(2, b("b2"), None));
    assert_eq!(result, Marked::new_normal(5, b("b3"), None));

    // Override again
    let (prev, result) = MapApi::<String>::set(&mut l, s("a2"), Some((b("b4"), None))).await;
    assert_eq!(prev, Marked::new_normal(5, b("b3"), None));
    assert_eq!(result, Marked::new_normal(6, b("b4"), None));

    // Delete by adding a tombstone
    let (prev, result) = MapApi::<String>::set(&mut l, s("a1"), None).await;
    assert_eq!(prev, Marked::new_normal(1, b("b1"), None));
    assert_eq!(result, Marked::new_tomb_stone(6));

    // Override tombstone
    let (prev, result) = MapApi::<String>::set(&mut l, s("a1"), Some((b("b5"), None))).await;
    assert_eq!(prev, Marked::new_tomb_stone(6));
    assert_eq!(result, Marked::new_normal(7, b("b5"), None));

    // Range
    let it = MapApiRO::<String>::range(&l, s("")..).await;
    let got = it.collect::<Vec<_>>().await;
    assert_eq!(got, vec![
        //
        (s("a1"), Marked::new_normal(7, b("b5"), None)),
        (s("a2"), Marked::new_normal(6, b("b4"), None)),
        (s("x1"), Marked::new_normal(3, b("y1"), None)),
        (s("x2"), Marked::new_normal(4, b("y2"), None)),
    ]);

    // Get

    let got = MapApiRO::<String>::get(&l, "a1").await;
    assert_eq!(got, Marked::new_normal(7, b("b5"), None));

    let got = MapApiRO::<String>::get(&l, "a2").await;
    assert_eq!(got, Marked::new_normal(6, b("b4"), None));

    let got = MapApiRO::<String>::get(&l, "w1").await;
    assert_eq!(got, Marked::new_tomb_stone(0));

    // Check base level

    let base = l.frozen_ref();

    let it = MapApiRO::<String>::range(base, s("")..).await;
    let got = it.collect::<Vec<_>>().await;
    assert_eq!(got, vec![
        //
        (s("a1"), Marked::new_normal(1, b("b1"), None)),
        (s("a2"), Marked::new_normal(2, b("b2"), None)),
        (s("x1"), Marked::new_normal(3, b("y1"), None)),
        (s("x2"), Marked::new_normal(4, b("y2"), None)),
    ]);

    Ok(())
}

/// Create multi levels store:
///
/// l2 |         c(D) d
/// l1 |    b(D) c        e
/// l0 | a  b    c    d
async fn build_3_levels() -> LeveledMap {
    let mut l = LeveledMap::default();
    // internal_seq: 0
    MapApi::<String>::set(&mut l, s("a"), Some((b("a0"), None))).await;
    MapApi::<String>::set(&mut l, s("b"), Some((b("b0"), None))).await;
    MapApi::<String>::set(&mut l, s("c"), Some((b("c0"), None))).await;
    MapApi::<String>::set(&mut l, s("d"), Some((b("d0"), None))).await;

    l.freeze_writable();
    // internal_seq: 4
    MapApi::<String>::set(&mut l, s("b"), None).await;
    MapApi::<String>::set(&mut l, s("c"), Some((b("c1"), None))).await;
    MapApi::<String>::set(&mut l, s("e"), Some((b("e1"), None))).await;

    l.freeze_writable();
    // internal_seq: 6
    MapApi::<String>::set(&mut l, s("c"), None).await;
    MapApi::<String>::set(&mut l, s("d"), Some((b("d2"), None))).await;

    l
}

#[tokio::test]
async fn test_three_levels_get_range() -> anyhow::Result<()> {
    let l = build_3_levels().await;

    let got = MapApiRO::<String>::get(&l, "a").await;
    assert_eq!(got, Marked::new_normal(1, b("a0"), None));

    let got = MapApiRO::<String>::get(&l, "b").await;
    assert_eq!(got, Marked::new_tomb_stone(4));

    let got = MapApiRO::<String>::get(&l, "c").await;
    assert_eq!(got, Marked::new_tomb_stone(6));

    let got = MapApiRO::<String>::get(&l, "d").await;
    assert_eq!(got, Marked::new_normal(7, b("d2"), None));

    let got = MapApiRO::<String>::get(&l, "e").await;
    assert_eq!(got, Marked::new_normal(6, b("e1"), None));

    let got = MapApiRO::<String>::get(&l, "f").await;
    assert_eq!(got, Marked::new_tomb_stone(0));

    let got = MapApiRO::<String>::range(&l, s("")..)
        .await
        .collect::<Vec<_>>()
        .await;
    assert_eq!(got, vec![
        //
        (s("a"), Marked::new_normal(1, b("a0"), None)),
        (s("b"), Marked::new_tomb_stone(4)),
        (s("c"), Marked::new_tomb_stone(6)),
        (s("d"), Marked::new_normal(7, b("d2"), None)),
        (s("e"), Marked::new_normal(6, b("e1"), None)),
    ]);

    Ok(())
}

#[tokio::test]
async fn test_three_levels_override() -> anyhow::Result<()> {
    let mut l = build_3_levels().await;

    let (prev, result) = MapApi::<String>::set(&mut l, s("a"), Some((b("x"), None))).await;
    assert_eq!(prev, Marked::new_normal(1, b("a0"), None));
    assert_eq!(result, Marked::new_normal(8, b("x"), None));

    let (prev, result) = MapApi::<String>::set(&mut l, s("b"), Some((b("y"), None))).await;
    assert_eq!(prev, Marked::new_tomb_stone(4));
    assert_eq!(result, Marked::new_normal(9, b("y"), None));

    let (prev, result) = MapApi::<String>::set(&mut l, s("c"), Some((b("z"), None))).await;
    assert_eq!(prev, Marked::new_tomb_stone(6));
    assert_eq!(result, Marked::new_normal(10, b("z"), None));

    let (prev, result) = MapApi::<String>::set(&mut l, s("d"), Some((b("u"), None))).await;
    assert_eq!(prev, Marked::new_normal(7, b("d2"), None));
    assert_eq!(result, Marked::new_normal(11, b("u"), None));

    let (prev, result) = MapApi::<String>::set(&mut l, s("e"), Some((b("v"), None))).await;
    assert_eq!(prev, Marked::new_normal(6, b("e1"), None));
    assert_eq!(result, Marked::new_normal(12, b("v"), None));

    let (prev, result) = MapApi::<String>::set(&mut l, s("f"), Some((b("w"), None))).await;
    assert_eq!(prev, Marked::new_tomb_stone(0));
    assert_eq!(result, Marked::new_normal(13, b("w"), None));

    let got = MapApiRO::<String>::range(&l, s("")..)
        .await
        .collect::<Vec<_>>()
        .await;
    assert_eq!(got, vec![
        //
        (s("a"), Marked::new_normal(8, b("x"), None)),
        (s("b"), Marked::new_normal(9, b("y"), None)),
        (s("c"), Marked::new_normal(10, b("z"), None)),
        (s("d"), Marked::new_normal(11, b("u"), None)),
        (s("e"), Marked::new_normal(12, b("v"), None)),
        (s("f"), Marked::new_normal(13, b("w"), None)),
    ]);

    Ok(())
}

#[tokio::test]
async fn test_three_levels_delete() -> anyhow::Result<()> {
    let mut l = build_3_levels().await;

    let (prev, result) = MapApi::<String>::set(&mut l, s("a"), None).await;
    assert_eq!(prev, Marked::new_normal(1, b("a0"), None));
    assert_eq!(result, Marked::new_tomb_stone(7));

    let (prev, result) = MapApi::<String>::set(&mut l, s("b"), None).await;
    assert_eq!(prev, Marked::new_tomb_stone(4));
    assert_eq!(result, Marked::new_tomb_stone(7));

    let (prev, result) = MapApi::<String>::set(&mut l, s("c"), None).await;
    assert_eq!(prev, Marked::new_tomb_stone(6));
    assert_eq!(result, Marked::new_tomb_stone(7));

    let (prev, result) = MapApi::<String>::set(&mut l, s("d"), None).await;
    assert_eq!(prev, Marked::new_normal(7, b("d2"), None));
    assert_eq!(result, Marked::new_tomb_stone(7));

    let (prev, result) = MapApi::<String>::set(&mut l, s("e"), None).await;
    assert_eq!(prev, Marked::new_normal(6, b("e1"), None));
    assert_eq!(result, Marked::new_tomb_stone(7));

    let (prev, result) = MapApi::<String>::set(&mut l, s("f"), None).await;
    assert_eq!(prev, Marked::new_tomb_stone(0));
    assert_eq!(result, Marked::new_tomb_stone(0));

    let got = MapApiRO::<String>::range(&l, s("")..)
        .await
        .collect::<Vec<_>>()
        .await;
    assert_eq!(got, vec![
        //
        (s("a"), Marked::new_tomb_stone(7)),
        (s("b"), Marked::new_tomb_stone(7)),
        (s("c"), Marked::new_tomb_stone(7)),
        (s("d"), Marked::new_tomb_stone(7)),
        (s("e"), Marked::new_tomb_stone(7)),
    ]);

    Ok(())
}

/// |      b(m) c
/// | a(m) b    c(m)
async fn build_2_level_with_meta() -> LeveledMap {
    let mut l = LeveledMap::default();

    // internal_seq: 0
    MapApi::<String>::set(
        &mut l,
        s("a"),
        Some((b("a0"), Some(KVMeta { expire_at: Some(1) }))),
    )
    .await;
    MapApi::<String>::set(&mut l, s("b"), Some((b("b0"), None))).await;
    MapApi::<String>::set(
        &mut l,
        s("c"),
        Some((b("c0"), Some(KVMeta { expire_at: Some(2) }))),
    )
    .await;

    l.freeze_writable();

    // internal_seq: 3
    MapApi::<String>::set(
        &mut l,
        s("b"),
        Some((
            b("b1"),
            Some(KVMeta {
                expire_at: Some(10),
            }),
        )),
    )
    .await;
    MapApi::<String>::set(&mut l, s("c"), Some((b("c1"), None))).await;

    l
}

#[tokio::test]
async fn test_two_level_update_value() -> anyhow::Result<()> {
    // Update value for a.
    {
        let mut l = build_2_level_with_meta().await;

        let (prev, result) = MapApi::<String>::upsert_value(&mut l, s("a"), b("a1")).await;
        assert_eq!(
            prev,
            Marked::new_normal(1, b("a0"), Some(KVMeta { expire_at: Some(1) }))
        );
        assert_eq!(
            result,
            Marked::new_normal(6, b("a1"), Some(KVMeta { expire_at: Some(1) }))
        );

        let got = MapApiRO::<String>::get(&l, "a").await;
        assert_eq!(
            got,
            Marked::new_normal(6, b("a1"), Some(KVMeta { expire_at: Some(1) }))
        );
    }

    // Update value for b.
    {
        let mut l = build_2_level_with_meta().await;

        let (prev, result) = MapApi::<String>::upsert_value(&mut l, s("b"), b("x1")).await;
        assert_eq!(
            prev,
            Marked::new_normal(
                4,
                b("b1"),
                Some(KVMeta {
                    expire_at: Some(10)
                })
            )
        );
        assert_eq!(
            result,
            Marked::new_normal(
                6,
                b("x1"),
                Some(KVMeta {
                    expire_at: Some(10)
                })
            )
        );

        let got = MapApiRO::<String>::get(&l, "b").await;
        assert_eq!(
            got,
            Marked::new_normal(
                6,
                b("x1"),
                Some(KVMeta {
                    expire_at: Some(10)
                })
            )
        );
    }

    // Update nonexistent.
    {
        let mut l = build_2_level_with_meta().await;

        let (prev, result) = MapApi::<String>::upsert_value(&mut l, s("d"), b("d1")).await;
        assert_eq!(prev, Marked::new_tomb_stone(0));
        assert_eq!(result, Marked::new_normal(6, b("d1"), None));

        let got = MapApiRO::<String>::get(&l, "d").await;
        assert_eq!(got, Marked::new_normal(6, b("d1"), None));
    }

    Ok(())
}

#[tokio::test]
async fn test_two_level_update_meta() -> anyhow::Result<()> {
    // Update meta for a.
    {
        let mut l = build_2_level_with_meta().await;

        let (prev, result) =
            MapApi::<String>::update_meta(&mut l, s("a"), Some(KVMeta { expire_at: Some(2) }))
                .await;
        assert_eq!(
            prev,
            Marked::new_normal(1, b("a0"), Some(KVMeta { expire_at: Some(1) }))
        );
        assert_eq!(
            result,
            Marked::new_normal(6, b("a0"), Some(KVMeta { expire_at: Some(2) }))
        );

        let got = MapApiRO::<String>::get(&l, "a").await;
        assert_eq!(
            got,
            Marked::new_normal(6, b("a0"), Some(KVMeta { expire_at: Some(2) }))
        );
    }

    // Delete meta for a.
    {
        let mut l = build_2_level_with_meta().await;

        let (prev, result) = MapApi::<String>::update_meta(&mut l, s("b"), None).await;
        assert_eq!(
            prev,
            Marked::new_normal(
                4,
                b("b1"),
                Some(KVMeta {
                    expire_at: Some(10)
                })
            )
        );
        assert_eq!(result, Marked::new_normal(6, b("b1"), None));

        let got = MapApiRO::<String>::get(&l, "b").await;
        assert_eq!(got, Marked::new_normal(6, b("b1"), None));
    }

    // Update meta for c.
    {
        let mut l = build_2_level_with_meta().await;

        let (prev, result) = MapApi::<String>::update_meta(
            &mut l,
            s("c"),
            Some(KVMeta {
                expire_at: Some(20),
            }),
        )
        .await;
        assert_eq!(prev, Marked::new_normal(5, b("c1"), None));
        assert_eq!(
            result,
            Marked::new_normal(
                6,
                b("c1"),
                Some(KVMeta {
                    expire_at: Some(20)
                })
            )
        );

        let got = MapApiRO::<String>::get(&l, "c").await;
        assert_eq!(
            got,
            Marked::new_normal(
                6,
                b("c1"),
                Some(KVMeta {
                    expire_at: Some(20)
                })
            )
        );
    }

    // Update nonexistent.
    {
        let mut l = build_2_level_with_meta().await;

        let (prev, result) =
            MapApi::<String>::update_meta(&mut l, s("d"), Some(KVMeta { expire_at: Some(2) }))
                .await;
        assert_eq!(prev, Marked::new_tomb_stone(0));
        assert_eq!(result, Marked::new_tomb_stone(0));

        let got = MapApiRO::<String>::get(&l, "d").await;
        assert_eq!(got, Marked::new_tomb_stone(0));
    }

    Ok(())
}

fn s(x: impl ToString) -> String {
    x.to_string()
}

fn b(x: impl ToString) -> Vec<u8> {
    x.to_string().as_bytes().to_vec()
}
