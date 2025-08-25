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

//! Test for db_map_api_ro_impl.

use databend_common_meta_types::UpsertKV;
use futures_util::TryStreamExt;
use map_api::map_api_ro::MapApiRO;
use seq_marked::SeqMarked;
use state_machine_api::ExpireKey;
use state_machine_api::KVMeta;
use state_machine_api::UserKey;

use crate::leveled_store::db_builder::DBBuilder;
use crate::leveled_store::db_map_api_ro_impl::MapView;
use crate::leveled_store::map_api::AsMap;
use crate::sm_v003::SMV003;

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_db_map_api_ro() -> anyhow::Result<()> {
    // Build a state machine
    let mut sm = {
        let mut sm = SMV003::default();

        let mut a = sm.new_applier().await;
        a.upsert_kv(&UpsertKV::update("a", b"a0").with_expire_sec(10))
            .await?;
        a.upsert_kv(&UpsertKV::update("b", b"b0").with_expire_sec(5))
            .await?;

        a.commit().await?;

        sm.levels_mut().freeze_writable();

        let mut a = sm.new_applier().await;

        a.upsert_kv(&UpsertKV::update("c", b"c0").with_expire_sec(20))
            .await?;
        a.upsert_kv(&UpsertKV::update("a", b"a1").with_expire_sec(15))
            .await?;
        a.upsert_kv(&UpsertKV::delete("b")).await?;

        a.commit().await?;

        sm
    };

    // Build a db from all data of the state machine
    let db = {
        let lm = sm.levels_mut();

        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path();

        let db_builder = DBBuilder::new(path, "temp-db", rotbl::v001::Config::default())?;
        db_builder
            .build_from_leveled_map(lm, |_| "1-1-1-1".to_string())
            .await?
    };

    // Test kv map

    let binding = MapView(&db);
    let smap = binding.as_user_map();
    assert_eq!(
        SeqMarked::new_normal(4, (Some(KVMeta::new(Some(15))), b("a1"))),
        smap.get(&user_key("a")).await?
    );
    assert_eq!(
        SeqMarked::new_not_found(),
        smap.get(&user_key("b")).await?,
        "no tombstone is stored"
    );
    assert_eq!(
        SeqMarked::new_normal(3, (Some(KVMeta::new(Some(20))), b("c0"))),
        smap.get(&user_key("c")).await?
    );
    assert_eq!(SeqMarked::new_not_found(), smap.get(&user_key("d")).await?);

    let got = smap.range(..).await?.try_collect::<Vec<_>>().await?;
    assert_eq!(
        vec![
            (
                user_key("a"),
                SeqMarked::new_normal(4, (Some(KVMeta::new(Some(15))), b("a1"))),
            ),
            (
                user_key("c"),
                SeqMarked::new_normal(3, (Some(KVMeta::new(Some(20))), b("c0"))),
            )
        ],
        got
    );

    // Test expire index

    let binding = MapView(&db);
    let emap = binding.as_expire_map();

    assert_eq!(
        SeqMarked::new_normal(4, s("a")),
        emap.get(&ExpireKey::new(15_000, 4)).await?
    );
    assert_eq!(
        SeqMarked::new_normal(3, s("c")),
        emap.get(&ExpireKey::new(20_000, 3)).await?
    );
    assert_eq!(
        SeqMarked::new_not_found(),
        emap.get(&ExpireKey::new(5_000, 2)).await?
    );

    let got = emap.range(..).await?.try_collect::<Vec<_>>().await?;
    assert_eq!(
        vec![
            (ExpireKey::new(15_000, 4), SeqMarked::new_normal(4, s("a"))),
            (ExpireKey::new(20_000, 3), SeqMarked::new_normal(3, s("c"))),
        ],
        got
    );

    Ok(())
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
