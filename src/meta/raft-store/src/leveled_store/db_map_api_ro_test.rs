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

use databend_common_meta_types::KVMeta;
use databend_common_meta_types::UpsertKV;
use futures_util::TryStreamExt;

use crate::leveled_store::db_builder::DBBuilder;
use crate::leveled_store::map_api::AsMap;
use crate::leveled_store::map_api::MapApiRO;
use crate::marked::Marked;
use crate::sm_v003::SMV003;
use crate::state_machine::ExpireKey;

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_db_map_api_ro() -> anyhow::Result<()> {
    // Build a state machine
    let mut sm = {
        let mut sm = SMV003::default();

        let mut a = sm.new_applier();
        a.upsert_kv(&UpsertKV::update("a", b"a0").with_expire_sec(10))
            .await?;
        a.upsert_kv(&UpsertKV::update("b", b"b0").with_expire_sec(5))
            .await?;

        sm.levels_mut().freeze_writable();

        let mut a = sm.new_applier();

        a.upsert_kv(&UpsertKV::update("c", b"c0").with_expire_sec(20))
            .await?;
        a.upsert_kv(&UpsertKV::update("a", b"a1").with_expire_sec(15))
            .await?;
        a.upsert_kv(&UpsertKV::delete("b")).await?;

        sm
    };

    // Build a db from all data of the state machine
    let db = {
        let lm = sm.levels_mut();

        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path();
        let path = path.join("temp-db");

        let db_builder = DBBuilder::new_with_default_config(path)?;
        db_builder
            .build_from_leveled_map(lm, |_| "1-1-1-1".to_string())
            .await?
    };

    // Test kv map

    let smap = db.str_map();
    assert_eq!(
        Marked::new_with_meta(4, b("a1"), Some(KVMeta::new(Some(15)))),
        smap.get("a").await?
    );
    assert_eq!(
        Marked::empty(),
        smap.get("b").await?,
        "no tombstone is stored"
    );
    assert_eq!(
        Marked::new_with_meta(3, b("c0"), Some(KVMeta::new(Some(20)))),
        smap.get("c").await?
    );
    assert_eq!(Marked::empty(), smap.get("d").await?);

    let got = smap.range(..).await?.try_collect::<Vec<_>>().await?;
    assert_eq!(
        vec![
            (
                s("a"),
                Marked::new_with_meta(4, b("a1"), Some(KVMeta::new(Some(15)))),
            ),
            (
                s("c"),
                Marked::new_with_meta(3, b("c0"), Some(KVMeta::new(Some(20)))),
            )
        ],
        got
    );

    // Test expire index

    let emap = db.expire_map();

    assert_eq!(
        Marked::new_normal(4, s("a")),
        emap.get(&ExpireKey::new(15_000, 4)).await?
    );
    assert_eq!(
        Marked::new_normal(3, s("c")),
        emap.get(&ExpireKey::new(20_000, 3)).await?
    );
    assert_eq!(Marked::empty(), emap.get(&ExpireKey::new(5_000, 2)).await?);

    let got = emap.range(..).await?.try_collect::<Vec<_>>().await?;
    assert_eq!(
        vec![
            (ExpireKey::new(15_000, 4), Marked::new_normal(4, s("a"))),
            (ExpireKey::new(20_000, 3), Marked::new_normal(3, s("c"))),
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
