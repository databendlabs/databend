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

use std::fmt;
use std::io;

use databend_common_meta_types::sys_data::SysData;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use futures_util::future;
use map_api::IOResultStream;
use map_api::MapKV;
use map_api::mvcc::ScopedSeqBoundedRange;
use seq_marked::SeqMarked;
use state_machine_api::ExpireKey;
use state_machine_api::UserKey;
use stream_more::KMerge;
use stream_more::StreamMore;

use crate::leveled_store::immutable_data::ImmutableData;
use crate::leveled_store::rotbl_codec::RotblCodec;
use crate::leveled_store::util;
use crate::utils::add_cooperative_yielding;

impl ImmutableData {
    /// Compacted all data into a stream.
    ///
    /// Tombstones are removed because no more compact with lower levels.
    ///
    /// It returns a small chunk of sys data that is always copied across levels,
    /// and a stream contains `kv` and `expire` entries.
    /// The stream Item is 2 items tuple of key, and value with seq.
    ///
    /// The exported stream contains encoded `String` key and rotbl value [`SeqMarked`]
    // TODO: mvcc snapshot_seq
    pub async fn compact_into_stream(
        &self,
    ) -> Result<(SysData, IOResultStream<(String, SeqMarked)>), io::Error> {
        fn with_context(e: io::Error, key: &impl fmt::Debug) -> io::Error {
            io::Error::new(
                e.kind(),
                format!("{}, while encoding kv, key: {:?}", e, key),
            )
        }

        let immutable_levels = self.levels();
        let d = immutable_levels.newest().unwrap();

        let sys_data = d.sys_data().clone();

        // expire index: prefix `exp-/`.

        let strm = immutable_levels
            .range(ExpireKey::default().., u64::MAX)
            .await?;
        let expire_strm = strm.map(|item: Result<(ExpireKey, SeqMarked<String>), io::Error>| {
            let (expire_key, marked_string) = item?;

            RotblCodec::encode_key_seq_marked(&expire_key, marked_string)
                .map_err(|e| with_context(e, &expire_key))
        });

        // kv: prefix: `kv--/`

        let strm = immutable_levels
            .range(UserKey::default().., u64::MAX)
            .await?;
        let kv_strm = strm.map(|item: Result<MapKV<UserKey>, io::Error>| {
            let (k, v) = item?;

            RotblCodec::encode_key_seq_marked(&k, v).map_err(|e| with_context(e, &k))
        });

        // `exp-/*` < `kv--/`: expire keys should be output first to maintain the order.
        let strm = expire_strm.chain(kv_strm);

        let mut kmerge = KMerge::by(util::rotbl_by_key_seq);
        kmerge = kmerge.merge(strm);

        if let Some(db) = self.persisted() {
            let db_strm = db.inner_range();
            kmerge = kmerge.merge(db_strm);
        }

        let coalesce = kmerge.coalesce(util::rotbl_choose_greater);

        // Filter out tombstone
        let normal_strm = coalesce.try_filter(|(_k, v)| future::ready(v.is_normal()));

        let normal_strm = add_cooperative_yielding(normal_strm, "compact");

        Ok((sys_data, normal_strm.boxed()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::leveled_store::testing_data::build_2_levels_leveled_map_with_expire;
    use crate::leveled_store::testing_data::build_3_levels_leveled_map;

    #[tokio::test]
    async fn test_compact_3_level() -> anyhow::Result<()> {
        let lm = build_3_levels_leveled_map().await?;
        println!("{:#?}", lm);

        lm.freeze_writable_without_permit();

        let immutable_data = lm.immutable_data();

        let (sys_data, strm) = immutable_data.compact_into_stream().await?;
        assert_eq!(
            r#"{"last_applied":{"leader_id":{"term":3,"node_id":3},"index":3},"last_membership":{"log_id":{"leader_id":{"term":3,"node_id":3},"index":3},"membership":{"configs":[],"nodes":{}}},"nodes":{"3":{"name":"3","endpoint":{"addr":"3","port":3},"grpc_api_advertise_address":null}},"sequence":7,"data_seq":2}"#,
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
        let leveled_map = build_2_levels_leveled_map_with_expire().await?;
        leveled_map.freeze_writable_without_permit();

        let immutable_data = leveled_map.immutable_data();

        let (sys_data, strm) = immutable_data.compact_into_stream().await?;
        let got = strm
            .map_ok(|x| serde_json::to_string(&x).unwrap())
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(
            r#"{"last_applied":null,"last_membership":{"log_id":null,"membership":{"configs":[],"nodes":{}}},"nodes":{},"sequence":4,"data_seq":1}"#,
            serde_json::to_string(&sys_data).unwrap()
        );

        assert_eq!(got, vec![
            r#"["exp-/00000000000000005000/00000000000000000002",{"seq":2,"marked":{"Normal":[1,4,110,117,108,108,1,98]}}]"#,
            r#"["exp-/00000000000000015000/00000000000000000004",{"seq":4,"marked":{"Normal":[1,4,110,117,108,108,1,97]}}]"#,
            r#"["exp-/00000000000000020000/00000000000000000003",{"seq":3,"marked":{"Normal":[1,4,110,117,108,108,1,99]}}]"#,
            r#"["kv--/a",{"seq":4,"marked":{"Normal":[1,35,123,34,101,120,112,105,114,101,95,97,116,34,58,49,53,44,34,112,114,111,112,111,115,101,100,95,97,116,95,109,115,34,58,48,125,2,97,49]}}]"#,
            r#"["kv--/b",{"seq":2,"marked":{"Normal":[1,34,123,34,101,120,112,105,114,101,95,97,116,34,58,53,44,34,112,114,111,112,111,115,101,100,95,97,116,95,109,115,34,58,48,125,2,98,48]}}]"#,
            r#"["kv--/c",{"seq":3,"marked":{"Normal":[1,35,123,34,101,120,112,105,114,101,95,97,116,34,58,50,48,44,34,112,114,111,112,111,115,101,100,95,97,116,95,109,115,34,58,48,125,2,99,48]}}]"#,
        ]);

        Ok(())
    }
}
