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

//! The persisted layer of the state machine data.

use std::borrow::Borrow;
use std::future;
use std::io;
use std::ops::RangeBounds;
use std::sync::Arc;

use databend_common_meta_types::snapshot_db::DB;
use databend_common_meta_types::sys_data::SysData;
use databend_common_meta_types::SeqNum;
use databend_common_meta_types::SeqV;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use log::info;
use openraft::SnapshotId;
use rotbl::v001::Rotbl;
use rotbl::v001::SeqMarked;

use crate::config::RaftConfig;
use crate::key_spaces::SMEntry;
use crate::leveled_store::map_api::AsMap;
use crate::leveled_store::map_api::IOResultStream;
use crate::leveled_store::map_api::KVResultStream;
use crate::leveled_store::map_api::MapApiRO;
use crate::leveled_store::map_api::MapKey;
use crate::leveled_store::map_api::MapKeyEncode;
use crate::leveled_store::rotbl_codec::RotblCodec;
use crate::marked::Marked;
use crate::sm_v003::open_snapshot::OpenSnapshot;
use crate::state_machine::ExpireValue;
use crate::state_machine::StateMachineMetaKey;
use crate::state_machine::StateMachineMetaValue;

pub struct DBExporter<'a> {
    db: &'a DB,
}

impl<'a> DBExporter<'a> {
    pub fn new(db: &'a DB) -> Self {
        Self { db }
    }

    pub fn sys_data_sm_entries(&self) -> Result<Vec<SMEntry>, io::Error> {
        let sys_data: SysData = serde_json::from_str(self.db.inner().meta().user_data())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let last_applied = *sys_data.last_applied_ref();
        let last_membership = sys_data.last_membership_ref().clone();

        let mut res = vec![];

        res.push(SMEntry::Sequences {
            key: "generic-kv".to_string(),
            value: SeqNum(sys_data.curr_seq()),
        });

        if let Some(last_applied) = last_applied {
            res.push(SMEntry::StateMachineMeta {
                key: StateMachineMetaKey::LastApplied,
                value: StateMachineMetaValue::LogId(last_applied),
            });
        }

        res.push(SMEntry::StateMachineMeta {
            key: StateMachineMetaKey::LastMembership,
            value: StateMachineMetaValue::Membership(last_membership),
        });

        for (nid, n) in sys_data.nodes_ref().iter() {
            res.push(SMEntry::Nodes {
                key: *nid,
                value: n.clone(),
            });
        }

        Ok(res)
    }

    /// Export all data in a stream of [`SMEntry`].
    ///
    /// First several lines are system data,
    /// including `seq`, `last_applied_log_id`, `last_applied_membership`, and `nodes`.
    ///
    /// The second parts are all of the key values, in alphabetical order,
    /// ExpireKeys(`exp-/`) then Generic KV(`kv--/`);
    pub async fn export(&self) -> Result<IOResultStream<SMEntry>, io::Error> {
        let sys_entries = self.sys_data_sm_entries()?;

        // expire index

        let strm = self.db.expire_map().range(..).await?;
        let expire_strm = strm.try_filter_map(|(k, v)| {
            let exp_val: Option<ExpireValue> = v.into();
            let ent = exp_val.map(|value| SMEntry::Expire { key: k, value });
            future::ready(Ok(ent))
        });

        let strm = self.db.str_map().range(..).await?;
        let kv_strm = strm.try_filter_map(|(k, v)| {
            let seqv: Option<SeqV<_>> = v.into();
            let ent = seqv.map(|value| SMEntry::GenericKV { key: k, value });
            future::ready(Ok(ent))
        });

        let strm = futures::stream::iter(sys_entries)
            .map(Ok)
            .chain(expire_strm)
            .chain(kv_strm);

        Ok(strm.boxed())
    }
}

impl OpenSnapshot for DB {
    fn open_snapshot(
        path: impl ToString,
        snapshot_id: SnapshotId,
        raft_config: &RaftConfig,
    ) -> Result<Self, io::Error> {
        let config = raft_config.to_rotbl_config();
        let r = Rotbl::open(config, path.to_string())?;

        info!("Opened snapshot at {}", path.to_string());

        Ok(Self::new(path, snapshot_id, Arc::new(r))?)
    }
}

#[async_trait::async_trait]
impl<K> MapApiRO<K> for DB
where
    K: MapKey,
    Marked<K::V>: TryFrom<SeqMarked, Error = io::Error>,
{
    async fn get<Q>(&self, key: &Q) -> Result<Marked<K::V>, io::Error>
    where
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
        Q: MapKeyEncode,
    {
        let key = RotblCodec::encode_key(key)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let res = self.rotbl.get(&key).await?;

        let Some(seq_marked) = res else {
            return Ok(Marked::empty());
        };

        let marked = Marked::<K::V>::try_from(seq_marked)?;
        Ok(marked)
    }

    async fn range<R>(&self, range: R) -> Result<KVResultStream<K>, io::Error>
    where R: RangeBounds<K> + Clone + Send + Sync + 'static {
        let rng = RotblCodec::encode_range(&range)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let strm = self.rotbl.range(rng);

        let strm = strm.map(|res_item: Result<(String, SeqMarked), io::Error>| {
            let (str_k, seq_marked) = res_item?;
            let key = RotblCodec::decode_key(&str_k)?;
            let marked = Marked::try_from(seq_marked)?;
            Ok((key, marked))
        });

        Ok(strm.boxed())
    }
}
