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

use std::future;
use std::io;

use databend_common_meta_types::seq_value::SeqV;
use databend_common_meta_types::snapshot_db::DB;
use databend_common_meta_types::SeqNum;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use log::info;
use map_api::map_api_ro::MapApiRO;
use map_api::IOResultStream;

use crate::key_spaces::SMEntry;
use crate::leveled_store::db_map_api_ro_impl::MapView;
use crate::leveled_store::map_api::AsMap;
use crate::state_machine::ExpireValue;
use crate::state_machine::StateMachineMetaKey;
use crate::state_machine::StateMachineMetaValue;

/// Export DB data to a stream of [`SMEntry`].
pub struct DBExporter<'a> {
    db: &'a DB,
}

impl<'a> DBExporter<'a> {
    pub fn new(db: &'a DB) -> Self {
        Self { db }
    }

    /// Convert sys data to a series of [`SMEntry`] for export.
    pub fn sys_data_sm_entries(&self) -> Result<Vec<SMEntry>, io::Error> {
        let sys_data = &self.db.sys_data;

        let last_applied = *sys_data.last_applied_ref();
        let last_membership = sys_data.last_membership_ref().clone();

        let mut res = Vec::with_capacity(3 + sys_data.nodes_ref().len());

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

    /// Export all data in a stream of [`SMEntry`], ignore tombstone.
    ///
    /// First several lines are system data,
    /// including `seq`, `last_applied_log_id`, `last_applied_membership`, and `nodes`.
    ///
    /// The second parts are all the key values, in alphabetical order,
    /// ExpireKeys(`exp-/`) then Generic KV(`kv--/`);
    pub async fn export(&self) -> Result<IOResultStream<SMEntry>, io::Error> {
        let sys_entries = self.sys_data_sm_entries()?;
        info!("DBExporter::export sys_data entries: {:?}", sys_entries);

        // expire index

        let strm = MapView(self.db).as_expire_map().range(..).await?;
        let expire_strm = strm.try_filter_map(|(exp_k, marked)| {
            // Tombstone will be converted to None and be ignored.
            let exp_val = ExpireValue::from_marked(marked);
            let ent = exp_val.map(|value| SMEntry::Expire { key: exp_k, value });
            future::ready(Ok(ent))
        });

        // kv

        let strm = MapView(self.db).as_user_map().range(..).await?;
        let kv_strm = strm.try_filter_map(|(user_key, seq_marked)| {
            // Tombstone will be converted to None and be ignored.
            let seqv: Option<SeqV<_>> = seq_marked.into();
            let ent = seqv.map(|value| SMEntry::GenericKV {
                key: user_key.to_string(),
                value,
            });
            future::ready(Ok(ent))
        });

        let strm = futures::stream::iter(sys_entries)
            .map(Ok)
            .chain(expire_strm)
            .chain(kv_strm);

        Ok(strm.boxed())
    }
}
