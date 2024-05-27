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

use std::future;
use std::io;

use databend_common_meta_types::SeqNum;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::SnapshotMeta;
use futures_util::StreamExt;
use futures_util::TryStreamExt;

use crate::key_spaces::SMEntry;
use crate::leveled_store::immutable::Immutable;
use crate::leveled_store::immutable_levels::ImmutableLevels;
use crate::leveled_store::map_api::AsMap;
use crate::leveled_store::map_api::MapApiRO;
use crate::leveled_store::map_api::ResultStream;
use crate::leveled_store::sys_data_api::SysDataApiRO;
use crate::ondisk::Header;
use crate::ondisk::OnDisk;
use crate::state_machine::ExpireValue;
use crate::state_machine::MetaSnapshotId;
use crate::state_machine::StateMachineMetaKey;
use crate::state_machine::StateMachineMetaValue;

/// A snapshot view of a state machine, which is static and not affected by further writing to the state machine.
pub struct SnapshotViewV002 {
    /// The compacted snapshot data.
    compacted: ImmutableLevels,

    /// Original non compacted snapshot data.
    ///
    /// This is kept just for debug.
    original: ImmutableLevels,
}

impl SnapshotViewV002 {
    pub fn new(top: ImmutableLevels) -> Self {
        Self {
            compacted: top.clone(),
            original: top,
        }
    }

    /// Return the data level of this snapshot
    pub fn compacted(&self) -> ImmutableLevels {
        self.compacted.clone()
    }

    /// The original, non compacted snapshot data.
    pub fn original_ref(&self) -> &ImmutableLevels {
        &self.original
    }

    /// Extract metadata of the snapshot.
    ///
    /// Includes: last_applied, last_membership, snapshot_id.
    // TODO: let the caller specify snapshot id?
    pub fn build_snapshot_meta(&self) -> SnapshotMeta {
        // The top level contains all information we need to build snapshot meta.
        let compacted = self.compacted();
        let level_data = compacted.newest().unwrap().as_ref();

        let last_applied = *level_data.last_applied_ref();
        let last_membership = level_data.last_membership_ref().clone();

        let snapshot_id = MetaSnapshotId::new_with_epoch(last_applied);

        SnapshotMeta {
            snapshot_id: snapshot_id.to_string(),
            last_log_id: last_applied,
            last_membership,
        }
    }

    /// Compact into one level and remove all tombstone record.
    pub async fn compact_mem_levels(&mut self) -> Result<(), io::Error> {
        if self.compacted.len() <= 1 {
            return Ok(());
        }

        // TODO: use a explicit method to return a compaction base
        let mut data = self.compacted.newest().unwrap().new_level();

        // `range()` will compact tombstone internally
        let strm = self.compacted.str_map().range(..).await?;
        let strm = strm.try_filter(|(_k, v)| future::ready(v.is_normal()));

        let bt = strm.try_collect().await?;

        data.replace_kv(bt);

        // `range()` will compact tombstone internally
        let strm = self.compacted.expire_map().range(..).await?;
        let strm = strm.try_filter(|(_k, v)| future::ready(v.is_normal()));

        let bt = strm.try_collect().await?;

        data.replace_expire(bt);

        self.compacted = ImmutableLevels::new([Immutable::new_from_level(data)]);
        Ok(())
    }

    /// Export all its data in SMEntry format.
    // pub async fn export(&self) -> Result<impl Stream<Item = SMEntry> + '_, io::Error> {
    pub async fn export(&self) -> Result<ResultStream<SMEntry>, io::Error> {
        let d = self.compacted.newest().unwrap();

        let mut sm_meta = vec![];

        // Data header to identify snapshot version

        sm_meta.push(SMEntry::DataHeader {
            key: OnDisk::KEY_HEADER.to_string(),
            value: Header::this_version(),
        });

        // Last applied

        if let Some(last_applied) = d.last_applied_ref() {
            sm_meta.push(SMEntry::StateMachineMeta {
                key: StateMachineMetaKey::LastApplied,
                value: StateMachineMetaValue::LogId(*last_applied),
            })
        }

        // Last membership

        {
            let last_membership = d.last_membership_ref();
            sm_meta.push(SMEntry::StateMachineMeta {
                key: StateMachineMetaKey::LastMembership,
                value: StateMachineMetaValue::Membership(last_membership.clone()),
            })
        }

        // Sequence

        sm_meta.push(SMEntry::Sequences {
            // Use this fixed key `generic-kv` for back compatibility:
            // Only this key is used.
            key: s("generic-kv"),
            value: SeqNum(d.curr_seq()),
        });

        // Nodes

        for (node_id, node) in d.nodes_ref().iter() {
            sm_meta.push(SMEntry::Nodes {
                key: *node_id,
                value: node.clone(),
            })
        }

        // kv

        let strm = self.compacted.str_map().range(..).await?;
        let kv_iter = strm.try_filter_map(|(k, v)| {
            let seqv: Option<SeqV<_>> = v.into();
            let ent = seqv.map(|value| SMEntry::GenericKV { key: k, value });
            future::ready(Ok(ent))
        });

        // expire index

        let strm = self.compacted.expire_map().range(..).await?;
        let expire_iter = strm.try_filter_map(|(k, v)| {
            let exp_val: Option<ExpireValue> = v.into();
            let ent = exp_val.map(|value| SMEntry::Expire { key: k, value });
            future::ready(Ok(ent))
        });

        let strm = futures::stream::iter(sm_meta)
            .map(Ok)
            .chain(kv_iter)
            .chain(expire_iter);

        Ok(strm.boxed())
    }
}

fn s(x: impl ToString) -> String {
    x.to_string()
}
