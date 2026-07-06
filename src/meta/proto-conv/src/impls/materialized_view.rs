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

use databend_common_meta_app::schema as mt;
use databend_common_protos::pb;

use crate::FromProtoOptionExt;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::ToProtoOptionExt;
use crate::VER;
use crate::reader_check_msg;

impl FromToProto for mt::SourceProgress {
    type PB = pb::SourceProgress;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(Self {
            table_meta_seq: p.table_meta_seq,
            snapshot_location: p.snapshot_location,
        })
    }

    fn to_pb(&self) -> Self::PB {
        pb::SourceProgress {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            table_meta_seq: self.table_meta_seq,
            snapshot_location: self.snapshot_location.clone(),
        }
    }
}

impl FromToProto for mt::MVMeta {
    type PB = pb::MvMeta;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let state = match p.state {
            0 => mt::MVState::Valid,
            1 => mt::MVState::Stale,
            2 => mt::MVState::Refreshing,
            3 => mt::MVState::Failed,
            state => return Err(Incompatible::new(format!("MVState can not be {state}"))),
        };

        let source_progress = p.source_progress.ok_or_else(|| {
            Incompatible::new("MVMeta.source_progress can not be None".to_string())
        })?;

        Ok(Self {
            source_table_id: p.source_table_id,
            source_progress: mt::SourceProgress::from_pb(source_progress)?,
            state,
            last_refresh_time: p.last_refresh_time.from_pb_opt()?,
            last_refresh_error: p.last_refresh_error,
        })
    }

    fn to_pb(&self) -> Self::PB {
        pb::MvMeta {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            source_table_id: self.source_table_id,
            source_progress: Some(self.source_progress.to_pb()),
            state: match self.state {
                mt::MVState::Valid => 0,
                mt::MVState::Stale => 1,
                mt::MVState::Refreshing => 2,
                mt::MVState::Failed => 3,
            },
            last_refresh_time: self.last_refresh_time.to_pb_opt(),
            last_refresh_error: self.last_refresh_error.clone(),
        }
    }
}

impl FromToProto for mt::MVDefinition {
    type PB = pb::MvDefinition;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(Self {
            original_query: p.original_query,
            query: p.query,
        })
    }

    fn to_pb(&self) -> Self::PB {
        pb::MvDefinition {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            original_query: self.original_query.clone(),
            query: self.query.clone(),
        }
    }
}
