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

use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use databend_common_meta_app as mt;
use databend_common_meta_app::background::ManualTriggerParams;
use databend_common_meta_app::schema::TableStatistics;
use databend_common_protos::pb;
use num::FromPrimitive;

use crate::reader_check_msg;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;

impl FromToProto for mt::background::BackgroundTaskInfo {
    type PB = pb::BackgroundTaskInfo;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        Ok(Self {
            last_updated: p
                .last_updated
                .and_then(|t| DateTime::<Utc>::from_pb(t).ok()),
            task_type: FromPrimitive::from_i32(p.task_type).ok_or_else(|| Incompatible {
                reason: format!("invalid TaskType: {}", p.task_type),
            })?,
            task_state: FromPrimitive::from_i32(p.task_state).ok_or_else(|| Incompatible {
                reason: format!("invalid TaskState: {}", p.task_state),
            })?,
            message: p.message,
            compaction_task_stats: p
                .compaction_task_stats
                .and_then(|t| mt::background::CompactionStats::from_pb(t).ok()),
            vacuum_stats: p
                .vacuum_stats
                .and_then(|t| mt::background::VacuumStats::from_pb(t).ok()),
            manual_trigger: p
                .manual_trigger
                .and_then(|t| ManualTriggerParams::from_pb(t).ok()),
            creator: match p.creator {
                Some(c) => Some(mt::background::task_creator::BackgroundTaskCreator::from_pb(c)?),
                None => None,
            },
            created_at: DateTime::<Utc>::from_pb(p.created_at)?,
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        let p = pb::BackgroundTaskInfo {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            last_updated: self.last_updated.and_then(|t| t.to_pb().ok()),
            task_type: self.task_type.clone() as i32,
            task_state: self.task_state.clone() as i32,
            message: self.message.clone(),
            compaction_task_stats: self
                .compaction_task_stats
                .clone()
                .and_then(|t| t.to_pb().ok()),
            vacuum_stats: self.vacuum_stats.clone().and_then(|t| t.to_pb().ok()),
            manual_trigger: self.manual_trigger.clone().and_then(|t| t.to_pb().ok()),
            creator: self.creator.as_ref().and_then(|c| c.to_pb().ok()),
            created_at: self.created_at.to_pb()?,
        };
        Ok(p)
    }
}

impl FromToProto for mt::background::CompactionStats {
    type PB = pb::CompactionStats;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        Ok(Self {
            db_id: p.db_id,
            table_id: p.table_id,
            before_compaction_stats: p
                .before_compaction_stats
                .clone()
                .and_then(|t| TableStatistics::from_pb(t).ok()),
            after_compaction_stats: p
                .after_compaction_stats
                .clone()
                .and_then(|t| TableStatistics::from_pb(t).ok()),
            total_compaction_time: p
                .total_compaction_time_secs
                .and_then(|t| Option::from(Duration::from_secs_f32(t))),
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        let p = pb::CompactionStats {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            db_id: self.db_id,
            table_id: self.table_id,
            before_compaction_stats: self
                .before_compaction_stats
                .clone()
                .and_then(|t| t.to_pb().ok()),
            after_compaction_stats: self
                .after_compaction_stats
                .clone()
                .and_then(|t| t.to_pb().ok()),
            total_compaction_time_secs: self.total_compaction_time.map(|t| t.as_secs_f32()),
        };
        Ok(p)
    }
}

impl FromToProto for mt::background::VacuumStats {
    type PB = pb::VacuumStats;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        Ok(Self {})
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        let p = pb::VacuumStats {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
        };
        Ok(p)
    }
}
