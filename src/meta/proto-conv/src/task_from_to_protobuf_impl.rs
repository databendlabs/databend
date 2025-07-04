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

use chrono::DateTime;
use chrono::Utc;
use databend_common_meta_app::principal as mt;
use databend_common_meta_app::principal::task::Status;
use databend_common_protos::pb;

use crate::reader_check_msg;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;

impl FromToProto for mt::Task {
    type PB = pb::Task;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::Task) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let status = match p.status {
            0 => Status::Suspended,
            1 => Status::Started,
            s => {
                return Err(Incompatible::new(format!("Status can not be {s}")));
            }
        };
        let schedule = match p.schedule_options {
            None => None,
            Some(ref s) => {
                if !p.after.is_empty() {
                    None
                } else {
                    let schedule_type = match s.schedule_type {
                        0 => mt::ScheduleType::IntervalType,
                        1 => mt::ScheduleType::CronType,
                        s => {
                            return Err(Incompatible::new(format!("ScheduleType can not be {s}")));
                        }
                    };

                    Some(mt::ScheduleOptions {
                        interval: s.interval,
                        cron: s.cron.clone(),
                        time_zone: s.time_zone.clone(),
                        schedule_type,
                        milliseconds_interval: s.milliseconds_interval,
                    })
                }
            }
        };
        let warehouse = match p.warehouse_options {
            None => None,
            Some(ref w) => Some(mt::WarehouseOptions {
                warehouse: w.warehouse.clone(),
                using_warehouse_size: w.using_warehouse_size.clone(),
            }),
        };
        Ok(Self {
            task_id: p.task_id,
            task_name: p.task_name,
            query_text: p.query_text,
            when_condition: p.when_condition.clone(),
            after: p.after,
            comment: p.comment,
            owner: p.owner,
            owner_user: p.owner_user,
            schedule_options: schedule,
            warehouse_options: warehouse,
            next_scheduled_at: match p.next_scheduled_at {
                Some(c) => Some(DateTime::<Utc>::from_pb(c)?),
                None => None,
            },
            suspend_task_after_num_failures: p.suspend_task_after_num_failures.map(|v| v as u64),
            error_integration: p.error_integration.clone(),
            status,
            created_at: DateTime::<Utc>::from_pb(p.created_at)?,
            updated_at: DateTime::<Utc>::from_pb(p.updated_at)?,
            last_suspended_at: match p.last_suspended_at {
                Some(c) => Some(DateTime::<Utc>::from_pb(c)?),
                None => None,
            },
            session_params: p.session_parameters,
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        Ok(pb::Task {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            task_id: self.task_id,
            task_name: self.task_name.clone(),
            query_text: self.query_text.clone(),
            comment: self.comment.clone(),
            owner: self.owner.clone(),
            schedule_options: match &self.schedule_options {
                None => None,
                Some(s) => Some(pb::ScheduleOptions {
                    interval: s.interval,
                    cron: s.cron.clone(),
                    time_zone: s.time_zone.clone(),
                    schedule_type: s.schedule_type as i32,
                    milliseconds_interval: s.milliseconds_interval,
                }),
            },
            warehouse_options: match &self.warehouse_options {
                None => None,
                Some(w) => Some(pb::WarehouseOptions {
                    warehouse: w.warehouse.clone(),
                    using_warehouse_size: w.using_warehouse_size.clone(),
                }),
            },
            next_scheduled_at: match &self.next_scheduled_at {
                None => None,
                Some(d) => Some(d.to_pb()?),
            },
            suspend_task_after_num_failures: self.suspend_task_after_num_failures.map(|v| v as i32),
            status: self.status as i32,
            created_at: self.created_at.to_pb()?,
            updated_at: self.updated_at.to_pb()?,
            last_suspended_at: match &self.last_suspended_at {
                None => None,
                Some(d) => Some(d.to_pb()?),
            },
            after: self.after.clone(),
            when_condition: self.when_condition.clone(),
            session_parameters: self.session_params.clone(),
            error_integration: self.error_integration.clone(),
            owner_user: self.owner_user.clone(),
        })
    }
}

impl FromToProto for mt::TaskRun {
    type PB = pb::TaskRun;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        Ok(mt::TaskRun {
            task: mt::Task::from_pb(
                p.task
                    .ok_or_else(|| Incompatible::new("State can not be empty"))?,
            )?,
            run_id: p.run_id,
            attempt_number: p.attempt_number,
            state: match p.state {
                0 => mt::State::Scheduled,
                1 => mt::State::Executing,
                2 => mt::State::Succeeded,
                3 => mt::State::Failed,
                4 => mt::State::Cancelled,
                n => return Err(Incompatible::new(format!("State can not be {n}"))),
            },
            scheduled_at: DateTime::<Utc>::from_pb(p.scheduled_at)?,
            completed_at: p.completed_at.map(DateTime::<Utc>::from_pb).transpose()?,
            error_code: p.error_code,
            error_message: p.error_message,
            root_task_id: p.root_task_id,
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        Ok(pb::TaskRun {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            run_id: self.run_id,
            attempt_number: self.attempt_number,
            state: self.state as i32,
            scheduled_at: self.scheduled_at.to_pb()?,
            completed_at: self
                .completed_at
                .as_ref()
                .map(DateTime::<Utc>::to_pb)
                .transpose()?,
            error_code: self.error_code,
            error_message: self.error_message.clone(),
            root_task_id: self.root_task_id,
            task: Some(self.task.to_pb()?),
        })
    }
}
