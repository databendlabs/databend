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
use databend_common_protos::pb::task_message::DeleteTask;
use databend_common_protos::pb::task_message::Message;

use crate::FromProtoOptionExt;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::ToProtoOptionExt;
use crate::VER;
use crate::reader_check_msg;

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
        let schedule = p
            .schedule_options
            .as_ref()
            .map(|s| {
                let schedule_type = match s.schedule_type {
                    0 => mt::ScheduleType::IntervalType,
                    1 => mt::ScheduleType::CronType,
                    s => {
                        return Err(Incompatible::new(format!("ScheduleType can not be {s}")));
                    }
                };

                Ok(mt::ScheduleOptions {
                    interval: s.interval,
                    cron: s.cron.clone(),
                    time_zone: s.time_zone.clone(),
                    schedule_type,
                    milliseconds_interval: s.milliseconds_interval,
                })
            })
            .transpose()?;

        let warehouse = p.warehouse_options.as_ref().map(|w| mt::WarehouseOptions {
            warehouse: w.warehouse.clone(),
            using_warehouse_size: w.using_warehouse_size.clone(),
        });
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
            next_scheduled_at: p.next_scheduled_at.from_pb_opt()?,
            suspend_task_after_num_failures: p.suspend_task_after_num_failures.map(|v| v as u64),
            error_integration: p.error_integration.clone(),
            status,
            created_at: DateTime::<Utc>::from_pb(p.created_at)?,
            updated_at: DateTime::<Utc>::from_pb(p.updated_at)?,
            last_suspended_at: p.last_suspended_at.from_pb_opt()?,
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
            schedule_options: self.schedule_options.as_ref().map(|s| pb::ScheduleOptions {
                interval: s.interval,
                cron: s.cron.clone(),
                time_zone: s.time_zone.clone(),
                schedule_type: s.schedule_type as i32,
                milliseconds_interval: s.milliseconds_interval,
            }),
            warehouse_options: self
                .warehouse_options
                .as_ref()
                .map(|w| pb::WarehouseOptions {
                    warehouse: w.warehouse.clone(),
                    using_warehouse_size: w.using_warehouse_size.clone(),
                }),
            next_scheduled_at: self.next_scheduled_at.to_pb_opt()?,
            suspend_task_after_num_failures: self.suspend_task_after_num_failures.map(|v| v as i32),
            status: self.status as i32,
            created_at: self.created_at.to_pb()?,
            updated_at: self.updated_at.to_pb()?,
            last_suspended_at: self.last_suspended_at.to_pb_opt()?,
            after: self.after.clone(),
            when_condition: self.when_condition.clone(),
            session_parameters: self.session_params.clone(),
            error_integration: self.error_integration.clone(),
            owner_user: self.owner_user.clone(),
        })
    }
}

impl FromToProto for mt::TaskMessage {
    type PB = pb::TaskMessage;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        Ok(match p.message {
            None => return Err(Incompatible::new("message can not be empty")),
            Some(message) => match message {
                Message::ExecuteTask(task) => {
                    mt::TaskMessage::ExecuteTask(mt::Task::from_pb(task)?)
                }
                Message::ScheduleTask(task) => {
                    mt::TaskMessage::ScheduleTask(mt::Task::from_pb(task)?)
                }
                Message::DeleteTask(task_name) => mt::TaskMessage::DeleteTask(task_name, None),
                Message::DeleteTaskV2(DeleteTask {
                    task_name,
                    warehouse_options,
                }) => {
                    let warehouse = warehouse_options.as_ref().map(|w| mt::WarehouseOptions {
                        warehouse: w.warehouse.clone(),
                        using_warehouse_size: w.using_warehouse_size.clone(),
                    });
                    mt::TaskMessage::DeleteTask(task_name, warehouse)
                }
                Message::AfterTask(task) => mt::TaskMessage::AfterTask(mt::Task::from_pb(task)?),
            },
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        let message = match self {
            mt::TaskMessage::ExecuteTask(task) => Message::ExecuteTask(task.to_pb()?),
            mt::TaskMessage::ScheduleTask(task) => Message::ScheduleTask(task.to_pb()?),
            mt::TaskMessage::DeleteTask(task_name, warehouse_options) => {
                Message::DeleteTaskV2(DeleteTask {
                    task_name: task_name.clone(),
                    warehouse_options: warehouse_options.as_ref().map(|w| pb::WarehouseOptions {
                        warehouse: w.warehouse.clone(),
                        using_warehouse_size: w.using_warehouse_size.clone(),
                    }),
                })
            }
            mt::TaskMessage::AfterTask(task) => Message::AfterTask(task.to_pb()?),
        };

        Ok(pb::TaskMessage {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            message: Some(message),
        })
    }
}
