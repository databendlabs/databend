use std::str::FromStr;

use chrono::DateTime;
use chrono::Utc;
use common_meta_app as mt;
use common_meta_app::background::BackgroundJobParams;
use common_meta_app::background::BackgroundJobStatus;
use common_protos::pb;
use num::FromPrimitive;

use crate::reader_check_msg;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;

impl FromToProto for mt::background::BackgroundJobInfo {
    type PB = pb::BackgroundJobInfo;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        Ok(Self {
            job_params: p
                .job_params
                .and_then(|t| BackgroundJobParams::from_pb(t).ok()),
            job_status: p
                .job_status
                .and_then(|t| BackgroundJobStatus::from_pb(t).ok()),
            task_type: FromPrimitive::from_i32(p.task_type).ok_or_else(|| Incompatible {
                reason: format!("invalid TaskType: {}", p.task_type),
            })?,

            last_updated: p
                .last_updated
                .and_then(|t| DateTime::<Utc>::from_pb(t).ok()),
            message: p.message,
            creator: match p.creator {
                Some(c) => Some(mt::principal::UserIdentity::from_pb(c)?),
                None => None,
            },
            created_at: DateTime::<Utc>::from_pb(p.created_at)?,
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        let p = pb::BackgroundJobInfo {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            job_params: self.job_params.clone().and_then(|t| t.to_pb().ok()),
            job_status: self.job_status.clone().and_then(|t| t.to_pb().ok()),
            task_type: self.task_type.clone() as i32,
            last_updated: self.last_updated.and_then(|t| t.to_pb().ok()),
            message: self.message.clone(),
            creator: self.creator.clone().and_then(|c| c.to_pb().ok()),
            created_at: self.created_at.to_pb()?,
        };
        Ok(p)
    }
}

impl FromToProto for mt::background::BackgroundJobParams {
    type PB = pb::BackgroundJobParams;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(Self {
            job_type: FromPrimitive::from_i32(p.job_type).ok_or_else(|| Incompatible {
                reason: format!("invalid job type: {}", p.job_type),
            })?,
            scheduled_job_interval: std::time::Duration::from_secs(
                p.scheduled_job_interval_seconds,
            ),
            scheduled_job_cron: p.scheduled_job_cron,
            scheduled_job_timezone: p
                .scheduled_job_timezone
                .map(|t| {
                    chrono_tz::Tz::from_str(&t).map_err(|e| Incompatible {
                        reason: format!("invalid timezone: {}", e),
                    })
                })
                .transpose()?,
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        let p = pb::BackgroundJobParams {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            job_type: self.job_type.clone() as i32,
            scheduled_job_interval_seconds: self.scheduled_job_interval.as_secs(),
            scheduled_job_cron: self.scheduled_job_cron.clone(),
            scheduled_job_timezone: self.scheduled_job_timezone.map(|t| t.name().to_string()),
        };
        Ok(p)
    }
}

impl FromToProto for BackgroundJobStatus {
    type PB = pb::BackgroundJobStatus;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(Self {
            job_state: FromPrimitive::from_i32(p.job_state).ok_or_else(|| Incompatible {
                reason: format!("invalid JobState: {}", p.job_state),
            })?,
            last_task_id: p.last_task_id,
            last_task_run_at: p
                .last_task_run_at
                .and_then(|t| DateTime::<Utc>::from_pb(t).ok()),
            next_task_scheduled_time: p
                .next_task_scheduled_time
                .and_then(|t| DateTime::<Utc>::from_pb(t).ok()),
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        Ok(pb::BackgroundJobStatus {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            job_state: self.job_state.clone() as i32,
            last_task_id: self.last_task_id.clone(),
            last_task_run_at: self.last_task_run_at.map(|t| t.to_pb()).transpose()?,
            next_task_scheduled_time: self
                .next_task_scheduled_time
                .map(|t| t.to_pb())
                .transpose()?,
        })
    }
}

impl FromToProto for mt::background::BackgroundJobIdent {
    type PB = pb::BackgroundJobIdent;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        Ok(Self {
            tenant: p.tenant.to_string(),
            name: p.name,
        })
    }
    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        let p = pb::BackgroundJobIdent {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            tenant: self.tenant.clone(),
            name: self.name.clone(),
        };
        Ok(p)
    }
}
