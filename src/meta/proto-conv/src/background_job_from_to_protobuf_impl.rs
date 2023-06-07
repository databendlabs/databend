use chrono::DateTime;
use chrono::Utc;
use common_meta_app as mt;
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
            task_type: FromPrimitive::from_i32(p.task_type).ok_or_else(|| Incompatible {
                reason: format!("invalid TaskType: {}", p.task_type),
            })?,
            job_state: FromPrimitive::from_i32(p.job_state).ok_or_else(|| Incompatible {
                reason: format!("invalid JobState: {}", p.job_state),
            })?,
            job_type: FromPrimitive::from_i32(p.job_type).ok_or_else(|| Incompatible {
                reason: format!("invalid JobType: {}", p.job_type),
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
            task_type: self.task_type.clone() as i32,
            job_state: self.job_state.clone() as i32,
            job_type: self.job_type.clone() as i32,
            last_updated: self.last_updated.and_then(|t| t.to_pb().ok()),
            message: self.message.clone(),
            creator: self.creator.clone().and_then(|c| c.to_pb().ok()),
            created_at: self.created_at.to_pb()?,
        };
        return Ok(p);
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
            name: p.name.to_string(),
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
