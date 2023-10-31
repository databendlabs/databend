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

//! This mod is the key point about compatibility.
//! Everytime update anything in this file, update the `VER` and let the tests pass.

use chrono::DateTime;
use chrono::Utc;
use common_meta_app::schema as mt;
use common_protos::pb;

use crate::reader_check_msg;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;

impl FromToProto for mt::LockLevel {
    type PB = pb::LockLevel;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: pb::LockLevel) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        match p.level {
            Some(pb::lock_level::Level::Table(pb::lock_level::Table {})) => {
                Ok(mt::LockLevel::Table)
            }
            None => Err(Incompatible {
                reason: "LockLevel cannot be None".to_string(),
            }),
        }
    }

    fn to_pb(&self) -> Result<pb::LockLevel, Incompatible> {
        let level = match self {
            mt::LockLevel::Table => Some(pb::lock_level::Level::Table(pb::lock_level::Table {})),
        };
        Ok(pb::LockLevel {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            level,
        })
    }
}

impl FromToProto for mt::LockMeta {
    type PB = pb::LockMeta;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: pb::LockMeta) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let v = Self {
            user: p.user,
            node: p.node,
            session_id: p.session_id,
            created_on: DateTime::<Utc>::from_pb(p.created_on)?,
            acquired_on: match p.acquired_on {
                Some(acquired_on) => Some(DateTime::<Utc>::from_pb(acquired_on)?),
                None => None,
            },
            level: mt::LockLevel::from_pb(p.level.ok_or_else(|| Incompatible {
                reason: "LockMeta.level cannot be None".to_string(),
            })?)?,
        };

        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::LockMeta, Incompatible> {
        let p = pb::LockMeta {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            user: self.user.clone(),
            node: self.node.clone(),
            session_id: self.session_id.clone(),
            created_on: self.created_on.to_pb()?,
            acquired_on: match self.acquired_on {
                Some(acquired_on) => Some(acquired_on.to_pb()?),
                None => None,
            },
            level: Some(mt::LockLevel::to_pb(&self.level)?),
        };
        Ok(p)
    }
}
