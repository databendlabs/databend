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
use databend_common_meta_app::schema as mt;
use databend_common_protos::pb;

use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;
use crate::reader_check_msg;

impl FromToProto for mt::TableBranch {
    type PB = pb::TableBranch;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(Self {
            expire_at: match p.expire_at {
                Some(v) => Some(DateTime::<Utc>::from_pb(v)?),
                None => None,
            },
            branch_id: p.branch_id,
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        Ok(Self::PB {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            expire_at: match self.expire_at {
                Some(v) => Some(v.to_pb()?),
                None => None,
            },
            branch_id: self.branch_id,
        })
    }
}

impl FromToProto for mt::TableTag {
    type PB = pb::TableTag;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(Self {
            expire_at: match p.expire_at {
                Some(v) => Some(DateTime::<Utc>::from_pb(v)?),
                None => None,
            },
            snapshot_loc: p.snapshot_loc,
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        Ok(Self::PB {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            expire_at: match self.expire_at {
                Some(v) => Some(v.to_pb()?),
                None => None,
            },
            snapshot_loc: self.snapshot_loc.clone(),
        })
    }
}
