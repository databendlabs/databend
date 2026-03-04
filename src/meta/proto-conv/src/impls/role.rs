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

use std::collections::HashSet;

use databend_common_meta_app as mt;
use databend_common_protos::pb;

use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;
use crate::reader_check_msg;

impl FromToProto for mt::principal::RoleInfo {
    type PB = pb::RoleInfo;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::RoleInfo) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(mt::principal::RoleInfo {
            name: p.name.clone(),
            grants: if let Some(grants) = p.grants {
                mt::principal::UserGrantSet::from_pb(grants)
                    .unwrap_or_else(|_| mt::principal::UserGrantSet::new(vec![], HashSet::new()))
            } else {
                mt::principal::UserGrantSet::new(vec![], HashSet::new())
            },
            created_on: p
                .created_on
                .map(FromToProto::from_pb)
                .transpose()?
                .unwrap_or_default(),
            update_on: p
                .update_on
                .map(FromToProto::from_pb)
                .transpose()?
                .unwrap_or_default(),
            comment: p.comment.clone(),
        })
    }

    fn to_pb(&self) -> Result<pb::RoleInfo, Incompatible> {
        Ok(pb::RoleInfo {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            name: self.name.clone(),
            grants: Some(mt::principal::UserGrantSet::to_pb(&self.grants)?),
            created_on: Some(self.created_on.to_pb()?),
            update_on: Some(self.update_on.to_pb()?),
            comment: self.comment.clone(),
        })
    }
}
