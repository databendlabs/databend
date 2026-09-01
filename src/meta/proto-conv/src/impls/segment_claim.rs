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
use databend_common_meta_app::schema as mt;
use databend_common_protos::pb;

use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;
use crate::reader_check_msg;

impl FromToProto for mt::SegmentClaimMeta {
    type PB = pb::SegmentClaimMeta;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: pb::SegmentClaimMeta) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(Self {
            user: p.user,
            node: p.node,
            query_id: p.query_id,
            created_on: DateTime::<Utc>::from_pb(p.created_on)?,
            segment_locations: p.segment_locations.into_iter().collect(),
        })
    }

    fn to_pb(&self) -> pb::SegmentClaimMeta {
        pb::SegmentClaimMeta {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            user: self.user.clone(),
            node: self.node.clone(),
            query_id: self.query_id.clone(),
            created_on: self.created_on.to_pb(),
            segment_locations: self.segment_locations.iter().cloned().collect(),
        }
    }
}
