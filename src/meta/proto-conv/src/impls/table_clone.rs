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

use databend_common_meta_app::schema::TableCloneBinding;
use databend_common_protos::pb::TableCloneBinding as PbTableCloneBinding;

use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;
use crate::reader_check_msg;

impl FromToProto for TableCloneBinding {
    type PB = PbTableCloneBinding;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        Ok(Self {
            source_table_id: p.source_table_id,
        })
    }

    fn to_pb(&self) -> Self::PB {
        PbTableCloneBinding {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            source_table_id: self.source_table_id,
        }
    }
}
