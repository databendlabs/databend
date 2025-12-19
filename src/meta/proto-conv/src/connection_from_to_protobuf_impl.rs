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

use databend_common_meta_app::principal as mt;
use databend_common_protos::pb;

use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;
use crate::reader_check_msg;

impl FromToProto for mt::UserDefinedConnection {
    type PB = pb::UserDefinedConnection;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(Self {
            name: p.name,
            storage_type: p.storage_type,
            storage_params: p.storage_params,
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        Ok(Self::PB {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            name: self.name.clone(),
            storage_type: self.storage_type.clone(),
            storage_params: self.storage_params.clone(),
        })
    }
}
