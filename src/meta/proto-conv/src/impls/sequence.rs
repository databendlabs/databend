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
use databend_common_meta_app::schema::SequenceMeta;
use databend_common_protos::pb;

use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;
use crate::reader_check_msg;

impl FromToProto for SequenceMeta {
    type PB = pb::SequenceMeta;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::SequenceMeta) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let v = Self {
            comment: p.comment.clone(),
            create_on: DateTime::<Utc>::from_pb(p.create_on)?,
            update_on: DateTime::<Utc>::from_pb(p.update_on)?,
            #[allow(deprecated)]
            current: p.current,
            step: p.step,
            storage_version: p.storage_version,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::SequenceMeta, Incompatible> {
        let p = pb::SequenceMeta {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            comment: self.comment.clone(),
            create_on: self.create_on.to_pb()?,
            update_on: self.update_on.to_pb()?,
            #[allow(deprecated)]
            current: self.current,
            step: self.step,
            storage_version: self.storage_version,
        };
        Ok(p)
    }
}
