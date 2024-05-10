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
use num::FromPrimitive;

use crate::reader_check_msg;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;

impl FromToProto for mt::IndexMeta {
    type PB = pb::IndexMeta;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let v = Self {
            table_id: p.table_id,
            index_type: FromPrimitive::from_i32(p.index_type).ok_or_else(|| Incompatible {
                reason: format!("invalid IndexType: {}", p.index_type),
            })?,
            created_on: DateTime::<Utc>::from_pb(p.created_on)?,
            dropped_on: match p.dropped_on {
                Some(drop_on) => Some(DateTime::<Utc>::from_pb(drop_on)?),
                None => None,
            },
            updated_on: match p.updated_on {
                Some(update_on) => Some(DateTime::<Utc>::from_pb(update_on)?),
                None => None,
            },
            original_query: p.original_query,
            query: p.query,
            sync_creation: p.sync_creation,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        let p = pb::IndexMeta {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            table_id: self.table_id,
            index_type: self.index_type.clone() as i32,
            created_on: self.created_on.to_pb()?,
            dropped_on: match self.dropped_on {
                Some(drop_on) => Some(drop_on.to_pb()?),
                None => None,
            },
            updated_on: match self.updated_on {
                Some(update_on) => Some(update_on.to_pb()?),
                None => None,
            },
            original_query: self.original_query.clone(),
            query: self.query.clone(),
            sync_creation: self.sync_creation,
        };
        Ok(p)
    }
}
