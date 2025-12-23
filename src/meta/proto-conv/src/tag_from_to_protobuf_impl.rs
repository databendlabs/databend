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

impl FromToProto for mt::TagMeta {
    type PB = pb::TagMeta;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let allowed_values = if p.allowed_values.is_empty() {
            None
        } else {
            Some(p.allowed_values)
        };

        Ok(Self {
            allowed_values,
            comment: p.comment,
            created_on: DateTime::<Utc>::from_pb(p.created_on)?,
            updated_on: match p.updated_on {
                Some(v) => Some(DateTime::<Utc>::from_pb(v)?),
                None => None,
            },
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        Ok(Self::PB {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            allowed_values: self.allowed_values.clone().unwrap_or_default(),
            comment: self.comment.clone(),
            created_on: self.created_on.to_pb()?,
            updated_on: match self.updated_on {
                Some(v) => Some(v.to_pb()?),
                None => None,
            },
        })
    }
}

impl FromToProto for mt::ObjectTagIdRefValue {
    type PB = pb::TagRefValue;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        Ok(Self {
            tag_allowed_value: p.tag_allowed_value,
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        Ok(Self::PB {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            tag_allowed_value: self.tag_allowed_value.clone(),
        })
    }
}
