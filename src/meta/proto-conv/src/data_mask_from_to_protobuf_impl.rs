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

use std::collections::BTreeMap;

use chrono::DateTime;
use chrono::Utc;
use common_meta_app::data_mask as mt;
use common_protos::pb;

use crate::reader_check_msg;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;

impl FromToProto for mt::DatamaskMeta {
    type PB = pb::DatamaskMeta;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::DatamaskMeta) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let v = Self {
            args: p
                .args
                .iter()
                .map(|(arg_name, arg_type)| (arg_name.clone(), arg_type.clone()))
                .collect::<Vec<_>>(),
            return_type: p.return_type,
            body: p.body,
            comment: p.comment.clone(),
            create_on: DateTime::<Utc>::from_pb(p.create_on)?,
            update_on: match p.update_on {
                Some(t) => Some(DateTime::<Utc>::from_pb(t)?),
                None => None,
            },
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::DatamaskMeta, Incompatible> {
        let mut args = BTreeMap::new();
        for (arg_name, arg_type) in &self.args {
            args.insert(arg_name.to_string(), arg_type.to_string());
        }
        let p = pb::DatamaskMeta {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            args,
            return_type: self.return_type.clone(),
            body: self.body.clone(),
            comment: self.comment.clone(),
            create_on: self.create_on.to_pb()?,
            update_on: match &self.update_on {
                Some(t) => Some(t.to_pb()?),
                None => None,
            },
        };
        Ok(p)
    }
}

impl FromToProto for mt::MaskpolicyTableIdList {
    type PB = pb::DbIdList;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::DbIdList) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let v = Self {
            id_list: p.ids.iter().copied().collect(),
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::DbIdList, Incompatible> {
        let p = pb::DbIdList {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            ids: self.id_list.iter().copied().collect(),
        };
        Ok(p)
    }
}
