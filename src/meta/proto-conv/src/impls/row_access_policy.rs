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
use databend_common_meta_app::row_access_policy as mt;
use databend_common_protos::pb;

use crate::FromProtoOptionExt;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::ToProtoOptionExt;
use crate::VER;
use crate::reader_check_msg;

impl FromToProto for mt::RowAccessPolicyMeta {
    type PB = pb::RowAccessPolicyMeta;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::RowAccessPolicyMeta) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        // Prioritize args_v2 (preserves order), fallback to args (backward compatibility)
        let args: Vec<(String, String)> = if !p.args_v2.is_empty() {
            p.args_v2
                .into_iter()
                .map(|arg| (arg.name, arg.r#type))
                .collect()
        } else {
            // Backward compatibility: read from old args map
            // Note: BTreeMap sorts keys alphabetically, so order may be lost for old data
            p.args.into_iter().collect()
        };

        let v = Self {
            args,
            body: p.body,
            comment: p.comment.clone(),
            create_on: DateTime::<Utc>::from_pb(p.create_on)?,
            update_on: p.update_on.from_pb_opt()?,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::RowAccessPolicyMeta, Incompatible> {
        // Write to args_v2 (new format that preserves order)
        let args_v2: Vec<pb::RowAccessPolicyArg> = self
            .args
            .iter()
            .map(|(arg_name, arg_type)| pb::RowAccessPolicyArg {
                name: arg_name.clone(),
                r#type: arg_type.clone(),
            })
            .collect();

        let p = pb::RowAccessPolicyMeta {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            args: BTreeMap::new(), // Keep empty for backward compatibility
            args_v2,
            body: self.body.clone(),
            comment: self.comment.clone(),
            create_on: self.create_on.to_pb()?,
            update_on: self.update_on.to_pb_opt()?,
        };
        Ok(p)
    }
}
