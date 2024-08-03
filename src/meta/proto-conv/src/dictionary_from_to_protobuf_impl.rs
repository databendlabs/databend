// Copyright 2021 Datafuse Labs.
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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use databend_common_expression as ex;
use databend_common_meta_app::schema as mt;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_types::NonEmptyString;
use databend_common_protos::pb;

use crate::reader_check_msg;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;

impl FromToProto for mt::DictionaryMeta {
    type PB = pb::DictionaryMeta;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::DictionaryMeta) -> Result<Self, Incompatible>
        where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        let schema = p.schema.ok_or_else(|| Incompatible {
            reason: "DictionaryMeta.schema can not be None".to_string(),
        })?;
        let v = Self {
            name: p.name,
            source: p.source,
            options: p.options,
            schema: ex::TableSchema::from_pb(p.schema),
            primary_column_ids: p.primary_column_ids,
            comment: p.comment,
            created_on: DateTime::<Utc>::from_pb(p.created_on)?,
            dropped_on: match p.dropped_on {
                Some(drop_on) => Some(DateTime::<Utc>::from_pb(drop_on)?),
                None => None,
            },
            updated_on: match p.updated_on {
                Some(update_on) => Some(DateTime::<Utc>::from_pb(update_on)?),
                None => None,
            },
        };
        Ok(v)
    }
    fn to_pb(&self) -> Result<pb::DictionaryMeta, Incompatible> {
        let p = pb::DictionaryMeta {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            name: self.name.clone(),
            source: self.source.clone(),
            options: self.options.clone(),
            primary_column_ids: self.primary_column_ids,
            created_on: self.created_on.to_pb()?,
            dropped_on: match  self.dropped_on {
                Some(dropped_on) => Some(dropped_on.to_pb()?),
                None => None,
            },
            updated_on: match self.updated_on {
                Some(updated_on) => Some(updated_on.to_pb()?),
                None => None,
            },
            comment: self.comment.clone(),
            schema: self.schema?.to_pb(),
        };
        Ok(p)
    }
}
