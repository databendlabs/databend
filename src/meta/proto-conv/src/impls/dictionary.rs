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

use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use databend_common_expression as ex;
use databend_common_meta_app::schema as mt;
use databend_common_protos::pb;

use crate::FromProtoOptionExt;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::ToProtoOptionExt;
use crate::VER;
use crate::reader_check_msg;

impl FromToProto for mt::DictionaryMeta {
    type PB = pb::DictionaryMeta;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::DictionaryMeta) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        let schema = p.schema.ok_or_else(|| {
            Incompatible::new("DictionaryMeta.schema can not be None".to_string())
        })?;
        let v = Self {
            source: p.source,
            options: p.options,
            schema: Arc::new(ex::TableSchema::from_pb(schema)?),
            primary_column_ids: p.primary_column_ids,
            comment: p.comment,
            created_on: DateTime::<Utc>::from_pb(p.created_on)?,
            updated_on: p.updated_on.from_pb_opt()?,
            field_comments: p.field_comments,
        };
        Ok(v)
    }
    fn to_pb(&self) -> Result<pb::DictionaryMeta, Incompatible> {
        let p = pb::DictionaryMeta {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            source: self.source.clone(),
            options: self.options.clone(),
            primary_column_ids: self.primary_column_ids.clone(),
            created_on: self.created_on.to_pb()?,
            updated_on: self.updated_on.to_pb_opt()?,
            comment: self.comment.clone(),
            schema: Some(self.schema.to_pb()?),
            field_comments: self.field_comments.clone(),
        };
        Ok(p)
    }
}
