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

use databend_common_expression::TableSchema;
use databend_common_meta_app::schema::MVDefinition;
use databend_common_meta_app::schema::SourceTableMVIds;
use databend_common_protos::pb::MvDefinition as PbMVDefinition;
use databend_common_protos::pb::SourceTableMvIds as PbSourceTableMVIds;

use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;
use crate::reader_check_msg;

impl FromToProto for MVDefinition {
    type PB = PbMVDefinition;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        let schema = p
            .schema
            .ok_or_else(|| Incompatible::new("MVDefinition.schema can not be None".to_string()))?;

        Ok(Self {
            original_query: p.original_query,
            query: p.query,
            schema: TableSchema::from_pb(schema)?,
        })
    }

    fn to_pb(&self) -> Self::PB {
        PbMVDefinition {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            original_query: self.original_query.clone(),
            query: self.query.clone(),
            schema: Some(self.schema.to_pb()),
        }
    }
}

impl FromToProto for SourceTableMVIds {
    type PB = PbSourceTableMVIds;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        Ok(Self::from_ids(p.mv_ids))
    }

    fn to_pb(&self) -> Self::PB {
        PbSourceTableMVIds {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            mv_ids: self.mv_ids().to_vec(),
        }
    }
}
