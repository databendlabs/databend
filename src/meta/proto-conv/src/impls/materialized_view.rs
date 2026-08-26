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
use databend_common_meta_app::schema::MVSourceBinding;
use databend_common_meta_app::schema::MVSourceBindingVersion;
use databend_common_protos::pb::MvDefinition as PbMVDefinition;
use databend_common_protos::pb::MvSourceBinding as PbMVSourceBinding;
use databend_common_protos::pb::MvSourceBindingVersion as PbMVSourceBindingVersion;

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
        let logical_schema = p.logical_schema.ok_or_else(|| {
            Incompatible::new("MVDefinition.logical_schema can not be None".to_string())
        })?;

        Ok(Self {
            original_query: p.original_query,
            query: p.query,
            logical_schema: TableSchema::from_pb(logical_schema)?,
            sync_creation: p.sync_creation,
        })
    }

    fn to_pb(&self) -> Self::PB {
        PbMVDefinition {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            original_query: self.original_query.clone(),
            query: self.query.clone(),
            logical_schema: Some(self.logical_schema.to_pb()),
            sync_creation: self.sync_creation,
        }
    }
}

impl FromToProto for MVSourceBinding {
    type PB = PbMVSourceBinding;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(Self {
            bound_source_generation: p.bound_source_generation,
        })
    }

    fn to_pb(&self) -> Self::PB {
        PbMVSourceBinding {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            bound_source_generation: self.bound_source_generation,
        }
    }
}

impl FromToProto for MVSourceBindingVersion {
    type PB = PbMVSourceBindingVersion;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(Self {
            current_source_generation: p.current_source_generation,
        })
    }

    fn to_pb(&self) -> Self::PB {
        PbMVSourceBindingVersion {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            current_source_generation: self.current_source_generation,
        }
    }
}
