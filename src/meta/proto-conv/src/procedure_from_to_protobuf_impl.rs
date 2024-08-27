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
use databend_common_expression::infer_schema_type;
use databend_common_expression::types::DataType;
use databend_common_expression::TableDataType;
use databend_common_meta_app as mt;
use databend_common_protos::pb;

use crate::reader_check_msg;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;

impl FromToProto for mt::principal::ProcedureMeta {
    type PB = pb::ProcedureMeta;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::ProcedureMeta) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        reader_check_msg(p.ver, p.min_reader_ver)?;

        let mut return_types = Vec::with_capacity(p.return_types.len());
        for arg_type in p.return_types {
            let arg_type = DataType::from(&TableDataType::from_pb(arg_type)?);
            return_types.push(arg_type);
        }

        let v = Self {
            return_types,
            created_on: DateTime::<Utc>::from_pb(p.created_on)?,
            updated_on: DateTime::<Utc>::from_pb(p.updated_on)?,
            script: p.script,
            comment: p.comment,
            procedure_language: p.language,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::ProcedureMeta, Incompatible> {
        let mut return_types = Vec::with_capacity(self.return_types.len());
        for arg_type in self.return_types.iter() {
            let arg_type = infer_schema_type(arg_type)
                .map_err(|e| Incompatible {
                    reason: format!("Convert DataType to TableDataType failed: {}", e.message()),
                })?
                .to_pb()?;
            return_types.push(arg_type);
        }

        let p = pb::ProcedureMeta {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            return_types,
            created_on: self.created_on.to_pb()?,
            updated_on: self.updated_on.to_pb()?,
            script: self.script.to_string(),
            comment: self.comment.to_string(),
            language: self.procedure_language.to_string(),
        };
        Ok(p)
    }
}

impl FromToProto for mt::principal::ProcedureIdList {
    type PB = pb::ProcedureIdList;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::ProcedureIdList) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let v = Self { id_list: p.ids };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::ProcedureIdList, Incompatible> {
        let p = pb::ProcedureIdList {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            ids: self.id_list.clone(),
        };
        Ok(p)
    }
}
