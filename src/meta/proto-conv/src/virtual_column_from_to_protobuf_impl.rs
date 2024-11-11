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
use databend_common_expression::TableDataType;
use databend_common_meta_app::schema as mt;
use databend_common_protos::pb;

use crate::reader_check_msg;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;

impl FromToProto for mt::VirtualColumnMeta {
    type PB = pb::VirtualColumnMeta;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        let virtual_columns = if p.data_types.is_empty() {
            p.virtual_columns
                .iter()
                .map(|v| {
                    (
                        v.clone(),
                        TableDataType::Nullable(Box::new(TableDataType::Variant)),
                    )
                })
                .collect()
        } else {
            let mut virtual_columns = Vec::new();
            for (v, ty) in p.virtual_columns.iter().zip(p.data_types.iter()) {
                virtual_columns.push((v.clone(), TableDataType::from_pb(ty.clone())?));
            }
            virtual_columns
        };

        let v = Self {
            table_id: p.table_id,
            virtual_columns,
            created_on: DateTime::<Utc>::from_pb(p.created_on)?,
            updated_on: match p.updated_on {
                Some(updated_on) => Some(DateTime::<Utc>::from_pb(updated_on)?),
                None => None,
            },
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        let mut data_types = Vec::new();
        let mut virtual_columns = Vec::new();
        for (v, ty) in self.virtual_columns.iter() {
            data_types.push(ty.to_pb()?);
            virtual_columns.push(v.clone());
        }
        let p = pb::VirtualColumnMeta {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            table_id: self.table_id,
            virtual_columns,
            created_on: self.created_on.to_pb()?,
            updated_on: match self.updated_on {
                Some(updated_on) => Some(updated_on.to_pb()?),
                None => None,
            },
            data_types,
        };
        Ok(p)
    }
}
