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
        if !p.data_types.is_empty() && p.virtual_columns.len() != p.data_types.len() {
            return Err(Incompatible::new(format!(
                "Incompatible virtual columns length is {}, but data types length is {}",
                p.virtual_columns.len(),
                p.data_types.len()
            )));
        }
        if !p.alias_names.is_empty() && p.virtual_columns.len() != p.alias_names.len() {
            return Err(Incompatible::new(format!(
                "Incompatible virtual columns length is {}, but alias names length is {}",
                p.virtual_columns.len(),
                p.alias_names.len()
            )));
        }
        let mut virtual_columns = Vec::with_capacity(p.virtual_columns.len());
        for (i, expr) in p.virtual_columns.iter().enumerate() {
            let data_type = if let Some(ty) = p.data_types.get(i) {
                TableDataType::from_pb(ty.clone())?
            } else {
                TableDataType::Nullable(Box::new(TableDataType::Variant))
            };
            let alias_name = if let Some(alias_name) = p.alias_names.get(i) {
                if !alias_name.is_empty() {
                    Some(alias_name.clone())
                } else {
                    None
                }
            } else {
                None
            };
            let virtual_column = mt::VirtualField {
                expr: expr.clone(),
                data_type,
                alias_name,
            };
            virtual_columns.push(virtual_column);
        }

        let v = Self {
            table_id: p.table_id,
            virtual_columns,
            created_on: DateTime::<Utc>::from_pb(p.created_on)?,
            updated_on: match p.updated_on {
                Some(updated_on) => Some(DateTime::<Utc>::from_pb(updated_on)?),
                None => None,
            },
            auto_generated: p.auto_generated.unwrap_or_default(),
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        let mut data_types = Vec::with_capacity(self.virtual_columns.len());
        let mut virtual_columns = Vec::with_capacity(self.virtual_columns.len());
        let mut alias_names = Vec::with_capacity(self.virtual_columns.len());
        for virtual_field in self.virtual_columns.iter() {
            data_types.push(virtual_field.data_type.to_pb()?);
            virtual_columns.push(virtual_field.expr.clone());
            alias_names.push(virtual_field.alias_name.clone().unwrap_or_default());
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
            alias_names,
            auto_generated: Some(self.auto_generated),
        };
        Ok(p)
    }
}
