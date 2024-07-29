// Copyright 2020-2022 Jorge C. Leitão
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

//! APIs to write to Arrow's IPC format.
pub(crate) mod common;
mod schema;
mod serialize;
mod stream;
pub(crate) mod writer;

pub use common::Compression;
pub use common::Record;
pub use common::WriteOptions;
pub use schema::schema_to_bytes;
pub use serialize::write;
use serialize::write_dictionary;
pub use stream::StreamWriter;
pub use writer::FileWriter;

pub(crate) mod common_sync;

use super::IpcField;
use crate::arrow::datatypes::DataType;
use crate::arrow::datatypes::Field;

fn default_ipc_field(data_type: &DataType, current_id: &mut i64) -> IpcField {
    use crate::arrow::datatypes::DataType::*;
    match data_type.to_logical_type() {
        // single child => recurse
        Map(inner, ..) | FixedSizeList(inner, _) | LargeList(inner) | List(inner) => IpcField {
            fields: vec![default_ipc_field(inner.data_type(), current_id)],
            dictionary_id: None,
        },
        // multiple children => recurse
        Union(fields, ..) | Struct(fields) => IpcField {
            fields: fields
                .iter()
                .map(|f| default_ipc_field(f.data_type(), current_id))
                .collect(),
            dictionary_id: None,
        },
        // dictionary => current_id
        Dictionary(_, data_type, _) => {
            let dictionary_id = Some(*current_id);
            *current_id += 1;
            IpcField {
                fields: vec![default_ipc_field(data_type, current_id)],
                dictionary_id,
            }
        }
        // no children => do nothing
        _ => IpcField {
            fields: vec![],
            dictionary_id: None,
        },
    }
}

/// Assigns every dictionary field a unique ID
pub fn default_ipc_fields(fields: &[Field]) -> Vec<IpcField> {
    let mut dictionary_id = 0i64;
    fields
        .iter()
        .map(|field| default_ipc_field(field.data_type().to_logical_type(), &mut dictionary_id))
        .collect()
}
