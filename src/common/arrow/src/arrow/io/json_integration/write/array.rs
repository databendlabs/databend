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

use crate::arrow::{
    array::{Array, PrimitiveArray},
    chunk::Chunk,
    datatypes::DataType,
};

use super::super::{ArrowJsonBatch, ArrowJsonColumn};

/// Serializes a [`Chunk`] to [`ArrowJsonBatch`].
pub fn serialize_chunk<A: ToString>(
    columns: &Chunk<Box<dyn Array>>,
    names: &[A],
) -> ArrowJsonBatch {
    let count = columns.len();

    let columns = columns
        .arrays()
        .iter()
        .zip(names.iter())
        .map(|(array, name)| match array.data_type() {
            DataType::Int8 => {
                let array = array.as_any().downcast_ref::<PrimitiveArray<i8>>().unwrap();

                let (validity, data) = array
                    .iter()
                    .map(|x| (x.is_some() as u8, x.copied().unwrap_or_default().into()))
                    .unzip();

                ArrowJsonColumn {
                    name: name.to_string(),
                    count: array.len(),
                    validity: Some(validity),
                    data: Some(data),
                    offset: None,
                    type_id: None,
                    children: None,
                }
            }
            _ => ArrowJsonColumn {
                name: name.to_string(),
                count: array.len(),
                validity: None,
                data: None,
                offset: None,
                type_id: None,
                children: None,
            },
        })
        .collect();

    ArrowJsonBatch { count, columns }
}
