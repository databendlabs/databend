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

use std::collections::VecDeque;
use std::io::Read;
use std::io::Seek;

use super::super::super::IpcField;
use super::super::deserialize::read;
use super::super::deserialize::skip;
use super::super::read_basic::*;
use super::super::Compression;
use super::super::Dictionaries;
use super::super::IpcBuffer;
use super::super::Node;
use super::super::OutOfSpecKind;
use super::super::Version;
use crate::arrow::array::UnionArray;
use crate::arrow::datatypes::DataType;
use crate::arrow::datatypes::UnionMode::Dense;
use crate::arrow::error::Error;
use crate::arrow::error::Result;

#[allow(clippy::too_many_arguments)]
pub fn read_union<R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    variadic_buffer_counts: &mut VecDeque<usize>,
    data_type: DataType,
    ipc_field: &IpcField,
    buffers: &mut VecDeque<IpcBuffer>,
    reader: &mut R,
    dictionaries: &Dictionaries,
    block_offset: u64,
    is_little_endian: bool,
    compression: Option<Compression>,
    limit: Option<usize>,
    version: Version,
    scratch: &mut Vec<u8>,
) -> Result<UnionArray> {
    let field_node = field_nodes.pop_front().ok_or_else(|| {
        Error::oos(format!(
            "IPC: unable to fetch the field for {data_type:?}. The file or stream is corrupted."
        ))
    })?;

    if version != Version::V5 {
        let _ = buffers
            .pop_front()
            .ok_or_else(|| Error::oos("IPC: missing validity buffer."))?;
    };

    let length: usize = field_node
        .length()
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;
    let length = limit.map(|limit| limit.min(length)).unwrap_or(length);

    let types = read_buffer(
        buffers,
        length,
        reader,
        block_offset,
        is_little_endian,
        compression,
        scratch,
    )?;

    let offsets = if let DataType::Union(_, _, mode) = data_type {
        if !mode.is_sparse() {
            Some(read_buffer(
                buffers,
                length,
                reader,
                block_offset,
                is_little_endian,
                compression,
                scratch,
            )?)
        } else {
            None
        }
    } else {
        unreachable!()
    };

    let fields = UnionArray::get_fields(&data_type);

    let fields = fields
        .iter()
        .zip(ipc_field.fields.iter())
        .map(|(field, ipc_field)| {
            read(
                field_nodes,
                variadic_buffer_counts,
                field,
                ipc_field,
                buffers,
                reader,
                dictionaries,
                block_offset,
                is_little_endian,
                compression,
                None,
                version,
                scratch,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    UnionArray::try_new(data_type, types, fields, offsets)
}

pub fn skip_union(
    field_nodes: &mut VecDeque<Node>,
    data_type: &DataType,
    buffers: &mut VecDeque<IpcBuffer>,
) -> Result<()> {
    let _ = field_nodes.pop_front().ok_or_else(|| {
        Error::oos("IPC: unable to fetch the field for struct. The file or stream is corrupted.")
    })?;

    let _ = buffers
        .pop_front()
        .ok_or_else(|| Error::oos("IPC: missing validity buffer."))?;
    if let DataType::Union(_, _, Dense) = data_type {
        let _ = buffers
            .pop_front()
            .ok_or_else(|| Error::oos("IPC: missing offsets buffer."))?;
    } else {
        unreachable!()
    };

    let fields = UnionArray::get_fields(data_type);

    fields
        .iter()
        .try_for_each(|field| skip(field_nodes, field.data_type(), buffers))
}
