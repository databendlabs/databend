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
use crate::arrow::array::MapArray;
use crate::arrow::buffer::Buffer;
use crate::arrow::datatypes::DataType;
use crate::arrow::error::Error;
use crate::arrow::error::Result;

#[allow(clippy::too_many_arguments)]
pub fn read_map<R: Read + Seek>(
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
) -> Result<MapArray> {
    let field_node = field_nodes.pop_front().ok_or_else(|| {
        Error::oos(format!(
            "IPC: unable to fetch the field for {data_type:?}. The file or stream is corrupted."
        ))
    })?;

    let validity = read_validity(
        buffers,
        field_node,
        reader,
        block_offset,
        is_little_endian,
        compression,
        limit,
        scratch,
    )?;

    let length: usize = field_node
        .length()
        .try_into()
        .map_err(|_| Error::from(OutOfSpecKind::NegativeFooterLength))?;
    let length = limit.map(|limit| limit.min(length)).unwrap_or(length);

    let offsets = read_buffer::<i32, _>(
        buffers,
        1 + length,
        reader,
        block_offset,
        is_little_endian,
        compression,
        scratch,
    )
    // Older versions of the IPC format sometimes do not report an offset
    .or_else(|_| Result::Ok(Buffer::<i32>::from(vec![0i32])))?;

    let field = MapArray::get_field(&data_type);

    let last_offset: usize = offsets.last().copied().unwrap() as usize;

    let field = read(
        field_nodes,
        variadic_buffer_counts,
        field,
        &ipc_field.fields[0],
        buffers,
        reader,
        dictionaries,
        block_offset,
        is_little_endian,
        compression,
        Some(last_offset),
        version,
        scratch,
    )?;
    MapArray::try_new(data_type, offsets.try_into()?, field, validity)
}

pub fn skip_map(
    field_nodes: &mut VecDeque<Node>,
    data_type: &DataType,
    buffers: &mut VecDeque<IpcBuffer>,
) -> Result<()> {
    let _ = field_nodes.pop_front().ok_or_else(|| {
        Error::oos("IPC: unable to fetch the field for map. The file or stream is corrupted.")
    })?;

    let _ = buffers
        .pop_front()
        .ok_or_else(|| Error::oos("IPC: missing validity buffer."))?;
    let _ = buffers
        .pop_front()
        .ok_or_else(|| Error::oos("IPC: missing offsets buffer."))?;

    let data_type = MapArray::get_field(data_type).data_type();

    skip(field_nodes, data_type, buffers)
}
