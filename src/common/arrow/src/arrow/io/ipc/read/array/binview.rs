// Copyright (c) 2020 Ritchie Vink
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
use std::sync::Arc;

use crate::arrow::array::BinaryViewArrayGeneric;
use crate::arrow::array::View;
use crate::arrow::array::ViewType;
use crate::arrow::buffer::Buffer;
use crate::arrow::datatypes::DataType;
use crate::arrow::error::Error;
use crate::arrow::error::Result;
use crate::arrow::io::ipc::read::read_basic::read_buffer;
use crate::arrow::io::ipc::read::read_basic::read_bytes;
use crate::arrow::io::ipc::read::read_basic::read_validity;
use crate::arrow::io::ipc::read::Compression;
use crate::arrow::io::ipc::read::IpcBuffer;
use crate::arrow::io::ipc::read::Node;
use crate::arrow::io::ipc::read::OutOfSpecKind;
use crate::ArrayRef;

#[allow(clippy::too_many_arguments)]
pub fn read_binview<T: ViewType + ?Sized, R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    variadic_buffer_counts: &mut VecDeque<usize>,
    data_type: DataType,
    buffers: &mut VecDeque<IpcBuffer>,
    reader: &mut R,
    block_offset: u64,
    is_little_endian: bool,
    compression: Option<Compression>,
    limit: Option<usize>,
    scratch: &mut Vec<u8>,
) -> Result<ArrayRef> {
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

    let views: Buffer<View> = read_buffer(
        buffers,
        length,
        reader,
        block_offset,
        is_little_endian,
        compression,
        scratch,
    )?;

    let n_variadic = variadic_buffer_counts.pop_front().ok_or_else(|| {
        Error::oos("IPC: unable to fetch the variadic buffers\n\nThe file or stream is corrupted.")
    })?;

    let variadic_buffers = (0..n_variadic)
        .map(|_| {
            read_bytes(
                buffers,
                reader,
                block_offset,
                is_little_endian,
                compression,
                scratch,
            )
        })
        .collect::<Result<Vec<Buffer<u8>>>>()?;

    BinaryViewArrayGeneric::<T>::try_new(data_type, views, Arc::from(variadic_buffers), validity)
        .map(|arr| arr.boxed())
}
