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

use arrow_format::ipc::BodyCompressionRef;
use arrow_format::ipc::MetadataVersion;

use super::array::*;
use super::Dictionaries;
use super::IpcBuffer;
use super::Node;
use crate::arrow::array::*;
use crate::arrow::datatypes::DataType;
use crate::arrow::datatypes::Field;
use crate::arrow::datatypes::PhysicalType;
use crate::arrow::error::Result;
use crate::arrow::io::ipc::IpcField;

#[allow(clippy::too_many_arguments)]
pub fn read<R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    field: &Field,
    ipc_field: &IpcField,
    buffers: &mut VecDeque<IpcBuffer>,
    reader: &mut R,
    dictionaries: &Dictionaries,
    block_offset: u64,
    is_little_endian: bool,
    compression: Option<BodyCompressionRef>,
    limit: Option<usize>,
    version: MetadataVersion,
    scratch: &mut Vec<u8>,
) -> Result<Box<dyn Array>> {
    use PhysicalType::*;
    let data_type = field.data_type.clone();

    match data_type.to_physical_type() {
        Null => read_null(field_nodes, data_type).map(|x| x.boxed()),
        Boolean => read_boolean(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
            limit,
            scratch,
        )
        .map(|x| x.boxed()),
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            read_primitive::<$T, _>(
                field_nodes,
                data_type,
                buffers,
                reader,
                block_offset,
                is_little_endian,
                compression,
                limit,
                scratch,
            )
            .map(|x| x.boxed())
        }),
        Binary => read_binary::<i32, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
            limit,
            scratch,
        )
        .map(|x| x.boxed()),
        LargeBinary => read_binary::<i64, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
            limit,
            scratch,
        )
        .map(|x| x.boxed()),
        FixedSizeBinary => read_fixed_size_binary(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
            limit,
            scratch,
        )
        .map(|x| x.boxed()),
        Utf8 => read_utf8::<i32, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
            limit,
            scratch,
        )
        .map(|x| x.boxed()),
        LargeUtf8 => read_utf8::<i64, _>(
            field_nodes,
            data_type,
            buffers,
            reader,
            block_offset,
            is_little_endian,
            compression,
            limit,
            scratch,
        )
        .map(|x| x.boxed()),
        List => read_list::<i32, _>(
            field_nodes,
            data_type,
            ipc_field,
            buffers,
            reader,
            dictionaries,
            block_offset,
            is_little_endian,
            compression,
            limit,
            version,
            scratch,
        )
        .map(|x| x.boxed()),
        LargeList => read_list::<i64, _>(
            field_nodes,
            data_type,
            ipc_field,
            buffers,
            reader,
            dictionaries,
            block_offset,
            is_little_endian,
            compression,
            limit,
            version,
            scratch,
        )
        .map(|x| x.boxed()),
        FixedSizeList => read_fixed_size_list(
            field_nodes,
            data_type,
            ipc_field,
            buffers,
            reader,
            dictionaries,
            block_offset,
            is_little_endian,
            compression,
            limit,
            version,
            scratch,
        )
        .map(|x| x.boxed()),
        Struct => read_struct(
            field_nodes,
            data_type,
            ipc_field,
            buffers,
            reader,
            dictionaries,
            block_offset,
            is_little_endian,
            compression,
            limit,
            version,
            scratch,
        )
        .map(|x| x.boxed()),
        Dictionary(key_type) => {
            match_integer_type!(key_type, |$T| {
                read_dictionary::<$T, _>(
                    field_nodes,
                    data_type,
                    ipc_field.dictionary_id,
                    buffers,
                    reader,
                    dictionaries,
                    block_offset,
                    compression,
                    limit,
                    is_little_endian,
                    scratch,
                )
                .map(|x| x.boxed())
            })
        }
        Union => read_union(
            field_nodes,
            data_type,
            ipc_field,
            buffers,
            reader,
            dictionaries,
            block_offset,
            is_little_endian,
            compression,
            limit,
            version,
            scratch,
        )
        .map(|x| x.boxed()),
        Map => read_map(
            field_nodes,
            data_type,
            ipc_field,
            buffers,
            reader,
            dictionaries,
            block_offset,
            is_little_endian,
            compression,
            limit,
            version,
            scratch,
        )
        .map(|x| x.boxed()),
    }
}

pub fn skip(
    field_nodes: &mut VecDeque<Node>,
    data_type: &DataType,
    buffers: &mut VecDeque<IpcBuffer>,
) -> Result<()> {
    use PhysicalType::*;
    match data_type.to_physical_type() {
        Null => skip_null(field_nodes),
        Boolean => skip_boolean(field_nodes, buffers),
        Primitive(_) => skip_primitive(field_nodes, buffers),
        LargeBinary | Binary => skip_binary(field_nodes, buffers),
        LargeUtf8 | Utf8 => skip_utf8(field_nodes, buffers),
        FixedSizeBinary => skip_fixed_size_binary(field_nodes, buffers),
        List => skip_list::<i32>(field_nodes, data_type, buffers),
        LargeList => skip_list::<i64>(field_nodes, data_type, buffers),
        FixedSizeList => skip_fixed_size_list(field_nodes, data_type, buffers),
        Struct => skip_struct(field_nodes, data_type, buffers),
        Dictionary(_) => skip_dictionary(field_nodes, buffers),
        Union => skip_union(field_nodes, data_type, buffers),
        Map => skip_map(field_nodes, data_type, buffers),
    }
}
