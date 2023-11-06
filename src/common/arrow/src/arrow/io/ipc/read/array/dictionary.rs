use std::collections::HashSet;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::io::Read;
use std::io::Seek;

use super::super::Compression;
use super::super::Dictionaries;
use super::super::IpcBuffer;
use super::super::Node;
use super::read_primitive;
use super::skip_primitive;
use crate::arrow::array::DictionaryArray;
use crate::arrow::array::DictionaryKey;
use crate::arrow::datatypes::DataType;
use crate::arrow::error::Error;
use crate::arrow::error::Result;

#[allow(clippy::too_many_arguments)]
pub fn read_dictionary<T: DictionaryKey, R: Read + Seek>(
    field_nodes: &mut VecDeque<Node>,
    data_type: DataType,
    id: Option<i64>,
    buffers: &mut VecDeque<IpcBuffer>,
    reader: &mut R,
    dictionaries: &Dictionaries,
    block_offset: u64,
    compression: Option<Compression>,
    limit: Option<usize>,
    is_little_endian: bool,
    scratch: &mut Vec<u8>,
) -> Result<DictionaryArray<T>>
where
    Vec<u8>: TryInto<T::Bytes>,
{
    let id = if let Some(id) = id {
        id
    } else {
        return Err(Error::OutOfSpec("Dictionary has no id.".to_string()));
    };
    let values = dictionaries
        .get(&id)
        .ok_or_else(|| {
            let valid_ids = dictionaries.keys().collect::<HashSet<_>>();
            Error::OutOfSpec(format!(
                "Dictionary id {id} not found. Valid ids: {valid_ids:?}"
            ))
        })?
        .clone();

    let keys = read_primitive(
        field_nodes,
        T::PRIMITIVE.into(),
        buffers,
        reader,
        block_offset,
        is_little_endian,
        compression,
        limit,
        scratch,
    )?;

    DictionaryArray::<T>::try_new(data_type, keys, values)
}

pub fn skip_dictionary(
    field_nodes: &mut VecDeque<Node>,
    buffers: &mut VecDeque<IpcBuffer>,
) -> Result<()> {
    skip_primitive(field_nodes, buffers)
}
