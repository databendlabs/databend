// Copyright [2021] [Jorge C Leitao]
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

use std::cmp::min;
use std::convert::TryInto;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;

use parquet_format_safe::thrift::protocol::TCompactInputProtocol;
use parquet_format_safe::FileMetaData as TFileMetaData;

use super::super::metadata::FileMetaData;
use super::super::DEFAULT_FOOTER_READ_SIZE;
use super::super::FOOTER_SIZE;
use super::super::HEADER_SIZE;
use super::super::PARQUET_MAGIC;
use crate::error::Error;
use crate::error::Result;

pub(super) fn metadata_len(buffer: &[u8], len: usize) -> i32 {
    i32::from_le_bytes(buffer[len - 8..len - 4].try_into().unwrap())
}

// see (unstable) Seek::stream_len
fn stream_len(seek: &mut impl Seek) -> std::result::Result<u64, std::io::Error> {
    let old_pos = seek.seek(SeekFrom::Current(0))?;
    let len = seek.seek(SeekFrom::End(0))?;

    // Avoid seeking a third time when we were already at the end of the
    // stream. The branch is usually way cheaper than a seek operation.
    if old_pos != len {
        seek.seek(SeekFrom::Start(old_pos))?;
    }

    Ok(len)
}

/// Reads a [`FileMetaData`] from the reader, located at the end of the file.
pub fn read_metadata<R: Read + Seek>(reader: &mut R) -> Result<FileMetaData> {
    // check file is large enough to hold footer
    let file_size = stream_len(reader)?;
    read_metadata_with_size(reader, file_size)
}

/// Reads a [`FileMetaData`] from the reader, located at the end of the file, with known file size.
pub fn read_metadata_with_size<R: Read + Seek>(
    reader: &mut R,
    file_size: u64,
) -> Result<FileMetaData> {
    if file_size < HEADER_SIZE + FOOTER_SIZE {
        return Err(Error::oos(
            "A parquet file must containt a header and footer with at least 12 bytes",
        ));
    }

    // read and cache up to DEFAULT_FOOTER_READ_SIZE bytes from the end and process the footer
    let default_end_len = min(DEFAULT_FOOTER_READ_SIZE, file_size) as usize;
    reader.seek(SeekFrom::End(-(default_end_len as i64)))?;

    let mut buffer = Vec::with_capacity(default_end_len);
    reader
        .by_ref()
        .take(default_end_len as u64)
        .read_to_end(&mut buffer)?;

    // check this is indeed a parquet file
    if buffer[default_end_len - 4..] != PARQUET_MAGIC {
        return Err(Error::oos("The file must end with PAR1"));
    }

    let metadata_len = metadata_len(&buffer, default_end_len);

    let metadata_len: u64 = metadata_len.try_into()?;

    let footer_len = FOOTER_SIZE + metadata_len;
    if footer_len > file_size {
        return Err(Error::oos(
            "The footer size must be smaller or equal to the file's size",
        ));
    }

    let reader: &[u8] = if (footer_len as usize) < buffer.len() {
        // the whole metadata is in the bytes we already read
        let remaining = buffer.len() - footer_len as usize;
        &buffer[remaining..]
    } else {
        // the end of file read by default is not long enough, read again including the metadata.
        reader.seek(SeekFrom::End(-(footer_len as i64)))?;

        buffer.clear();
        buffer.try_reserve(footer_len as usize)?;
        reader.take(footer_len).read_to_end(&mut buffer)?;

        &buffer
    };

    // a highly nested but sparse struct could result in many allocations
    let max_size = reader.len() * 2 + 1024;

    deserialize_metadata(reader, max_size)
}

/// Parse loaded metadata bytes
pub fn deserialize_metadata<R: Read>(reader: R, max_size: usize) -> Result<FileMetaData> {
    let mut prot = TCompactInputProtocol::new(reader, max_size);
    let metadata = TFileMetaData::read_from_in_protocol(&mut prot)?;

    FileMetaData::try_from_thrift(metadata)
}
