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

use bytes::Buf;
use futures::AsyncReadExt;

use super::super::metadata::FileMetaData;
use super::super::DEFAULT_FOOTER_READ_SIZE;
use super::super::FOOTER_SIZE;
use super::super::PARQUET_MAGIC;
use super::metadata::deserialize_metadata;
use crate::error::Error;
use crate::error::Result;

/// Decodes the footer returning the metadata length in bytes
pub fn decode_footer(slice: &[u8]) -> Result<usize> {
    assert_eq!(slice.len(), FOOTER_SIZE as usize, "Invalid footer size");

    // check this is indeed a parquet file
    if slice[4..] != PARQUET_MAGIC {
        return Err(Error::oos("Invalid Parquet file. Corrupt footer"));
    }

    // get the metadata length from the footer
    let metadata_len = u32::from_le_bytes(slice[..4].try_into().unwrap());
    // u32 won't be larger than usize in most cases
    Ok(metadata_len as usize)
}

/// Asynchronously reads the files' metadata
///
/// This implementation is based on the implementation in `parquet`: https://docs.rs/parquet/latest/src/parquet/file/footer.rs.html#69
pub async fn read_metadata(reader: opendal::Reader, file_size: u64) -> Result<FileMetaData> {
    if file_size < 8 {
        return Err(Error::oos(format!(
            "file size of {file_size} is less than footer"
        )));
    }

    // If a size hint is provided, read more than the minimum size
    // to try and avoid a second fetch.
    let footer_start = file_size.saturating_sub(DEFAULT_FOOTER_READ_SIZE);

    let suffix = reader
        .read(footer_start..file_size)
        .await
        .map_err(|err| Error::oos(err.to_string()))?;
    let suffix_len = suffix.len();

    let footer = suffix.slice(suffix_len - 8..suffix_len).to_vec();
    let length = decode_footer(&footer)?;

    if file_size < length as u64 + 8 {
        return Err(Error::oos(format!(
            "file size of {} is less than footer + metadata {}",
            file_size,
            length + 8
        )));
    }

    // Did not fetch the entire file metadata in the initial read, need to make a second request
    if length > suffix_len - 8 {
        let metadata_start = file_size as usize - length - 8;
        let meta = reader
            .read(metadata_start as _..file_size - 8)
            .await
            .map_err(|err| Error::oos(err.to_string()))?;

        // a highly nested but sparse struct could result in many allocations
        let max_size = meta.len() * 2 + 1024;
        deserialize_metadata(meta.reader(), max_size)
    } else {
        let metadata_start = file_size as usize - length - 8 - footer_start as usize;

        let slice = suffix.slice(metadata_start as _..suffix_len - 8);

        // a highly nested but sparse struct could result in many allocations
        let max_size = slice.len() * 2 + 1024;
        deserialize_metadata(slice.reader(), max_size)
    }
}
