// Copyright 2023 Datafuse Labs.
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
//

use std::io::Cursor;
use std::sync::Arc;

use common_exception::Result;
use common_io::prelude::BinaryRead;
use futures::AsyncRead;
use futures_util::AsyncReadExt;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::Encoding;
use storages_common_table_meta::meta::MetaCompression;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::Statistics;
use storages_common_table_meta::meta::Versioned;

use crate::io::read::meta::meta_readers::read_and_deserialize;

/// Reads a segment from Vec<u8> and returns a `SegmentInfo` object.
///
/// This function reads the buffer from the stream and constructs a `SegmentInfo` object:
///
/// * `version` (u64): The version number of the segment.
/// * `encoding` (u8): The encoding format used to serialize the segment's data.
/// * `compression` (u8): The compression format used to compress the segment's data.
/// * `blocks_size` (u64): The size (in bytes) of the compressed block metadata.
/// * `summary_size` (u64): The size (in bytes) of the compressed segment summary.
///
/// The function then reads the compressed block metadata and segment summary from the stream,
/// decompresses them using the specified compression format, and deserializes them using the specified
/// encoding format. Finally, it constructs a `SegmentInfo` object using the deserialized block metadata
/// and segment summary, and returns it.
pub async fn load_segment_v3<R>(mut reader: R) -> Result<SegmentInfo>
where R: AsyncRead + Unpin + Send {
    let mut buffer: Vec<u8> = vec![];
    reader.read_to_end(&mut buffer).await?;

    let mut cursor = Cursor::new(buffer);
    let version = cursor.read_scalar::<u64>()?;
    assert_eq!(version, SegmentInfo::VERSION);
    let encoding = Encoding::try_from(cursor.read_scalar::<u8>()?)?;
    let compression = MetaCompression::try_from(cursor.read_scalar::<u8>()?)?;
    let blocks_size: u64 = cursor.read_scalar::<u64>()?;
    let summary_size: u64 = cursor.read_scalar::<u64>()?;

    let blocks: Vec<Arc<BlockMeta>> =
        read_and_deserialize(&mut cursor, blocks_size, &encoding, &compression)?;
    let summary: Statistics =
        read_and_deserialize(&mut cursor, summary_size, &encoding, &compression)?;

    Ok(SegmentInfo::new(blocks, summary))
}
