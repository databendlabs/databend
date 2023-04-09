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

use std::marker::PhantomData;
use std::sync::Arc;

use common_exception::Result;
use common_io::prelude::BinaryRead;
use futures::AsyncRead;
use serde::de::DeserializeOwned;
use storages_common_table_meta::meta::decode;
use storages_common_table_meta::meta::decompress;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::Encoding;
use storages_common_table_meta::meta::SegmentCompression;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::Statistics;
use storages_common_table_meta::meta::Versioned;

async fn read_u64_exact<R>(reader: &mut R) -> Result<u64>
where R: AsyncRead + Unpin + Send {
    let mut buffer = [0; 8];
    use futures::AsyncReadExt;
    reader.read_exact(&mut buffer).await?;
    Ok(u64::from_le_bytes(buffer))
}

async fn read_and_deserialize<R, T>(
    reader: &mut R,
    size: u64,
    encoding: &Encoding,
    compression: &SegmentCompression,
) -> Result<T>
where
    R: AsyncRead + Unpin + Send,
    T: DeserializeOwned,
{
    let mut compressed_data = vec![0; size as usize];
    use futures::AsyncReadExt;
    reader.read_exact(&mut compressed_data).await?;

    let decompressed_data = decompress(compression, compressed_data)?;

    Ok(decode(encoding, &decompressed_data)?)
}

pub async fn load_segment_v3<R, T>(mut reader: R, _v: &PhantomData<T>) -> Result<SegmentInfo>
where
    T: DeserializeOwned,
    R: AsyncRead + Unpin + Send,
{
    let version: u64 = reader.read_scalar()?;
    assert_eq!(version, SegmentInfo::VERSION);
    let encoding = Encoding::try_from(reader.read_scalar::<u64>()?)?;
    let compression = SegmentCompression::try_from(reader.read_scalar::<u64>()?)?;
    let blocks_size: u64 = reader.read_scalar()?;
    let summary_size: u64 = reader.read_scalar()?;

    let blocks: Vec<Arc<BlockMeta>> =
        read_and_deserialize(&mut reader, blocks_size, &encoding, &compression).await?;
    let summary: Statistics =
        read_and_deserialize(&mut reader, summary_size, &encoding, &compression).await?;

    Ok(SegmentInfo::new(blocks, summary))
}
