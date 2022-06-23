//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use common_arrow::arrow::chunk::Chunk;
use common_arrow::arrow::datatypes::DataType as ArrowDataType;
use common_arrow::arrow::io::parquet::write::RowGroupIterator;
use common_arrow::arrow::io::parquet::write::WriteOptions;
use common_arrow::parquet::compression::CompressionOptions;
use common_arrow::parquet::encoding::Encoding;
use common_arrow::parquet::write::Version;
use common_arrow::parquet::FileMetaData;
use common_arrow::write_parquet_file;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_tracing::tracing::warn;
use opendal::Operator;

use crate::storages::fuse::io::retry;
use crate::storages::fuse::io::retry::Retryable;
use crate::storages::fuse::io::TableMetaLocationGenerator;
use crate::storages::fuse::meta::BlockMeta;
use crate::storages::fuse::meta::Versioned;
use crate::storages::fuse::operations::util;
use crate::storages::fuse::statistics::accumulator;

pub struct BlockWriter<'a> {
    location_generator: &'a TableMetaLocationGenerator,
    data_accessor: &'a Operator,
}

impl<'a> BlockWriter<'a> {
    pub fn new(
        data_accessor: &'a Operator,
        location_generator: &'a TableMetaLocationGenerator,
    ) -> Self {
        Self {
            location_generator,
            data_accessor,
        }
    }
    pub async fn write_block(&self, block: DataBlock) -> Result<BlockMeta> {
        let location = self.location_generator.gen_block_location();
        let data_accessor = &self.data_accessor;
        let row_count = block.num_rows() as u64;
        let block_size = block.memory_size() as u64;
        let col_stats = accumulator::columns_statistics(&block)?;
        let (file_size, file_meta_data) = write_block(block, data_accessor, &location).await?;
        let col_metas = util::column_metas(&file_meta_data)?;
        let cluster_stats = None; // TODO confirm this with zhyass
        let location = (location, DataBlock::VERSION);
        let block_meta = BlockMeta::new(
            row_count,
            block_size,
            file_size,
            col_stats,
            col_metas,
            cluster_stats,
            location,
        );
        Ok(block_meta)
    }
}

pub async fn write_block(
    block: DataBlock,
    data_accessor: &Operator,
    location: &str,
) -> Result<(u64, FileMetaData)> {
    // we need a configuration of block size threshold here
    let mut buf = Vec::with_capacity(100 * 1024 * 1024);

    let schema = block.schema().clone();
    let result = serialize_data_blocks(vec![block], &schema, &mut buf)?;

    let bytes = buf.as_slice();
    let op = || async {
        data_accessor
            .object(location)
            .write(bytes)
            .await
            .map_err(retry::from_io_error)
    };

    let notify = |e: std::io::Error, duration| {
        warn!(
            "transient error encountered while write block, location {}, at duration {:?} : {}",
            location, duration, e,
        )
    };

    op.retry_with_notify(notify).await?;

    Ok(result)
}

pub fn serialize_data_blocks(
    blocks: Vec<DataBlock>,
    schema: &DataSchemaRef,
    buf: &mut Vec<u8>,
) -> Result<(u64, FileMetaData)> {
    let arrow_schema = schema.to_arrow();

    let row_group_write_options = WriteOptions {
        write_statistics: false,
        compression: CompressionOptions::Lz4Raw,
        version: Version::V2,
    };
    let batches = blocks
        .iter()
        .map(|b| Chunk::try_from(b.clone()))
        .collect::<Result<Vec<_>>>()?;

    let encodings: Vec<Vec<_>> = arrow_schema
        .fields
        .iter()
        .map(|f| match f.data_type() {
            ArrowDataType::Dictionary(..) => vec![Encoding::RleDictionary],
            _ => vec![col_encoding(f.data_type())],
        })
        .collect();

    let row_groups = RowGroupIterator::try_new(
        batches.iter().map(|c| Ok(c.clone())),
        &arrow_schema,
        row_group_write_options,
        encodings,
    )?;

    use common_arrow::parquet::write::WriteOptions as FileWriteOption;
    let options = FileWriteOption {
        write_statistics: false,
        version: Version::V2,
    };

    match write_parquet_file(buf, row_groups, arrow_schema.clone(), options) {
        Ok(result) => Ok(result),
        Err(cause) => Err(ErrorCode::ParquetError(cause.to_string())),
    }
}

fn col_encoding(_data_type: &ArrowDataType) -> Encoding {
    // Although encoding does work, parquet2 has not implemented decoding of DeltaLengthByteArray yet, we fallback to Plain
    // From parquet2: Decoding "DeltaLengthByteArray"-encoded required V2 pages is not yet implemented for Binary.
    //
    //match data_type {
    //    ArrowDataType::Binary
    //    | ArrowDataType::LargeBinary
    //    | ArrowDataType::Utf8
    //    | ArrowDataType::LargeUtf8 => Encoding::DeltaLengthByteArray,
    //    _ => Encoding::Plain,
    //}
    Encoding::Plain
}
