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
use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::io::parquet::write::WriteOptions;
use common_arrow::arrow::io::parquet::write::*;
use common_arrow::parquet::encoding::Encoding;
use common_arrow::parquet::FileMetaData;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use opendal::Operator;

pub async fn write_block(
    arrow_schema: &ArrowSchema,
    block: DataBlock,
    data_accessor: Operator,
    location: &str,
) -> Result<(u64, FileMetaData)> {
    let options = WriteOptions {
        write_statistics: false,
        compression: Compression::Lz4Raw,
        version: Version::V2,
    };
    let batch = Chunk::try_from(block)?;
    let encodings: Vec<_> = arrow_schema
        .fields
        .iter()
        .map(|f| col_encoding(&f.data_type))
        .collect();

    let iter = vec![Ok(batch)];
    let row_groups = RowGroupIterator::try_new(iter.into_iter(), arrow_schema, options, encodings)?;

    // we need a configuration of block size threshold here
    let mut buf = Vec::with_capacity(100 * 1024 * 1024);

    let result =
        common_arrow::write_parquet_file(&mut buf, row_groups, arrow_schema.clone(), options)
            .map_err(|e| ErrorCode::ParquetError(e.to_string()))?;

    data_accessor.object(location).write(buf).await?;

    Ok(result)
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
