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

use common_arrow::arrow::datatypes::DataType as ArrowDataType;
use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::io::parquet::write::WriteOptions;
use common_arrow::arrow::io::parquet::write::*;
use common_arrow::arrow::record_batch::RecordBatch;
use common_arrow::parquet::encoding::Encoding;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::io::Cursor;
use opendal::Operator;

pub async fn write_block(
    arrow_schema: &ArrowSchema,
    block: DataBlock,
    data_accessor: Operator,
    location: &str,
) -> Result<u64> {
    let options = WriteOptions {
        write_statistics: true,
        compression: Compression::Lz4, // let's begin with lz4
        version: Version::V2,
    };
    let batch = RecordBatch::try_from(block)?;
    let encodings: Vec<_> = arrow_schema
        .fields()
        .iter()
        .map(|f| col_encoding(&f.data_type))
        .collect();

    let iter = vec![Ok(batch)];
    let row_groups = RowGroupIterator::try_new(iter.into_iter(), arrow_schema, options, encodings)?;
    let parquet_schema = row_groups.parquet_schema().clone();

    // PutObject in S3 need to know the content-length in advance
    // multipart upload may intimidate this, but let's fit things together first
    // see issue #xxx

    use bytes::BufMut;
    // we need a configuration of block size threshold here
    let mut writer = Vec::with_capacity(100 * 1024 * 1024).writer();

    let len = common_arrow::parquet::write::write_file(
        &mut writer,
        row_groups,
        parquet_schema,
        options,
        None,
        None,
    )
    .map_err(|e| ErrorCode::ParquetError(e.to_string()))?;

    let parquet = writer.into_inner();
    let stream_len = parquet.len();

    data_accessor
        .write(location, stream_len as u64)
        .run(Box::new(Cursor::new(parquet)))
        .await
        .map_err(|e| ErrorCode::DalTransportError(e.to_string()))?;

    Ok(len)
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
