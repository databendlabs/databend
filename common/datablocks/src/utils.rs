// Copyright 2021 Datafuse Labs.
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

use common_arrow::arrow::chunk::Chunk;
use common_arrow::arrow::datatypes::DataType as ArrowDataType;
use common_arrow::arrow::io::parquet::write::RowGroupIterator;
use common_arrow::arrow::io::parquet::write::WriteOptions;
use common_arrow::parquet::compression::CompressionOptions;
use common_arrow::parquet::encoding::Encoding;
use common_arrow::parquet::metadata::ThriftFileMetaData;
use common_arrow::parquet::write::Version;
use common_arrow::write_parquet_file;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::DataBlock;

pub fn serialize_data_blocks(
    blocks: Vec<DataBlock>,
    schema: &DataSchemaRef,
    buf: &mut Vec<u8>,
) -> Result<(u64, ThriftFileMetaData)> {
    let arrow_schema = schema.to_arrow();

    let row_group_write_options = WriteOptions {
        write_statistics: false,
        compression: CompressionOptions::Lz4Raw,
        version: Version::V2,
    };
    let batches = blocks
        .into_iter()
        .map(|b| Chunk::try_from(b))
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
        batches.into_iter().map(Ok),
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
    // match data_type {
    //    ArrowDataType::Binary
    //    | ArrowDataType::LargeBinary
    //    | ArrowDataType::Utf8
    //    | ArrowDataType::LargeUtf8 => Encoding::DeltaLengthByteArray,
    //    _ => Encoding::Plain,
    //}
    Encoding::Plain
}
