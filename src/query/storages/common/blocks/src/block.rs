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

use common_arrow::arrow::chunk::Chunk;
use common_arrow::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
use common_arrow::arrow::io::parquet::write::transverse;
use common_arrow::arrow::io::parquet::write::RowGroupIterator;
use common_arrow::arrow::io::parquet::write::WriteOptions;
use common_arrow::parquet::encoding::Encoding;
use common_arrow::parquet::metadata::ThriftFileMetaData;
use common_arrow::parquet::write::Version;
use common_arrow::write_parquet_file;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::serialize::col_encoding;
use common_expression::{DataBlock, Value};
use common_expression::TableSchema;
use common_expression::types::AnyType;
use storages_common_table_meta::table::TableCompression;

/// Serialize data blocks to parquet format.
pub fn blocks_to_parquet(
    schema: impl AsRef<TableSchema>,
    blocks: Vec<DataBlock>,
    write_buffer: &mut Vec<u8>,
    compression: TableCompression,
) -> Result<(u64, ThriftFileMetaData)> {
    println!("schema: {:?}", schema.as_ref());
    let arrow_schema = schema.as_ref().to_arrow();
    println!("arrow_schema: {:?}", arrow_schema);

    let row_group_write_options = WriteOptions {
        write_statistics: false,
        version: Version::V2,
        compression: compression.into(),
        data_pagesize_limit: None,
    };
    let batches = blocks.clone()
        .into_iter()
        .map(Chunk::try_from)
        .collect::<Result<Vec<_>>>()?;

    let encodings: Vec<Vec<_>> = blocks
        .into_iter()
        .map(|block| get_encodings(&block, &arrow_schema))
        .collect::<Vec<_>>();


    // let encoding_map = |data_type: &ArrowDataType| match data_type {
    //     ArrowDataType::Dictionary(..) => Encoding::RleDictionary,
    //     _ => col_encoding(data_type),
    // };
    //
    // let encodings: Vec<Vec<_>> = arrow_schema
    //     .fields
    //     .iter()
    //     .map(|f| {
    //         println!("f.data_type: {:?}", f.data_type);
    //         transverse(&f.data_type, encoding_map)
    //     })
    //     .collect::<Vec<_>>();

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

    match write_parquet_file(write_buffer, row_groups, arrow_schema, options) {
        Ok(result) => Ok(result),
        Err(cause) => Err(ErrorCode::Internal(format!(
            "write_parquet_file: {:?}",
            cause,
        ))),
    }
}

fn get_encodings(block: &DataBlock, schema: &ArrowSchema) -> Vec<Encoding> {
    block.columns().into_iter().zip(schema.fields.iter()).map(
        |(column, field)| {
            get_encoding(&column.value, &field.data_type)
        }
    ).collect()
    //
    // let mut encodings = Vec::with_capacity(block.num_columns());
    // for i in 0..block.num_columns() {
    //     let field = schema.field(i);
    //     let data_type = field.data_type();
    //     let encoding = match data_type {
    //         ArrowDataType::Dictionary(..) => Encoding::RleDictionary,
    //         _ => col_encoding(data_type),
    //     };
    //     encodings.push(encoding);
    // }
    // encodings
}

fn get_encoding(value: &Value<AnyType>, data_type:&ArrowDataType)->Encoding{
    let encoding = match data_type {
        ArrowDataType::Dictionary(..) => Encoding::RleDictionary,
        _ => col_encoding(data_type),
    };
    encoding
}