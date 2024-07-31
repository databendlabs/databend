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

use std::io::Write;

use parquet2::metadata::KeyValue;
use parquet2::metadata::ThriftFileMetaData;
use parquet2::write::FileWriter;
use parquet2::write::WriteOptions;

use crate::arrow::array::Array;
use crate::arrow::chunk::Chunk;
use crate::arrow::datatypes::DataType;
use crate::arrow::datatypes::Schema;
use crate::arrow::error::Result;
use crate::arrow::io::parquet::write::to_parquet_schema;
use crate::arrow::io::parquet::write::RowGroupIterator;

// a simple wrapper for code reuse
pub fn write_parquet_file<W, A, I>(
    writer: &mut W,
    row_groups: RowGroupIterator<A, I>,
    schema: Schema,
    options: WriteOptions,
    created_by: Option<String>,
) -> Result<(u64, ThriftFileMetaData)>
where
    W: Write,
    A: AsRef<dyn Array> + 'static + Send + Sync,
    I: Iterator<Item = Result<Chunk<A>>>,
{
    // add extension data type to parquet meta.
    let mut key_values = Vec::new();
    for field in &schema.fields {
        if let DataType::Extension(ty, _, _) = &field.data_type {
            let key_value = KeyValue {
                key: field.name.clone(),
                value: Some(ty.clone()),
            };
            key_values.push(key_value);
        }
    }

    let parquet_schema = to_parquet_schema(&schema)?;

    let mut file_writer = FileWriter::new(writer, parquet_schema, options, created_by);

    for group in row_groups {
        file_writer.write(group?)?;
    }

    let key_value_metadata = if !key_values.is_empty() {
        Some(key_values)
    } else {
        None
    };
    let file_size = file_writer.end(key_value_metadata)?;
    let (_meta_size, thrift_file_meta_data) = file_writer.into_inner_and_metadata();
    Ok((file_size, thrift_file_meta_data))
}
