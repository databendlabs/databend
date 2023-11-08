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

use common_arrow::arrow::array::Array;
use common_arrow::arrow::chunk::Chunk;
use common_arrow::arrow::datatypes::Schema;
use common_arrow::arrow::error::Error;
use common_arrow::arrow::io::flight::*;
use common_arrow::arrow::io::ipc::write::default_ipc_fields;
use common_arrow::arrow::io::ipc::write::WriteOptions;

use super::ipc::read_gzip_json;

fn round_trip(schema: Schema, chunk: Chunk<Box<dyn Array>>) -> Result<(), Error> {
    let fields = default_ipc_fields(&schema.fields);
    let serialized = serialize_schema(&schema, Some(&fields));
    let (result, ipc_schema) = deserialize_schemas(&serialized.data_header)?;
    assert_eq!(schema, result);

    let (_, batch) = serialize_batch(&chunk, &fields, &WriteOptions { compression: None })?;

    let result = deserialize_batch(&batch, &result.fields, &ipc_schema, &Default::default())?;
    assert_eq!(result, chunk);
    Ok(())
}

#[test]
fn generated_nested_dictionary() -> Result<(), Error> {
    let (schema, _, mut batches) =
        read_gzip_json("1.0.0-littleendian", "generated_nested").unwrap();

    round_trip(schema, batches.pop().unwrap())?;

    Ok(())
}
