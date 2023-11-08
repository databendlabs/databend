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

use common_arrow::arrow::array::*;
use common_arrow::arrow::chunk::Chunk;
use common_arrow::arrow::datatypes::*;
use common_arrow::arrow::error::Result;
use common_arrow::arrow::io::ipc::read;
use common_arrow::arrow::io::ipc::write::FileWriter;
use common_arrow::arrow::io::ipc::write::WriteOptions;

use super::file::write;

#[test]
fn basic() -> Result<()> {
    // prepare some data
    let array = BooleanArray::from([Some(true), Some(false), None, Some(true)]).boxed();
    let schema = Schema::from(vec![Field::new("a", array.data_type().clone(), true)]);
    let columns = Chunk::try_new(vec![array])?;

    let (expected_schema, expected_batches) = (schema.clone(), vec![columns.clone()]);

    // write to a file
    let result = write(&expected_batches, &schema, None, None)?;

    // read the file to append
    let mut file = std::io::Cursor::new(result);
    let metadata = read::read_file_metadata(&mut file)?;
    let mut writer = FileWriter::try_from_file(file, metadata, WriteOptions { compression: None })?;

    // write a new column
    writer.write(&columns, None)?;
    writer.finish()?;

    let data = writer.into_inner();
    let mut reader = std::io::Cursor::new(data.into_inner());

    // read the file again and confirm that it contains both messages
    let metadata = read::read_file_metadata(&mut reader)?;
    assert_eq!(schema, expected_schema);
    let reader = read::FileReader::new(reader, metadata, None, None);

    let chunks = reader.collect::<Result<Vec<_>>>()?;

    assert_eq!(chunks, vec![columns.clone(), columns]);

    Ok(())
}
