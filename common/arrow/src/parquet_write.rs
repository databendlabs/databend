// Copyright 2022 Datafuse Labs.
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

use arrow::array::Array;
use arrow::chunk::Chunk;
use arrow::datatypes::Schema;
use arrow::error::Result;
use arrow::io::parquet::write::FileWriter;
use arrow::io::parquet::write::RowGroupIterator;
use parquet2::write::WriteOptions;

// a simple wrapper for code reuse
pub fn write_parquet_file<W: Write, A, I>(
    writer: &mut W,
    row_groups: RowGroupIterator<A, I>,
    schema: Schema,
    options: WriteOptions,
) -> Result<u64>
where
    W: Write,
    A: AsRef<dyn Array> + 'static + Send + Sync,
    I: Iterator<Item = Result<Chunk<A>>>,
{
    let mut file_writer = FileWriter::try_new(writer, schema, options)?;

    file_writer.start()?;
    for group in row_groups {
        let (group, len) = group?;
        file_writer.write(group, len)?;
    }
    let (size, _) = file_writer.end(None)?;
    Ok(size)
}
