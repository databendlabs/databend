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

use common_arrow::arrow::error::Result;

use super::integration_read;
use super::integration_write;
use crate::arrow::io::ipc::read_gzip_json;

fn test_file(version: &str, file_name: &str) -> Result<()> {
    let (schema, _, batches) = read_gzip_json(version, file_name)?;

    // empty batches are not written/read from parquet and can be ignored
    let batches = batches
        .into_iter()
        .filter(|x| !x.is_empty())
        .collect::<Vec<_>>();

    let data = integration_write(&schema, &batches)?;

    let (read_schema, read_batches) = integration_read(&data, None)?;

    assert_eq!(schema, read_schema);
    assert_eq!(batches, read_batches);

    Ok(())
}

#[test]
fn roundtrip_100_primitive() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_primitive")?;
    test_file("1.0.0-bigendian", "generated_primitive")
}

#[test]
fn roundtrip_100_dict() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_dictionary")?;
    test_file("1.0.0-bigendian", "generated_dictionary")
}

#[test]
fn roundtrip_100_extension() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_extension")?;
    test_file("1.0.0-bigendian", "generated_extension")
}
