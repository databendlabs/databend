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
use common_arrow::arrow::io::ipc::read::stream_async::*;
use futures::StreamExt;
use tokio::fs::File;
use tokio_util::compat::*;

use crate::io::ipc::common::read_gzip_json;

async fn test_file(version: &str, file_name: &str) -> Result<()> {
    let testdata = crate::test_util::arrow_test_data();
    let mut file = File::open(format!(
        "{testdata}/arrow-ipc-stream/integration/{version}/{file_name}.stream"
    ))
    .await?
    .compat();

    let metadata = read_stream_metadata_async(&mut file).await?;
    let mut reader = AsyncStreamReader::new(file, metadata);

    // read expected JSON output
    let (schema, ipc_fields, batches) = read_gzip_json(version, file_name)?;

    assert_eq!(&schema, &reader.metadata().schema);
    assert_eq!(&ipc_fields, &reader.metadata().ipc_schema.fields);

    let mut items = vec![];
    while let Some(item) = reader.next().await {
        items.push(item?)
    }

    batches
        .iter()
        .zip(items.into_iter())
        .for_each(|(lhs, rhs)| {
            assert_eq!(lhs, &rhs);
        });
    Ok(())
}

#[tokio::test]
async fn write_async() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_primitive").await
}
