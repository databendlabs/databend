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

use std::fs::File;
use std::io::Read;

use ahash::AHashMap;
use common_arrow::arrow::array::Array;
use common_arrow::arrow::chunk::Chunk;
use common_arrow::arrow::datatypes::Schema;
use common_arrow::arrow::error::Result;
use common_arrow::arrow::io::ipc::read::read_stream_metadata;
use common_arrow::arrow::io::ipc::read::StreamReader;
use common_arrow::arrow::io::ipc::IpcField;
use common_arrow::arrow::io::json_integration::read;
use common_arrow::arrow::io::json_integration::ArrowJson;
use flate2::read::GzDecoder;

type IpcRead = (Schema, Vec<IpcField>, Vec<Chunk<Box<dyn Array>>>);

/// Read gzipped JSON file
pub fn read_gzip_json(version: &str, file_name: &str) -> Result<IpcRead> {
    let testdata = crate::arrow::test_util::arrow_test_data();
    let file = File::open(format!(
        "{testdata}/arrow-ipc-stream/integration/{version}/{file_name}.json.gz"
    ))
    .unwrap();
    let mut gz = GzDecoder::new(&file);
    let mut s = String::new();
    gz.read_to_string(&mut s).unwrap();
    // convert to Arrow JSON
    let arrow_json: ArrowJson = serde_json::from_str(&s)?;

    let schema = serde_json::to_value(arrow_json.schema).unwrap();

    let (schema, ipc_fields) = read::deserialize_schema(&schema)?;

    // read dictionaries
    let mut dictionaries = AHashMap::new();
    if let Some(dicts) = arrow_json.dictionaries {
        for json_dict in dicts {
            // TODO: convert to a concrete Arrow type
            dictionaries.insert(json_dict.id, json_dict);
        }
    }

    let batches = arrow_json
        .batches
        .iter()
        .map(|batch| read::deserialize_chunk(&schema, &ipc_fields, batch, &dictionaries))
        .collect::<Result<Vec<_>>>()?;

    Ok((schema, ipc_fields, batches))
}

pub fn read_arrow_stream(
    version: &str,
    file_name: &str,
    projection: Option<Vec<usize>>,
) -> IpcRead {
    let testdata = crate::arrow::test_util::arrow_test_data();
    let mut file = File::open(format!(
        "{testdata}/arrow-ipc-stream/integration/{version}/{file_name}.stream"
    ))
    .unwrap();

    let metadata = read_stream_metadata(&mut file).unwrap();
    let reader = StreamReader::new(file, metadata, projection);

    let schema = reader.metadata().schema.clone();
    let ipc_fields = reader.metadata().ipc_schema.fields.clone();

    (
        schema,
        ipc_fields,
        reader
            .map(|x| x.map(|x| x.unwrap()))
            .collect::<Result<_>>()
            .unwrap(),
    )
}
