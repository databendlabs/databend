use std::fs::File;
use std::io::Read;

use ahash::AHashMap;
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::error::Result;
use arrow2::io::ipc::read::read_stream_metadata;
use arrow2::io::ipc::read::StreamReader;
use arrow2::io::ipc::IpcField;
use arrow2::io::json_integration::read;
use arrow2::io::json_integration::ArrowJson;
use flate2::read::GzDecoder;

type IpcRead = (Schema, Vec<IpcField>, Vec<Chunk<Box<dyn Array>>>);

/// Read gzipped JSON file
pub fn read_gzip_json(version: &str, file_name: &str) -> Result<IpcRead> {
    let testdata = crate::test_util::arrow_test_data();
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
    let testdata = crate::test_util::arrow_test_data();
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
