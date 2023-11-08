use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::error::Error;
use arrow2::io::flight::*;
use arrow2::io::ipc::write::default_ipc_fields;
use arrow2::io::ipc::write::WriteOptions;

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
