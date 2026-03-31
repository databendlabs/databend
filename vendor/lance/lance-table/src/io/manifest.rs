// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use async_trait::async_trait;
use byteorder::{ByteOrder, LittleEndian};
use bytes::{Bytes, BytesMut};
use lance_arrow::DataTypeExt;
use lance_file::{
    previous::writer::ManifestProvider as PreviousManifestProvider, version::LanceFileVersion,
};
use object_store::path::Path;
use prost::Message;
use snafu::location;
use std::collections::HashMap;
use std::{ops::Range, sync::Arc};
use tracing::instrument;

use lance_core::{datatypes::Schema, Error, Result};
use lance_io::{
    encodings::{binary::BinaryEncoder, plain::PlainEncoder, Encoder},
    object_store::ObjectStore,
    object_writer::ObjectWriter,
    traits::{WriteExt, Writer},
    utils::read_message,
};

use crate::format::{pb, DataStorageFormat, IndexMetadata, Manifest, Transaction, MAGIC};

use super::commit::ManifestLocation;

/// Read Manifest on URI.
///
/// This only reads manifest files. It does not read data files.
#[instrument(level = "debug", skip(object_store))]
pub async fn read_manifest(
    object_store: &ObjectStore,
    path: &Path,
    known_size: Option<u64>,
) -> Result<Manifest> {
    let file_size = if let Some(known_size) = known_size {
        known_size
    } else {
        object_store.inner.head(path).await?.size
    };
    const PREFETCH_SIZE: u64 = 64 * 1024;
    let initial_start = file_size.saturating_sub(PREFETCH_SIZE);
    let range = Range {
        start: initial_start,
        end: file_size,
    };
    let buf = object_store.inner.get_range(path, range).await?;

    // In case of corruption, the known_size might be wrong. We can retry without
    // the size to be more robust.
    if (buf.len() < 16 || !buf.ends_with(MAGIC)) && known_size.is_some() {
        return Box::pin(read_manifest(object_store, path, None)).await;
    }

    if buf.len() < 16 {
        return Err(Error::io(
            "Invalid format: file size is smaller than 16 bytes".to_string(),
            location!(),
        ));
    }
    if !buf.ends_with(MAGIC) {
        return Err(Error::io(
            "Invalid format: magic number does not match".to_string(),
            location!(),
        ));
    }
    let manifest_pos = LittleEndian::read_i64(&buf[buf.len() - 16..buf.len() - 8]) as usize;
    let manifest_len = file_size as usize - manifest_pos;

    let buf: Bytes = if manifest_len <= buf.len() {
        // The prefetch captured the entire manifest. We just need to trim the buffer.
        buf.slice(buf.len() - manifest_len..buf.len())
    } else {
        // The prefetch only captured part of the manifest. We need to make an
        // additional range request to read the remainder.
        let mut buf2: BytesMut = object_store
            .inner
            .get_range(
                path,
                Range {
                    start: manifest_pos as u64,
                    end: file_size - PREFETCH_SIZE,
                },
            )
            .await?
            .into_iter()
            .collect();
        buf2.extend_from_slice(&buf);
        buf2.freeze()
    };

    let recorded_length = LittleEndian::read_u32(&buf[0..4]) as usize;
    // Need to trim the magic number at end and message length at beginning
    let buf = buf.slice(4..buf.len() - 16);

    if buf.len() != recorded_length {
        return Err(Error::io(
            format!(
                "Invalid format: manifest length does not match. Expected {}, got {}",
                recorded_length,
                buf.len()
            ),
            location!(),
        ));
    }

    let proto = pb::Manifest::decode(buf)?;
    Manifest::try_from(proto)
}

#[instrument(level = "debug", skip(object_store, manifest))]
pub async fn read_manifest_indexes(
    object_store: &ObjectStore,
    location: &ManifestLocation,
    manifest: &Manifest,
) -> Result<Vec<IndexMetadata>> {
    if let Some(pos) = manifest.index_section.as_ref() {
        let reader = if let Some(size) = location.size {
            object_store
                .open_with_size(&location.path, size as usize)
                .await?
        } else {
            object_store.open(&location.path).await?
        };
        let section: pb::IndexSection = read_message(reader.as_ref(), *pos).await?;

        let indices = section
            .indices
            .into_iter()
            .map(IndexMetadata::try_from)
            .collect::<Result<Vec<_>>>()?;
        Ok(indices)
    } else {
        Ok(vec![])
    }
}

async fn do_write_manifest(
    writer: &mut dyn Writer,
    manifest: &mut Manifest,
    indices: Option<Vec<IndexMetadata>>,
    mut transaction: Option<Transaction>,
) -> Result<usize> {
    // Write indices if presented.
    if let Some(indices) = indices.as_ref() {
        let section = pb::IndexSection {
            indices: indices.iter().map(|i| i.into()).collect(),
        };
        let pos = writer.write_protobuf(&section).await?;
        manifest.index_section = Some(pos);
    }

    // Write inline transaction if presented.
    if let Some(tx) = transaction.take() {
        // Convert to protobuf at the write boundary to persist inline
        let pb_tx: pb::Transaction = tx.into();
        let pos = writer.write_protobuf(&pb_tx).await?;
        manifest.transaction_section = Some(pos);
    }

    writer.write_struct(manifest).await
}

/// Write manifest to an open file.
pub async fn write_manifest(
    writer: &mut dyn Writer,
    manifest: &mut Manifest,
    indices: Option<Vec<IndexMetadata>>,
    transaction: Option<Transaction>,
) -> Result<usize> {
    // Write dictionary values.
    let max_field_id = manifest.schema.max_field_id().unwrap_or(-1);
    let is_legacy_storage = manifest.should_use_legacy_format();
    for field_id in 0..max_field_id + 1 {
        if let Some(field) = manifest.schema.mut_field_by_id(field_id) {
            if field.data_type().is_dictionary() && is_legacy_storage {
                let dict_info = field.dictionary.as_mut().ok_or_else(|| {
                    Error::io(
                        format!("Lance field {} misses dictionary info", field.name),
                        location!(),
                    )
                })?;

                let value_arr = dict_info.values.as_ref().ok_or_else(|| {
                    Error::io(
                        format!(
                        "Lance field {} is dictionary type, but misses the dictionary value array",
                        field.name
                    ),
                        location!(),
                    )
                })?;

                let data_type = value_arr.data_type();
                let pos = match data_type {
                    dt if dt.is_numeric() => {
                        let mut encoder = PlainEncoder::new(writer, dt);
                        encoder.encode(&[value_arr]).await?
                    }
                    dt if dt.is_binary_like() => {
                        let mut encoder = BinaryEncoder::new(writer);
                        encoder.encode(&[value_arr]).await?
                    }
                    _ => {
                        return Err(Error::io(
                            format!(
                                "Does not support {} as dictionary value type",
                                value_arr.data_type()
                            ),
                            location!(),
                        ));
                    }
                };
                dict_info.offset = pos;
                dict_info.length = value_arr.len();
            }
        }
    }

    do_write_manifest(writer, manifest, indices, transaction).await
}

/// Implementation of ManifestProvider that describes a Lance file by writing
/// a manifest that contains nothing but default fields and the schema
pub struct ManifestDescribing {}

#[async_trait]
impl PreviousManifestProvider for ManifestDescribing {
    async fn store_schema(
        object_writer: &mut ObjectWriter,
        schema: &Schema,
    ) -> Result<Option<usize>> {
        let mut manifest = Manifest::new(
            schema.clone(),
            Arc::new(vec![]),
            DataStorageFormat::new(LanceFileVersion::Legacy),
            HashMap::new(),
        );
        let pos = do_write_manifest(object_writer, &mut manifest, None, None).await?;
        Ok(Some(pos))
    }
}

#[cfg(test)]
mod test {
    use arrow_array::{Int32Array, RecordBatch};
    use std::collections::HashMap;

    use crate::format::SelfDescribingFileReader;
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use lance_file::format::{MAGIC, MAJOR_VERSION, MINOR_VERSION};
    use lance_file::previous::{
        reader::FileReader as PreviousFileReader, writer::FileWriter as PreviousFileWriter,
    };
    use rand::{distr::Alphanumeric, Rng};
    use tokio::io::AsyncWriteExt;

    use super::*;

    async fn test_roundtrip_manifest(prefix_size: usize, manifest_min_size: usize) {
        let store = ObjectStore::memory();
        let path = Path::from("/read_large_manifest");

        let mut writer = store.create(&path).await.unwrap();

        // Write prefix we should ignore
        let prefix: Vec<u8> = rand::rng()
            .sample_iter(&Alphanumeric)
            .take(prefix_size)
            .collect();
        writer.write_all(&prefix).await.unwrap();

        let long_name: String = rand::rng()
            .sample_iter(&Alphanumeric)
            .take(manifest_min_size)
            .map(char::from)
            .collect();

        let arrow_schema =
            ArrowSchema::new(vec![ArrowField::new(long_name, DataType::Int64, false)]);
        let schema = Schema::try_from(&arrow_schema).unwrap();

        let mut config = HashMap::new();
        config.insert("key".to_string(), "value".to_string());

        let mut manifest = Manifest::new(
            schema,
            Arc::new(vec![]),
            DataStorageFormat::default(),
            HashMap::new(),
        );
        let pos = write_manifest(&mut writer, &mut manifest, None, None)
            .await
            .unwrap();
        writer
            .write_magics(pos, MAJOR_VERSION, MINOR_VERSION, MAGIC)
            .await
            .unwrap();
        writer.shutdown().await.unwrap();

        let roundtripped_manifest = read_manifest(&store, &path, None).await.unwrap();

        assert_eq!(manifest, roundtripped_manifest);

        store.inner.delete(&path).await.unwrap();
    }

    #[tokio::test]
    async fn test_read_large_manifest() {
        test_roundtrip_manifest(0, 100_000).await;
        test_roundtrip_manifest(1000, 100_000).await;
        test_roundtrip_manifest(1000, 1000).await;
    }

    #[tokio::test]
    async fn test_update_schema_metadata() {
        let store = ObjectStore::memory();
        let path = Path::from("/update_schema_metadata");

        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "i",
            DataType::Int32,
            false,
        )]));
        let schema = Schema::try_from(arrow_schema.as_ref()).unwrap();
        let mut file_writer = PreviousFileWriter::<ManifestDescribing>::try_new(
            &store,
            &path,
            schema.clone(),
            &Default::default(),
        )
        .await
        .unwrap();

        let array = Int32Array::from_iter_values(0..10);
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(array)]).unwrap();
        file_writer
            .write(std::slice::from_ref(&batch))
            .await
            .unwrap();
        let mut metadata = HashMap::new();
        metadata.insert(String::from("lance:extra"), String::from("for_test"));
        file_writer.finish_with_metadata(&metadata).await.unwrap();

        let reader = store.open(&path).await.unwrap();
        let reader = PreviousFileReader::try_new_self_described_from_reader(reader.into(), None)
            .await
            .unwrap();
        let schema = ArrowSchema::from(reader.schema());
        assert_eq!(schema.metadata().get("lance:extra").unwrap(), "for_test");
    }
}
