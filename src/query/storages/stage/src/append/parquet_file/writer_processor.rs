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

use std::mem;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::RecordBatchOptions;
use arrow_schema::DataType;
use arrow_schema::FieldRef;
use arrow_schema::Schema;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::principal::StageFileCompression;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_storages_common_stage::CopyIntoLocationInfo;
use opendal::Operator;
use parquet::arrow::ARROW_SCHEMA_META_KEY;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::arrow::encode_arrow_schema;
use parquet::basic::Compression;
use parquet::basic::ZstdLevel;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::EnabledStatistics;
use parquet::file::properties::WriterProperties;
use parquet::file::properties::WriterVersion;

use crate::append::column_based::file_writer::ColumnarFileEncoder;
use crate::append::column_based::file_writer::ColumnarFileWriter;

pub struct ParquetFileWriter;

struct ParquetEncoder {
    arrow_schema: Arc<Schema>,
    compression: Compression,
    create_by: String,
    target_file_size: Option<usize>,
    writer: ArrowWriter<Vec<u8>>,
}

const MAX_BUFFER_SIZE: usize = 64 * 1024 * 1024;
// Maximum number of rows in a Parquet row group.
const MAX_ROW_GROUP_SIZE: usize = 1024 * 1024;
// Maximum estimated encoded size of a Parquet row group.
const MAX_ROW_GROUP_BYTES: usize = 128 * 1024 * 1024;

/// Return an Arrow schema with canonical names for Parquet map entries.
///
/// Some older Arrow implementations look up map children by name. The arrays are
/// reused without copying; only their schema is reattached with field-name matching
/// disabled before writing.
fn parquet_compatible_schema(schema: &Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .map(parquet_compatible_field)
        .collect::<Vec<_>>();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

fn parquet_compatible_field(field: &FieldRef) -> FieldRef {
    Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(parquet_compatible_data_type(field.data_type())),
    )
}

fn parquet_compatible_map_field(field: &FieldRef) -> FieldRef {
    let field = parquet_compatible_field(field);
    let DataType::Struct(fields) = field.data_type() else {
        return field;
    };
    if fields.len() != 2 {
        return field;
    }

    let key = Arc::new(fields[0].as_ref().clone().with_name("key"));
    let value = Arc::new(fields[1].as_ref().clone().with_name("value"));
    Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(DataType::Struct(vec![key, value].into())),
    )
}

fn parquet_compatible_data_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::List(field) => DataType::List(parquet_compatible_field(field)),
        DataType::ListView(field) => DataType::ListView(parquet_compatible_field(field)),
        DataType::FixedSizeList(field, size) => {
            DataType::FixedSizeList(parquet_compatible_field(field), *size)
        }
        DataType::LargeList(field) => DataType::LargeList(parquet_compatible_field(field)),
        DataType::LargeListView(field) => DataType::LargeListView(parquet_compatible_field(field)),
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(parquet_compatible_field)
                .collect::<Vec<_>>()
                .into(),
        ),
        DataType::Dictionary(key, value) => DataType::Dictionary(
            Box::new(parquet_compatible_data_type(key)),
            Box::new(parquet_compatible_data_type(value)),
        ),
        DataType::Map(field, sorted) => DataType::Map(parquet_compatible_map_field(field), *sorted),
        _ => data_type.clone(),
    }
}

/// Return an Arrow schema that can be decoded by older Arrow implementations.
///
/// This schema is only embedded in the Parquet footer as an `ARROW:schema` hint. The
/// writer schema and arrays still use their original data types. Every conversion below
/// must therefore have the same Parquet representation as the writer type.
fn legacy_compatible_schema(schema: &Schema) -> Schema {
    let schema = parquet_compatible_schema(schema);
    let fields = schema
        .fields()
        .iter()
        .map(legacy_compatible_field)
        .collect::<Vec<_>>();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

fn legacy_compatible_field(field: &FieldRef) -> FieldRef {
    Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(legacy_compatible_data_type(field.data_type())),
    )
}

fn legacy_compatible_data_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::BinaryView => DataType::Binary,
        DataType::Utf8View => DataType::Utf8,
        DataType::List(field) => DataType::List(legacy_compatible_field(field)),
        DataType::ListView(field) => DataType::List(legacy_compatible_field(field)),
        DataType::FixedSizeList(field, size) => {
            DataType::FixedSizeList(legacy_compatible_field(field), *size)
        }
        DataType::LargeList(field) => DataType::LargeList(legacy_compatible_field(field)),
        DataType::LargeListView(field) => DataType::LargeList(legacy_compatible_field(field)),
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(legacy_compatible_field)
                .collect::<Vec<_>>()
                .into(),
        ),
        DataType::Dictionary(key, value) => DataType::Dictionary(
            Box::new(legacy_compatible_data_type(key)),
            Box::new(legacy_compatible_data_type(value)),
        ),
        DataType::Decimal32(precision, scale) | DataType::Decimal64(precision, scale) => {
            DataType::Decimal128(*precision, *scale)
        }
        DataType::Map(field, sorted) => DataType::Map(legacy_compatible_field(field), *sorted),
        _ => data_type.clone(),
    }
}

fn create_writer(
    arrow_schema: Arc<Schema>,
    target_file_size: Option<usize>,
    compression: Compression,
    create_by: String,
) -> Result<ArrowWriter<Vec<u8>>> {
    let metadata_schema = legacy_compatible_schema(&arrow_schema);
    let metadata = KeyValue {
        key: ARROW_SCHEMA_META_KEY.to_string(),
        value: Some(encode_arrow_schema(&metadata_schema)),
    };

    let props = WriterProperties::builder()
        // COPY INTO LOCATION is an interoperability boundary. Keep its output readable by
        // older Parquet implementations.
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .set_compression(compression)
        .set_created_by(create_by)
        .set_max_row_group_row_count(Some(MAX_ROW_GROUP_SIZE))
        .set_max_row_group_bytes(Some(MAX_ROW_GROUP_BYTES))
        .set_statistics_enabled(EnabledStatistics::Chunk)
        // RLE_DICTIONARY was added in Parquet 2.0 even though arrow-rs may use it with
        // WriterVersion::PARQUET_1_0. Disable dictionaries to keep value encodings at 1.0.
        .set_dictionary_enabled(false)
        .set_bloom_filter_enabled(false)
        .set_key_value_metadata(Some(vec![metadata]))
        .build();

    let buf_size = match target_file_size {
        Some(n) if n < MAX_BUFFER_SIZE => n,
        _ => MAX_BUFFER_SIZE,
    };
    let options = ArrowWriterOptions::new()
        .with_properties(props)
        // `ARROW:schema` above intentionally describes storage-equivalent legacy types.
        // Do not let ArrowWriter replace it with the original modern schema.
        .with_skip_arrow_metadata(true);
    Ok(ArrowWriter::try_new_with_options(
        Vec::with_capacity(buf_size),
        arrow_schema,
        options,
    )?)
}

impl ParquetEncoder {
    fn try_create(
        info: &CopyIntoLocationInfo,
        schema: TableSchemaRef,
        target_file_size: Option<usize>,
        create_by: String,
    ) -> Result<Self> {
        let arrow_schema = Arc::new(parquet_compatible_schema(&Schema::from(schema.as_ref())));
        let compression = info.stage.file_format_params.compression();
        let compression = match &compression {
            StageFileCompression::Zstd => Compression::ZSTD(ZstdLevel::default()),
            StageFileCompression::Snappy => Compression::SNAPPY,
            StageFileCompression::None => Compression::UNCOMPRESSED,
            _ => {
                return Err(ErrorCode::Internal(format!(
                    "unexpected compression {compression}"
                )));
            }
        };
        let writer = create_writer(
            arrow_schema.clone(),
            target_file_size,
            compression,
            create_by.clone(),
        )?;

        Ok(ParquetEncoder {
            arrow_schema,
            compression,
            create_by,
            target_file_size,
            writer,
        })
    }

    fn reinit_writer(&mut self) -> Result<()> {
        self.writer = create_writer(
            self.arrow_schema.clone(),
            self.target_file_size,
            self.compression,
            self.create_by.clone(),
        )?;
        Ok(())
    }
}

impl ColumnarFileEncoder for ParquetEncoder {
    const NAME: &'static str = "ParquetFileWriter";

    fn write(&mut self, block: DataBlock, schema: &TableSchemaRef) -> Result<()> {
        let batch = block.to_record_batch(schema)?;
        let options = RecordBatchOptions::new().with_match_field_names(false);
        let batch = RecordBatch::try_new_with_options(
            self.arrow_schema.clone(),
            batch.columns().to_vec(),
            &options,
        )?;
        self.writer.write(&batch)?;
        Ok(())
    }

    fn bytes_written(&self) -> usize {
        self.writer.bytes_written() + self.writer.in_progress_size()
    }

    fn finish(&mut self) -> Result<Vec<u8>> {
        self.writer.finish().ok();
        let buf = mem::take(self.writer.inner_mut());
        self.reinit_writer()?;
        Ok(buf)
    }
}

impl ParquetFileWriter {
    #[allow(clippy::too_many_arguments)]
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        info: CopyIntoLocationInfo,
        schema: TableSchemaRef,
        data_accessor: Operator,
        query_id: String,
        group_id: usize,
        target_file_size: Option<usize>,
        create_by: String,
    ) -> Result<ProcessorPtr> {
        let encoder =
            ParquetEncoder::try_create(&info, schema.clone(), target_file_size, create_by)?;
        ColumnarFileWriter::try_create(
            input,
            output,
            info,
            schema,
            data_accessor,
            query_id,
            group_id,
            target_file_size,
            encoder,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow_array::Array;
    use arrow_array::ArrayRef;
    use arrow_array::BooleanArray;
    use arrow_array::Decimal64Array;
    use arrow_array::Decimal128Array;
    use arrow_array::MapArray;
    use arrow_array::StringArray;
    use arrow_array::StringViewArray;
    use arrow_schema::Field;
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ArrowReaderOptions;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::basic::Encoding;
    use parquet::column::page::Page;
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;

    use super::*;

    #[test]
    fn test_legacy_compatible_schema_recursively_downgrades_storage_equivalent_types() {
        let field_metadata = HashMap::from([("extension".to_string(), "value".to_string())]);
        let nested = Field::new(
            "item",
            DataType::Struct(
                vec![
                    Field::new("string", DataType::Utf8View, true),
                    Field::new("binary", DataType::BinaryView, true),
                    Field::new("decimal", DataType::Decimal64(18, 3), true),
                    Field::new("wide_decimal", DataType::Decimal256(50, 4), true),
                ]
                .into(),
            ),
            true,
        )
        .with_metadata(field_metadata.clone());
        let schema_metadata = HashMap::from([("schema".to_string(), "metadata".to_string())]);
        let schema = Schema::new_with_metadata(
            vec![
                Field::new("list", DataType::ListView(Arc::new(nested)), true),
                Field::new(
                    "large_list",
                    DataType::LargeListView(Arc::new(Field::new(
                        "item",
                        DataType::Decimal32(9, 2),
                        false,
                    ))),
                    false,
                ),
                Field::new(
                    "map",
                    DataType::Map(
                        Arc::new(Field::new(
                            "entries",
                            DataType::Struct(
                                vec![
                                    Field::new("1", DataType::Utf8View, false),
                                    Field::new("2", DataType::Decimal64(18, 2), true),
                                ]
                                .into(),
                            ),
                            false,
                        )),
                        false,
                    ),
                    false,
                ),
            ],
            schema_metadata.clone(),
        );

        let compatible = legacy_compatible_schema(&schema);
        assert_eq!(compatible.metadata(), &schema_metadata);

        let DataType::List(item) = compatible.field(0).data_type() else {
            panic!("ListView must be downgraded to List");
        };
        assert_eq!(item.metadata(), &field_metadata);
        let DataType::Struct(fields) = item.data_type() else {
            panic!("nested struct must be preserved");
        };
        assert_eq!(fields[0].data_type(), &DataType::Utf8);
        assert_eq!(fields[1].data_type(), &DataType::Binary);
        assert_eq!(fields[2].data_type(), &DataType::Decimal128(18, 3));
        assert_eq!(fields[3].data_type(), &DataType::Decimal256(50, 4));

        let DataType::LargeList(item) = compatible.field(1).data_type() else {
            panic!("LargeListView must be downgraded to LargeList");
        };
        assert_eq!(item.data_type(), &DataType::Decimal128(9, 2));

        let DataType::Map(entries, false) = compatible.field(2).data_type() else {
            panic!("map must be preserved");
        };
        let DataType::Struct(fields) = entries.data_type() else {
            panic!("map entries must remain a struct");
        };
        assert_eq!(fields[0].name(), "key");
        assert_eq!(fields[0].data_type(), &DataType::Utf8);
        assert_eq!(fields[1].name(), "value");
        assert_eq!(fields[1].data_type(), &DataType::Decimal128(18, 2));
    }

    #[test]
    fn test_parquet_unload_uses_legacy_writer_and_compatible_schema() {
        let map = MapArray::new_from_strings(
            ["first", "second"].into_iter(),
            &StringArray::from(vec!["one", "two"]),
            &[0, 1, 2],
        )
        .unwrap();
        let original_schema = Arc::new(Schema::new(vec![
            Field::new("bool", DataType::Boolean, false),
            Field::new("string", DataType::Utf8View, false),
            Field::new("decimal", DataType::Decimal64(10, 2), false),
            Field::new("map", map.data_type().clone(), false),
        ]));
        let writer_schema = Arc::new(parquet_compatible_schema(&original_schema));
        let expected_metadata_schema = legacy_compatible_schema(&writer_schema);
        let batch = RecordBatch::try_new(original_schema.clone(), vec![
            Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef,
            Arc::new(StringViewArray::from(vec!["alpha", "beta"])) as ArrayRef,
            Arc::new(
                Decimal64Array::from(vec![1234_i64, -5678_i64])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ) as ArrayRef,
            Arc::new(map) as ArrayRef,
        ])
        .unwrap();
        let options = RecordBatchOptions::new().with_match_field_names(false);
        let batch = RecordBatch::try_new_with_options(
            writer_schema.clone(),
            batch.columns().to_vec(),
            &options,
        )
        .unwrap();

        let mut writer = create_writer(
            writer_schema,
            None,
            Compression::UNCOMPRESSED,
            "test".to_string(),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        let parquet_data = Bytes::copy_from_slice(writer.inner());

        let builder = ParquetRecordBatchReaderBuilder::try_new(parquet_data.clone()).unwrap();
        assert_eq!(builder.metadata().file_metadata().version(), 1);
        let kvs = builder
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .expect("ARROW:schema must be present");
        let arrow_schema_metadata = kvs
            .iter()
            .filter(|kv| kv.key == ARROW_SCHEMA_META_KEY)
            .collect::<Vec<_>>();
        assert_eq!(arrow_schema_metadata.len(), 1);
        assert_eq!(
            arrow_schema_metadata[0].value.as_deref(),
            Some(encode_arrow_schema(&expected_metadata_schema).as_str())
        );

        // Column chunk encodings also include RLE for definition/repetition levels. Inspect
        // every page to ensure values use only Parquet 1.0 PLAIN encoding and Data Page V1.
        let file_reader = SerializedFileReader::new(parquet_data.clone()).unwrap();
        let row_group = file_reader.get_row_group(0).unwrap();
        for column_index in 0..builder.metadata().row_group(0).columns().len() {
            let mut pages = row_group.get_column_page_reader(column_index).unwrap();
            let mut data_page_count = 0;
            while let Some(page) = pages.get_next_page().unwrap() {
                match page {
                    Page::DataPage { encoding, .. } => {
                        data_page_count += 1;
                        assert_eq!(encoding, Encoding::PLAIN);
                    }
                    Page::DataPageV2 { .. } => panic!("legacy unload must use Data Page V1"),
                    Page::DictionaryPage { .. } => {
                        panic!("legacy unload must not use dictionary encoding")
                    }
                }
            }
            assert!(data_page_count > 0);
        }
        for column in builder.metadata().row_group(0).columns() {
            for encoding in column.encodings() {
                assert!(matches!(encoding, Encoding::PLAIN | Encoding::RLE));
            }
        }

        assert_eq!(builder.schema().as_ref(), &expected_metadata_schema);
        let mut reader = builder.build().unwrap();
        let output = reader.next().unwrap().unwrap();
        let strings = output
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(strings.value(0), "alpha");
        assert_eq!(strings.value(1), "beta");
        let decimals = output
            .column(2)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(decimals.value(0), 1234_i128);
        assert_eq!(decimals.value(1), -5678_i128);
        let maps = output
            .column(3)
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();
        let (key, value) = maps.entries_fields();
        assert_eq!(key.name(), "key");
        assert_eq!(value.name(), "value");

        let options = ArrowReaderOptions::new().with_skip_arrow_metadata(true);
        let builder =
            ParquetRecordBatchReaderBuilder::try_new_with_options(parquet_data, options).unwrap();
        assert_eq!(builder.schema().field(1).data_type(), &DataType::Utf8);
        assert_eq!(
            builder.schema().field(2).data_type(),
            &DataType::Decimal128(10, 2)
        );
        let mut reader = builder.build().unwrap();
        let output = reader.next().unwrap().unwrap();
        let decimals = output
            .column(2)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(decimals.value(0), 1234_i128);
        assert_eq!(decimals.value(1), -5678_i128);
    }
}
