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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchema;
use databend_common_expression::converts::arrow::table_schema_arrow_leaf_paths;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::table::TableCompression;
use parquet::arrow::ArrowWriter;
use parquet::basic::Encoding;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::EnabledStatistics;
use parquet::file::properties::WriterProperties;
use parquet::file::properties::WriterVersion;
use parquet::format::FileMetaData;
use parquet::schema::types::ColumnPath;

/// Disable dictionary encoding once the NDV-to-row ratio is greater than this threshold.
const HIGH_CARDINALITY_RATIO_THRESHOLD: f64 = 0.1;

/// Serialize data blocks to parquet format.
pub fn blocks_to_parquet(
    table_schema: &TableSchema,
    blocks: Vec<DataBlock>,
    write_buffer: &mut Vec<u8>,
    compression: TableCompression,
    enable_dictionary: bool,
    metadata: Option<Vec<KeyValue>>,
) -> Result<FileMetaData> {
    blocks_to_parquet_with_stats(
        table_schema,
        blocks,
        write_buffer,
        compression,
        enable_dictionary,
        metadata,
        None,
    )
}

/// Serialize blocks while optionally tuning dictionary behavior via NDV statistics.
///
/// * `table_schema` - Logical schema used to build Arrow batches.
/// * `blocks` - In-memory blocks that will be serialized into a single Parquet file.
/// * `write_buffer` - Destination buffer that receives the serialized Parquet bytes.
/// * `compression` - Compression algorithm specified by table-level settings.
/// * `enable_dictionary` - Enables dictionary encoding globally before per-column overrides.
/// * `metadata` - Additional user metadata embedded into the Parquet footer.
/// * `column_stats` - Optional NDV stats from the first block, used to configure writer properties
///   before ArrowWriter instantiation disables further changes.
pub fn blocks_to_parquet_with_stats(
    table_schema: &TableSchema,
    blocks: Vec<DataBlock>,
    write_buffer: &mut Vec<u8>,
    compression: TableCompression,
    enable_dictionary: bool,
    metadata: Option<Vec<KeyValue>>,
    column_stats: Option<&StatisticsOfColumns>,
) -> Result<FileMetaData> {
    assert!(!blocks.is_empty());

    // Writer properties cannot be tweaked after ArrowWriter creation, so we mirror the behavior of
    // the streaming writer and only rely on the first block's NDV (and row count) snapshot.
    let num_rows = blocks[0].num_rows();
    let arrow_schema = Arc::new(table_schema.into());

    let props = build_parquet_writer_properties(
        compression,
        enable_dictionary,
        column_stats,
        metadata,
        num_rows,
        table_schema,
    );

    let batches = blocks
        .into_iter()
        .map(|block| block.to_record_batch_with_arrow_schema(&arrow_schema))
        .collect::<Result<Vec<_>>>()?;

    let mut writer = ArrowWriter::try_new(write_buffer, arrow_schema, Some(props))?;
    for batch in batches {
        writer.write(&batch)?;
    }
    let file_meta = writer.close()?;
    Ok(file_meta)
}

/// Create writer properties, optionally disabling dictionaries for high-cardinality columns.
pub fn build_parquet_writer_properties(
    compression: TableCompression,
    enable_dictionary: bool,
    cols_stats: Option<impl NdvProvider>,
    metadata: Option<Vec<KeyValue>>,
    num_rows: usize,
    table_schema: &TableSchema,
) -> WriterProperties {
    let mut builder = WriterProperties::builder()
        .set_compression(compression.into())
        // use `usize::MAX` to effectively limit the number of row groups to 1
        .set_max_row_group_size(usize::MAX)
        .set_encoding(Encoding::PLAIN)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_bloom_filter_enabled(false)
        .set_key_value_metadata(metadata);

    if enable_dictionary {
        // Enable dictionary for all columns
        builder = builder
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_dictionary_enabled(true);
        if let Some(cols_stats) = cols_stats {
            // Disable dictionary of columns that have high cardinality
            for (column_id, components) in table_schema_arrow_leaf_paths(table_schema) {
                if let Some(ndv) = cols_stats.column_ndv(&column_id) {
                    if num_rows > 0
                        && (ndv as f64 / num_rows as f64) > HIGH_CARDINALITY_RATIO_THRESHOLD
                    {
                        builder = builder
                            .set_column_dictionary_enabled(ColumnPath::from(components), false);
                    }
                }
            }
        }
        builder.build()
    } else {
        builder.set_dictionary_enabled(false).build()
    }
}

/// Provides per column NDV statistics
pub trait NdvProvider {
    fn column_ndv(&self, column_id: &ColumnId) -> Option<u64>;
}

impl NdvProvider for &StatisticsOfColumns {
    fn column_ndv(&self, column_id: &ColumnId) -> Option<u64> {
        self.get(column_id).and_then(|item| item.distinct_of_values)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::types::number::NumberDataType;

    use super::*;

    struct TestNdvProvider {
        ndv: HashMap<ColumnId, u64>,
    }

    impl NdvProvider for TestNdvProvider {
        fn column_ndv(&self, column_id: &ColumnId) -> Option<u64> {
            self.ndv.get(column_id).copied()
        }
    }

    fn sample_schema() -> TableSchema {
        TableSchema::new(vec![
            TableField::new("simple", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("nested", TableDataType::Tuple {
                fields_name: vec!["leaf".to_string(), "arr".to_string()],
                fields_type: vec![
                    TableDataType::Number(NumberDataType::Int64),
                    TableDataType::Array(Box::new(TableDataType::Number(NumberDataType::UInt64))),
                ],
            }),
            TableField::new("no_stats", TableDataType::String),
        ])
    }

    fn column_id(schema: &TableSchema, name: &str) -> ColumnId {
        schema
            .leaf_fields()
            .into_iter()
            .find(|field| field.name() == name)
            .unwrap_or_else(|| panic!("missing field {}", name))
            .column_id()
    }

    #[test]
    fn test_build_parquet_writer_properties_handles_nested_leaves() {
        let schema = sample_schema();

        let mut ndv = HashMap::new();
        ndv.insert(column_id(&schema, "simple"), 500);
        ndv.insert(column_id(&schema, "nested:leaf"), 50);
        ndv.insert(column_id(&schema, "nested:arr:0"), 400);

        let column_paths: HashMap<ColumnId, ColumnPath> = table_schema_arrow_leaf_paths(&schema)
            .into_iter()
            .map(|(id, path)| (id, ColumnPath::from(path)))
            .collect();

        let props = build_parquet_writer_properties(
            TableCompression::Zstd,
            true,
            Some(TestNdvProvider { ndv }),
            None,
            1000,
            &schema,
        );

        assert!(
            !props.dictionary_enabled(&column_paths[&column_id(&schema, "simple")]),
            "high cardinality top-level column should disable dictionary"
        );
        assert!(
            props.dictionary_enabled(&column_paths[&column_id(&schema, "nested:leaf")]),
            "low cardinality nested column should keep dictionary"
        );
        assert!(
            !props.dictionary_enabled(&column_paths[&column_id(&schema, "nested:arr:0")]),
            "high cardinality nested array element should disable dictionary"
        );
        assert!(
            props.dictionary_enabled(&column_paths[&column_id(&schema, "no_stats")]),
            "columns without NDV stats keep the default dictionary behavior"
        );
    }

    #[test]
    fn test_build_parquet_writer_properties_disabled_globally() {
        let schema = sample_schema();

        let column_paths: HashMap<ColumnId, ColumnPath> = table_schema_arrow_leaf_paths(&schema)
            .into_iter()
            .map(|(id, path)| (id, ColumnPath::from(path)))
            .collect();

        let props = build_parquet_writer_properties(
            TableCompression::Zstd,
            false,
            None::<TestNdvProvider>,
            None,
            1000,
            &schema,
        );

        for field in schema.leaf_fields() {
            assert!(
                !props.dictionary_enabled(&column_paths[&field.column_id()]),
                "dictionary must remain disabled when enable_dictionary is false",
            );
        }
    }
}
