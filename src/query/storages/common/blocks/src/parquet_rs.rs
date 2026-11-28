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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_expression::converts::arrow::table_schema_arrow_leaf_paths;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::table::TableCompression;
use parquet::basic::Encoding;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::EnabledStatistics;
use parquet::file::properties::WriterProperties;
use parquet::file::properties::WriterVersion;
use parquet::schema::types::ColumnPath;

use crate::parquet_writer::BlockParquetWriter;
use crate::parquet_writer::SerializedParquet;

/// Disable dictionary encoding once the NDV-to-row ratio is greater than this threshold.
///
/// Columns such as URLs often have a third of a block distinct: their dictionary is still much
/// smaller than the plain values, and readers can keep it (see `enable_encoded_columns`).
const HIGH_CARDINALITY_RATIO_THRESHOLD: f64 = 0.5;

/// Size limit of a dictionary page, beyond which the writer falls back to the encoding of the
/// column for the rest of the column chunk. It is far above the 1 MiB other parquet writers use,
/// because a block is a few hundred thousand rows and a dictionary that holds all the values of a
/// block is much smaller than their bytes: on the `URL` and `Title` columns of the ClickBench
/// `hits` table, keeping it saves a tenth of the size of the column, and reads of those columns
/// are cheaper for it, lengths only or not. Lowering it to 1 MiB costs 7.6% more bytes there and
/// gives nothing back, since the pages it falls back to are the larger ones; a table that wants
/// the length-first pages for its strings asks for them with
/// `FUSE_OPT_KEY_STRING_DICTIONARY_PAGE_SIZE`.
const DICTIONARY_PAGE_SIZE_LIMIT: usize = 16 * 1024 * 1024;

/// The encoding of the pages of a `String` column that a dictionary does not hold: the lengths
/// of the values come first, those of a page encoded with `DELTA_BINARY_PACKED`, then the bytes
/// of the values. A scan that reads only the length of a value, such as one for `WHERE c = ''`
/// or `WHERE octet_length(c) > 0`, then decompresses only the start of a page (see
/// `length_chunk`).
const STRING_ENCODING: Encoding = Encoding::DELTA_LENGTH_BYTE_ARRAY;

/// The column ids of the `String` and `Nullable(String)` columns of `table_schema`.
fn string_columns(table_schema: &TableSchema) -> HashSet<ColumnId> {
    table_schema
        .fields()
        .iter()
        .filter(|field| field.data_type().remove_nullable() == TableDataType::String)
        .map(|field| field.column_id())
        .collect()
}

/// Parquet page size hard limit (~2GB). Arrow-rs 56.2.0+ errors if exceeded.
pub(crate) const PARQUET_PAGE_SIZE_HARD_LIMIT: usize = i32::MAX as usize - (1 << 20);

/// Soft limit for batch splitting. We use 64MB (vs 2GB hard limit) to provide safety
/// margin for estimation inaccuracy when row sizes are not uniform.
pub(crate) const MAX_BATCH_MEMORY_SIZE: usize = 1 << 26; // 64MB

/// Serialize data blocks to parquet format, returning the serialized bytes and metadata.
pub fn blocks_to_parquet(
    table_schema: &TableSchema,
    blocks: Vec<DataBlock>,
    compression: TableCompression,
    enable_dictionary: bool,
    metadata: Option<Vec<KeyValue>>,
) -> Result<SerializedParquet> {
    blocks_to_parquet_with_stats(
        table_schema,
        blocks,
        compression,
        enable_dictionary,
        metadata,
        None,
        None,
        None,
        None,
    )
}

/// Serialize blocks while optionally tuning dictionary behavior via NDV statistics.
///
/// * `table_schema` - Logical schema used to build Arrow batches.
/// * `blocks` - In-memory blocks that will be serialized into a single Parquet file.
/// * `compression` - Compression algorithm specified by table-level settings.
/// * `enable_dictionary` - Enables dictionary encoding globally before per-column overrides.
/// * `metadata` - Additional user metadata embedded into the Parquet footer.
/// * `column_stats` - Optional NDV stats from the first block, used to configure writer properties
///   before ArrowWriter instantiation disables further changes.
///
/// Returns the serialized parquet bytes together with the file metadata; the caller owns the
/// buffer directly (no intermediate copy into a borrowed buffer).
pub fn blocks_to_parquet_with_stats(
    table_schema: &TableSchema,
    blocks: Vec<DataBlock>,
    compression: TableCompression,
    enable_dictionary: bool,
    metadata: Option<Vec<KeyValue>>,
    column_stats: Option<&StatisticsOfColumns>,
    data_page_rows: Option<usize>,
    data_page_bytes: Option<usize>,
    string_dictionary_page_size: Option<usize>,
) -> Result<SerializedParquet> {
    assert!(!blocks.is_empty());

    // Dictionary heuristics rely only on the first block's NDV (and row count) snapshot,
    // matching the streaming writer's behavior.
    let num_rows = blocks[0].num_rows();
    let arrow_schema = Arc::new(table_schema.into());

    let props = Arc::new(build_parquet_writer_properties(
        compression,
        enable_dictionary,
        column_stats,
        metadata,
        num_rows,
        table_schema,
        data_page_rows,
        data_page_bytes,
        string_dictionary_page_size,
    ));

    // `BlockParquetWriter` encodes each block immediately and splits oversized batches into
    // page-bounded chunks internally, so no pre-splitting is needed here.
    let mut writer = BlockParquetWriter::new(arrow_schema, props);
    writer.write_blocks(blocks)?;
    writer.finish()
}

/// Create writer properties, optionally disabling dictionaries for high-cardinality columns.
pub fn build_parquet_writer_properties(
    compression: TableCompression,
    enable_dictionary: bool,
    cols_stats: Option<impl NdvProvider>,
    metadata: Option<Vec<KeyValue>>,
    num_rows: usize,
    table_schema: &TableSchema,
    data_page_rows: Option<usize>,
    data_page_bytes: Option<usize>,
    string_dictionary_page_size: Option<usize>,
) -> WriterProperties {
    let mut builder = WriterProperties::builder()
        .set_compression(compression.into())
        // use `usize::MAX` to effectively limit the number of row groups to 1
        .set_max_row_group_row_count(Some(usize::MAX))
        .set_encoding(Encoding::PLAIN)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_bloom_filter_enabled(false)
        .set_key_value_metadata(metadata);

    if let Some(rows) = data_page_rows {
        builder = builder.set_data_page_row_count_limit(rows);
    }
    if let Some(bytes) = data_page_bytes {
        builder = builder.set_data_page_size_limit(bytes);
    }

    // The pages a string column falls back to once its dictionary is full are string encoded,
    // which a read of only the lengths of its values decompresses from the page start.
    let string_columns = string_columns(table_schema);
    for (column_id, components) in table_schema_arrow_leaf_paths(table_schema) {
        if !string_columns.contains(&column_id) {
            continue;
        }
        let path = ColumnPath::from(components);
        builder = builder.set_column_encoding(path.clone(), STRING_ENCODING);
        if let Some(limit) = string_dictionary_page_size {
            builder = builder.set_column_dictionary_page_size_limit(path, limit);
        }
    }

    if enable_dictionary {
        // Enable dictionary for all columns
        builder = builder
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_dictionary_enabled(true)
            .set_dictionary_page_size_limit(DICTIONARY_PAGE_SIZE_LIMIT);
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

    use databend_common_expression::BlockEntry;
    use databend_common_expression::FromData;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::types::StringType;
    use databend_common_expression::types::number::NumberDataType;
    use parquet::file::reader::FileReader;

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
        ndv.insert(column_id(&schema, "simple"), 600);
        ndv.insert(column_id(&schema, "nested:leaf"), 400);
        ndv.insert(column_id(&schema, "nested:arr:0"), 900);

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
            None,
            None,
            None,
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
        for field in schema.leaf_fields() {
            assert_eq!(
                props.column_dictionary_page_size_limit(&column_paths[&field.column_id()]),
                DICTIONARY_PAGE_SIZE_LIMIT
            );
        }
        assert_eq!(
            props.encoding(&column_paths[&column_id(&schema, "no_stats")]),
            Some(STRING_ENCODING),
            "a string column is encoded for length-only reads"
        );
        assert_eq!(
            props.encoding(&column_paths[&column_id(&schema, "simple")]),
            Some(Encoding::PLAIN),
            "a column of another type is left to the plain encoding"
        );
    }

    #[test]
    fn test_string_dictionary_page_size_option() {
        let schema = sample_schema();
        let column_paths: HashMap<ColumnId, ColumnPath> = table_schema_arrow_leaf_paths(&schema)
            .into_iter()
            .map(|(id, path)| (id, ColumnPath::from(path)))
            .collect();
        let props = |limit| {
            build_parquet_writer_properties(
                TableCompression::Zstd,
                true,
                None::<&StatisticsOfColumns>,
                None,
                1000,
                &schema,
                None,
                None,
                limit,
            )
        };

        // Unset: every column keeps the default limit.
        let default = props(None);
        for field in schema.leaf_fields() {
            assert_eq!(
                default.column_dictionary_page_size_limit(&column_paths[&field.column_id()]),
                DICTIONARY_PAGE_SIZE_LIMIT
            );
        }

        // Set: it overrides the limit of the string columns only, so that they fall back to the
        // pages a read of only their lengths decompresses from the start.
        let small = props(Some(64 * 1024));
        assert_eq!(
            small.column_dictionary_page_size_limit(&column_paths[&column_id(&schema, "no_stats")]),
            64 * 1024
        );
        assert_eq!(
            small.column_dictionary_page_size_limit(&column_paths[&column_id(&schema, "simple")]),
            DICTIONARY_PAGE_SIZE_LIMIT
        );
    }

    #[test]
    fn test_string_pages_are_delta_length_encoded() {
        let schema = TableSchema::new(vec![TableField::new("s", TableDataType::String)]);
        let column_id = schema.fields()[0].column_id();
        let values = (0..1000).map(|i| format!("value-{i}")).collect::<Vec<_>>();
        let block = DataBlock::new(vec![BlockEntry::from(StringType::from_data(values))], 1000);
        let parquet =
            blocks_to_parquet(&schema, vec![block], TableCompression::Zstd, false, None).unwrap();

        let file = bytes::Bytes::from(parquet.payload.concat());
        let reader = parquet::file::serialized_reader::SerializedFileReader::new(file).unwrap();
        let mut pages = reader
            .get_row_group(0)
            .unwrap()
            .get_column_page_reader(0)
            .unwrap();
        let mut count = 0;
        while let Some(page) = pages.get_next_page().unwrap() {
            if let parquet::column::page::Page::DataPage { encoding, .. }
            | parquet::column::page::Page::DataPageV2 { encoding, .. } = page
            {
                assert_eq!(encoding, Encoding::DELTA_LENGTH_BYTE_ARRAY);
                count += 1;
            }
        }
        assert!(count > 0, "column {column_id} has no data page");
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
            None,
            None,
            None,
        );

        for field in schema.leaf_fields() {
            assert!(
                !props.dictionary_enabled(&column_paths[&field.column_id()]),
                "dictionary must remain disabled when enable_dictionary is false",
            );
        }
    }
}
