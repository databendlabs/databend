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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchema;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::table::TableCompression;
use parquet::arrow::ArrowWriter;
use parquet::basic::Encoding;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::EnabledStatistics;
use parquet::file::properties::WriterProperties;
use parquet::file::properties::WriterVersion;
use parquet::format::FileMetaData;

use crate::encoding_rules::ColumnPathsCache;
use crate::encoding_rules::ColumnStatsView;
use crate::encoding_rules::DeltaOrderingStats;
use crate::encoding_rules::EncodingStatsProvider;
use crate::encoding_rules::NdvProvider;
use crate::encoding_rules::delta_ordering::collect_delta_ordering_stats;
use crate::encoding_rules::dictionary::apply_dictionary_high_cardinality_heuristic;
use crate::encoding_rules::page_limit::MAX_BATCH_MEMORY_SIZE;
use crate::encoding_rules::page_limit::write_batch_with_page_limit;

/// Serialize data blocks to parquet format.
#[derive(Clone, Debug)]
pub struct ParquetWriteOptions {
    pub compression: TableCompression,
    pub enable_dictionary: bool,
    pub enable_delta_binary_packed_heuristic_rule: bool,
    pub metadata: Option<Vec<KeyValue>>,
}

impl ParquetWriteOptions {
    pub fn builder(compression: TableCompression) -> ParquetWriteOptionsBuilder {
        ParquetWriteOptionsBuilder {
            compression,
            enable_dictionary: false,
            enable_delta_binary_packed_heuristic_rule: false,
            metadata: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ParquetWriteOptionsBuilder {
    compression: TableCompression,
    enable_dictionary: bool,
    enable_delta_binary_packed_heuristic_rule: bool,
    metadata: Option<Vec<KeyValue>>,
}

impl ParquetWriteOptionsBuilder {
    pub fn enable_dictionary(mut self, enable: bool) -> Self {
        self.enable_dictionary = enable;
        self
    }

    pub fn enable_delta_binary_packed_heuristic_rule(mut self, enable: bool) -> Self {
        self.enable_delta_binary_packed_heuristic_rule = enable;
        self
    }

    pub fn metadata(mut self, metadata: Option<Vec<KeyValue>>) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn build(self) -> ParquetWriteOptions {
        ParquetWriteOptions {
            compression: self.compression,
            enable_dictionary: self.enable_dictionary,
            enable_delta_binary_packed_heuristic_rule: self
                .enable_delta_binary_packed_heuristic_rule,
            metadata: self.metadata,
        }
    }
}

struct ColumnStatsWithDelta<'a> {
    stats: &'a StatisticsOfColumns,
    delta_stats: HashMap<ColumnId, DeltaOrderingStats>,
}

impl<'a> NdvProvider for ColumnStatsWithDelta<'a> {
    fn column_ndv(&self, column_id: &ColumnId) -> Option<u64> {
        self.stats
            .get(column_id)
            .and_then(|item| item.distinct_of_values)
    }
}

impl<'a> EncodingStatsProvider for ColumnStatsWithDelta<'a> {
    fn column_stats(&self, column_id: &ColumnId) -> Option<&ColumnStatistics> {
        self.stats.get(column_id)
    }

    fn column_delta_stats(&self, column_id: &ColumnId) -> Option<&DeltaOrderingStats> {
        self.delta_stats.get(column_id)
    }
}

pub fn blocks_to_parquet(
    table_schema: &TableSchema,
    blocks: Vec<DataBlock>,
    write_buffer: &mut Vec<u8>,
    options: &ParquetWriteOptions,
) -> Result<FileMetaData> {
    blocks_to_parquet_with_stats(table_schema, blocks, write_buffer, options, None)
}

/// Serialize blocks while optionally tuning dictionary behavior via NDV statistics.
///
/// * `table_schema` - Logical schema used to build Arrow batches.
/// * `blocks` - In-memory blocks that will be serialized into a single Parquet file.
/// * `write_buffer` - Destination buffer that receives the serialized Parquet bytes.
/// * `options` - Parquet writer options controlling compression, dictionary, delta heuristics.
/// * `column_stats` - Optional NDV stats from the first block, used to configure writer properties
///   before ArrowWriter instantiation disables further changes.
pub fn blocks_to_parquet_with_stats(
    table_schema: &TableSchema,
    blocks: Vec<DataBlock>,
    write_buffer: &mut Vec<u8>,
    options: &ParquetWriteOptions,
    column_stats: Option<&StatisticsOfColumns>,
) -> Result<FileMetaData> {
    assert!(!blocks.is_empty());

    // Writer properties cannot be tweaked after ArrowWriter creation, so we mirror the behavior of
    // the streaming writer and only rely on the first block's NDV (and row count) snapshot.
    let num_rows = blocks[0].num_rows();
    let arrow_schema = Arc::new(table_schema.into());
    let column_stats_view = column_stats.map(ColumnStatsView);
    let column_metrics = if let Some(stats) = column_stats {
        if options.enable_delta_binary_packed_heuristic_rule {
            let delta_stats = collect_delta_ordering_stats(table_schema, &blocks[0])?;
            Some(ColumnStatsWithDelta { stats, delta_stats })
        } else {
            None
        }
    } else {
        None
    };
    let column_metrics_ref = column_metrics
        .as_ref()
        .map(|view| view as &dyn EncodingStatsProvider)
        .or_else(|| {
            column_stats_view
                .as_ref()
                .map(|view| view as &dyn EncodingStatsProvider)
        });

    let props = build_parquet_writer_properties(
        options.compression,
        options.enable_dictionary,
        options.enable_delta_binary_packed_heuristic_rule,
        column_metrics_ref,
        options.metadata.clone(),
        num_rows,
        table_schema,
    );

    let batches = blocks
        .into_iter()
        .map(|block| block.to_record_batch_with_arrow_schema(&arrow_schema))
        .collect::<Result<Vec<_>>>()?;

    let mut writer = ArrowWriter::try_new(write_buffer, arrow_schema, Some(props))?;
    for batch in batches {
        write_batch_with_page_limit(&mut writer, &batch, MAX_BATCH_MEMORY_SIZE)?;
    }
    let file_meta = writer.close()?;
    Ok(file_meta)
}

/// Create writer properties, optionally disabling dictionaries for high-cardinality columns.
pub fn build_parquet_writer_properties(
    compression: TableCompression,
    enable_dictionary: bool,
    enable_parquet_delta_binary_packed_heuristic_rule: bool,
    column_metrics: Option<&dyn EncodingStatsProvider>,
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
        .set_key_value_metadata(metadata)
        .set_dictionary_enabled(enable_dictionary);

    if enable_dictionary || enable_parquet_delta_binary_packed_heuristic_rule {
        builder = builder.set_writer_version(WriterVersion::PARQUET_2_0);
    }

    let mut column_paths_cache = ColumnPathsCache::new();

    if enable_dictionary {
        if let Some(metrics) = column_metrics {
            builder = apply_dictionary_high_cardinality_heuristic(
                builder,
                metrics,
                table_schema,
                num_rows,
                &mut column_paths_cache,
            );
        }
    }

    if enable_parquet_delta_binary_packed_heuristic_rule {
        if let Some(metrics) = column_metrics {
            builder =
                crate::encoding_rules::delta_binary_packed::apply_delta_binary_packed_heuristic(
                    builder,
                    metrics,
                    table_schema,
                    num_rows,
                    &mut column_paths_cache,
                );
        }
    }

    builder.build()
}

// Dictionary encoding tests are in encoding_rules/dictionary.rs.
// Delta binary packed encoding tests are in encoding_rules/delta_binary_packed.rs.
// Page-size limiting tests are in encoding_rules/page_limit.rs.
