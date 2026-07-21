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

//! Low-level, logical-column-oriented FUSE block writer.
//!
//! The writer owns the complete physical output of one FUSE block: cluster-key metadata, the main
//! Parquet file, statistics, and all configured index files. Logical columns must be supplied in
//! order; each column may be supplied through multiple `write` calls. Every file is closed before
//! [`FuseLowLevelBlockWriter::finish`] returns.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use chrono::Utc;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::FieldIndex;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::converts::arrow::table_schema_arrow_leaf_paths;
use databend_common_expression::types::DataType;
use databend_common_metrics::storage::metrics_inc_block_index_write_bytes;
use databend_common_metrics::storage::metrics_inc_block_index_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_index_write_nums;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_bytes;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_nums;
use databend_common_metrics::storage::metrics_inc_block_virtual_column_write_bytes;
use databend_common_metrics::storage::metrics_inc_block_virtual_column_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_virtual_column_write_nums;
use databend_common_metrics::storage::metrics_inc_block_write_bytes;
use databend_common_metrics::storage::metrics_inc_block_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_write_nums;
use databend_storages_common_blocks::BlockingWrite;
use databend_storages_common_blocks::BulkParquetFileWriter;
use databend_storages_common_blocks::BulkParquetLeafWriter;
use databend_storages_common_index::NgramArgs;
use databend_storages_common_io::OpenDalBlockingWrite;
use databend_storages_common_io::create_blocking_write;
use databend_storages_common_table_meta::meta::BlockHLLState;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::BlockTopN;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::Compression;
use databend_storages_common_table_meta::meta::GranuleIndexFileLayout;
use databend_storages_common_table_meta::meta::GranuleIndexLayout;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::StatisticsOfSpatialColumns;
use databend_storages_common_table_meta::meta::StatisticsOfVectorColumns;
use databend_storages_common_table_meta::meta::encode_column_hll;
use opendal::Operator;
use parquet::arrow::arrow_writer::compute_leaves;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::WriterPropertiesPtr;
use parquet::schema::types::ColumnPath;

use super::BlockColumnSketchesBuilder;
use super::BloomIndexWriteSpec;
use super::GranuleIndexFileState;
use super::GranuleIndexFileWriter;
use super::InvertedIndexBuilder;
use super::SpatialIndexBuilder;
use super::VectorIndexBuilder;
use super::VirtualColumnBuilder;
use super::WriteSettings;
use super::block_index::BlockIndexLowLevelWriteContext;
use super::block_index::BlockIndexLowLevelWriter;
use super::block_index::BlockIndexSpec;
use super::block_index::WrittenBlockIndexOutput;
use super::stream::ColumnStatisticsState;
use crate::FuseStorageFormat;
use crate::io::granule_index::GranuleIndexLowLevelColumnWriter;
use crate::io::granule_index::GranuleIndexLowLevelOutput;
use crate::io::granule_index::GranuleIndexLowLevelWriter;
use crate::operations::column_parquet_metas;

/// Cluster-key configuration for one output block.
#[derive(Clone)]
pub struct FuseLowLevelClusterKeyOptions {
    cluster_key_id: u32,
    fields: Vec<DataType>,
    level: i32,
}

impl FuseLowLevelClusterKeyOptions {
    pub fn new(cluster_key_id: u32, fields: Vec<DataType>, level: i32) -> Self {
        Self {
            cluster_key_id,
            fields,
            level,
        }
    }
}

/// Configuration shared by block writes using the same table schema and write settings.
#[derive(Clone)]
pub struct FuseLowLevelWriteContext {
    func_ctx: FunctionContext,
    operator: Operator,
    schema: TableSchemaRef,
    write_settings: WriteSettings,
}

impl FuseLowLevelWriteContext {
    pub fn new(
        func_ctx: FunctionContext,
        operator: Operator,
        schema: TableSchemaRef,
        write_settings: WriteSettings,
    ) -> Self {
        Self {
            func_ctx,
            operator,
            schema,
            write_settings,
        }
    }
}

#[derive(Default)]
pub struct FuseLowLevelStatisticsOptions {
    stats_columns: Vec<(ColumnId, DataType)>,
    distinct_columns: Vec<(ColumnId, DataType)>,
    serialize_hll: bool,
}

impl FuseLowLevelStatisticsOptions {
    pub fn new(
        stats_columns: Vec<(ColumnId, DataType)>,
        distinct_columns: Vec<(ColumnId, DataType)>,
        serialize_hll: bool,
    ) -> Self {
        Self {
            stats_columns,
            distinct_columns,
            serialize_hll,
        }
    }
}

pub struct FuseLowLevelBloomIndexOptions {
    location: Location,
    columns: BTreeMap<FieldIndex, TableField>,
    ngram_args: Vec<NgramArgs>,
}

impl FuseLowLevelBloomIndexOptions {
    pub fn new(
        location: Location,
        columns: BTreeMap<FieldIndex, TableField>,
        ngram_args: Vec<NgramArgs>,
    ) -> Self {
        Self {
            location,
            columns,
            ngram_args,
        }
    }

    fn into_spec(self) -> BloomIndexWriteSpec {
        BloomIndexWriteSpec::new(self.columns, self.ngram_args, self.location)
    }
}

pub struct FuseLowLevelGranuleIndexOptions {
    mins_location: Option<Location>,
    offsets_location: Location,
    writers: Vec<Box<dyn GranuleIndexLowLevelWriter>>,
}

impl FuseLowLevelGranuleIndexOptions {
    pub fn new(
        mins_location: Option<Location>,
        offsets_location: Location,
        writers: Vec<Box<dyn GranuleIndexLowLevelWriter>>,
    ) -> Self {
        Self {
            mins_location,
            offsets_location,
            writers,
        }
    }

    fn locations(&self) -> (Option<Location>, Location) {
        (self.mins_location.clone(), self.offsets_location.clone())
    }
}

#[derive(Clone)]
struct ClusterKeysWriteOptions {
    keys: FuseLowLevelClusterKeyOptions,
    stats: Option<ClusterStatistics>,
}

#[derive(Clone)]
struct VirtualColumnsWrite {
    builder: VirtualColumnBuilder,
    write_settings: WriteSettings,
    block_location: Location,
}

impl VirtualColumnsWrite {
    fn add_column(&mut self, field_index: FieldIndex, column: &Column) -> Result<()> {
        self.builder.add_column(field_index, column)
    }

    fn finish(&mut self) -> Result<super::virtual_column_builder::VirtualColumnState> {
        self.builder
            .finalize(&self.write_settings, &self.block_location)
    }
}

struct GranuleIndexesWrite {
    rows: usize,
    writers: Vec<Box<dyn GranuleIndexLowLevelWriter>>,
    output: GranuleIndexLowLevelOutput,
    writer: GranuleIndexFileWriter,
}

impl GranuleIndexesWrite {
    fn next_column(mut self) -> Result<GranuleIndexesColumnWrite> {
        let writers = std::mem::take(&mut self.writers)
            .into_iter()
            .map(|writer| writer.next_column())
            .collect::<Result<Vec<_>>>()?;
        Ok(GranuleIndexesColumnWrite {
            parent: Some(self),
            writers,
        })
    }

    fn finish(
        mut self,
        metadata: &ParquetMetaData,
        row_count: usize,
    ) -> Result<GranuleIndexFileState> {
        for writer in self.writers {
            self.output.merge(writer.finish()?)?;
        }
        let page_layout = page_layout_from_metadata(metadata)?;
        self.writer.serialize_offsets(
            &page_layout,
            row_count.div_ceil(self.rows),
            self.output.marks,
        )
    }
}

struct GranuleIndexesColumnWrite {
    parent: Option<GranuleIndexesWrite>,
    writers: Vec<Box<dyn GranuleIndexLowLevelColumnWriter>>,
}

impl GranuleIndexesColumnWrite {
    fn write(&mut self, column: &Column) -> Result<()> {
        for writer in self.writers.iter_mut() {
            writer.write(column)?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<GranuleIndexesWrite> {
        let mut parent = self
            .parent
            .take()
            .ok_or_else(|| ErrorCode::Internal("granule index column group has no parent"))?;
        for writer in self.writers {
            parent.writers.push(writer.finish()?);
        }
        Ok(parent)
    }
}

/// Immutable configuration of a low-level block write.
pub struct FuseLowLevelBlockWriteOptions {
    context: FuseLowLevelWriteContext,
    writer_properties: WriterPropertiesPtr,
    block_location: Location,
    statistics: FuseLowLevelStatisticsOptions,
    block_indexes: Vec<Box<dyn BlockIndexSpec>>,
    ndv_columns: BTreeMap<FieldIndex, TableField>,
    top_n: Option<(BTreeMap<FieldIndex, TableField>, usize)>,
    virtual_columns: Option<VirtualColumnsWrite>,
    granule_indexes: Option<FuseLowLevelGranuleIndexOptions>,
    cluster_keys: Option<ClusterKeysWriteOptions>,
}

impl FuseLowLevelBlockWriteOptions {
    pub fn new(
        context: FuseLowLevelWriteContext,
        writer_properties: WriterPropertiesPtr,
        block_location: Location,
    ) -> Self {
        Self {
            context,
            writer_properties,
            block_location,
            statistics: FuseLowLevelStatisticsOptions::default(),
            block_indexes: Vec::new(),
            ndv_columns: BTreeMap::new(),
            top_n: None,
            virtual_columns: None,
            granule_indexes: None,
            cluster_keys: None,
        }
    }

    pub fn set_statistics(&mut self, options: FuseLowLevelStatisticsOptions) {
        self.statistics = options;
    }

    pub fn set_bloom_indexes(&mut self, options: FuseLowLevelBloomIndexOptions) {
        self.block_indexes.push(Box::new(options.into_spec()));
    }

    pub fn set_ndv_columns(&mut self, columns: BTreeMap<FieldIndex, TableField>) {
        self.ndv_columns = columns;
    }

    pub fn set_top_n_columns(&mut self, columns: BTreeMap<FieldIndex, TableField>, size: usize) {
        self.top_n = Some((columns, size));
    }

    pub fn set_inverted_indexes(&mut self, builders: Vec<InvertedIndexBuilder>) {
        self.block_indexes.extend(
            builders
                .into_iter()
                .map(|builder| Box::new(builder) as Box<dyn BlockIndexSpec>),
        );
    }

    pub fn set_virtual_columns(&mut self, builder: Option<VirtualColumnBuilder>) {
        self.virtual_columns = builder.map(|builder| VirtualColumnsWrite {
            builder,
            write_settings: self.context.write_settings.clone(),
            block_location: self.block_location.clone(),
        });
    }

    pub fn set_vector_index(&mut self, location: Location, builder: VectorIndexBuilder) {
        self.block_indexes.push(Box::new(
            builder.into_write_spec(location, self.context.schema.num_fields()),
        ));
    }

    pub fn set_spatial_index(&mut self, location: Location, builder: SpatialIndexBuilder) {
        self.block_indexes.push(Box::new(
            builder.into_write_spec(location, self.context.schema.num_fields()),
        ));
    }

    pub fn set_granule_indexes(&mut self, options: FuseLowLevelGranuleIndexOptions) {
        self.granule_indexes = Some(options);
    }

    pub fn set_cluster_keys(
        &mut self,
        keys: FuseLowLevelClusterKeyOptions,
        stats: Option<ClusterStatistics>,
    ) {
        self.cluster_keys = Some(ClusterKeysWriteOptions { keys, stats });
    }

    fn granule_rows(&self) -> Option<usize> {
        self.context.write_settings.index_granularity
    }

    fn validate(&self) -> Result<()> {
        if !matches!(
            self.context.write_settings.storage_format,
            FuseStorageFormat::Parquet
        ) {
            return Err(crate::unsupported_storage_format_error());
        }
        if self.context.schema.fields().is_empty() {
            return Err(ErrorCode::BadArguments(
                "FuseLowLevelBlockWriter requires a non-empty schema",
            ));
        }
        let granule_rows = self.granule_rows();
        if granule_rows == Some(0) {
            return Err(ErrorCode::BadArguments(
                "FuseLowLevelBlockWriter index_granularity must be greater than zero",
            ));
        }
        match (granule_rows, &self.granule_indexes) {
            (Some(_), None) => {
                return Err(ErrorCode::BadArguments(
                    "FuseLowLevelBlockWriter requires granule index configuration when granules are enabled",
                ));
            }
            (None, Some(_)) => {
                return Err(ErrorCode::BadArguments(
                    "FuseLowLevelBlockWriter granule index configuration requires index_granularity",
                ));
            }
            _ => {}
        }
        if let Some(cluster) = &self.cluster_keys {
            if let Some(granule) = &self.granule_indexes
                && granule.mins_location.is_none()
            {
                return Err(ErrorCode::BadArguments(
                    "FuseLowLevelBlockWriter requires a mins location for granule cluster keys",
                ));
            }
            if cluster.keys.fields.is_empty() {
                return Err(ErrorCode::BadArguments(
                    "FuseLowLevelBlockWriter cluster key fields must not be empty",
                ));
            }
        }
        let expected_compression = self.context.write_settings.table_compression.into();
        let granules_enabled = granule_rows.is_some();
        for (_, path) in table_schema_arrow_leaf_paths(&self.context.schema) {
            let path = ColumnPath::from(path);
            let actual = self.writer_properties.compression(&path);
            if actual != expected_compression {
                return Err(ErrorCode::BadArguments(format!(
                    "FuseLowLevelBlockWriter Parquet compression {actual:?} does not match write settings {expected_compression:?}",
                )));
            }
            if granules_enabled && self.writer_properties.dictionary_enabled(&path) {
                return Err(ErrorCode::BadArguments(
                    "FuseLowLevelBlockWriter requires Parquet dictionary encoding to be disabled when granule indexes are enabled",
                ));
            }
        }
        Ok(())
    }
}

/// Completed output. All referenced files have been closed successfully before this is returned.
pub struct FuseLowLevelBlockWriteOutput {
    pub block_meta: BlockMeta,
    pub column_hlls: Option<BlockHLLState>,
    pub column_top_n: Option<BlockTopN>,
    pub draft_virtual_block_meta:
        Option<databend_storages_common_table_meta::meta::DraftVirtualBlockMeta>,
}

struct FuseBlockClusterKeysResult {
    row_count: usize,
    granule_count: usize,
    cluster_stats: ClusterStatistics,
    mins_layout: Option<GranuleIndexFileLayout>,
}

struct FuseBlockDataResult {
    row_count: usize,
    block_size: usize,
    file_size: u64,
    col_stats: StatisticsOfColumns,
    col_metas: HashMap<ColumnId, databend_storages_common_table_meta::meta::ColumnMeta>,
    bloom_index_location: Option<Location>,
    bloom_index_size: u64,
    ngram_index_size: Option<u64>,
    inverted_index_size: Option<u64>,
    vector_index_size: Option<u64>,
    vector_index_location: Option<Location>,
    vector_stats: Option<StatisticsOfVectorColumns>,
    spatial_index_size: Option<u64>,
    spatial_index_location: Option<Location>,
    spatial_stats: Option<StatisticsOfSpatialColumns>,
    draft_virtual_block_meta:
        Option<databend_storages_common_table_meta::meta::DraftVirtualBlockMeta>,
    column_hlls: Option<BlockHLLState>,
    column_top_n: Option<BlockTopN>,
    offsets_layout: Option<GranuleIndexFileLayout>,
}

/// Low-level FUSE block writer driven one logical column at a time.
pub struct FuseLowLevelBlockWriter {
    options: FuseLowLevelBlockWriteOptions,
    cluster_keys_result: Option<FuseBlockClusterKeysResult>,
    data_result: Option<FuseBlockDataResult>,
}

impl FuseLowLevelBlockWriter {
    pub fn create(options: FuseLowLevelBlockWriteOptions) -> Result<Self> {
        options.validate()?;
        Ok(Self {
            options,
            cluster_keys_result: None,
            data_result: None,
        })
    }

    fn write_file(&self, data: impl Into<opendal::Buffer>, location: &Location) -> Result<()> {
        use std::io::Write;

        let mut writer =
            create_blocking_write(self.options.context.operator.clone(), location.0.clone(), 2);
        for chunk in data.into() {
            writer.write_all(&chunk)?;
        }
        writer.close()
    }

    /// Start the optional cluster-key phase. It may be opened once per block.
    pub fn write_cluster_keys(self) -> Result<FuseLowLevelClusterKeyWriter> {
        if self.cluster_keys_result.is_some() {
            return Err(ErrorCode::BadArguments(
                "FuseLowLevelBlockWriter cluster keys have already been written",
            ));
        }
        let cluster = match &self.options.cluster_keys {
            Some(options) => options.keys.clone(),
            None => {
                return Err(ErrorCode::BadArguments(
                    "FuseLowLevelBlockWriter has no cluster-key configuration",
                ));
            }
        };
        let granule_rows = self.options.granule_rows();
        let (granule_mins_location, granule_offsets_location) = match granule_rows {
            Some(_) => {
                let granule = self
                    .options
                    .granule_indexes
                    .as_ref()
                    .expect("granule options validated");
                let (mins, offsets) = granule.locations();
                (mins, Some(offsets))
            }
            None => (None, None),
        };
        Ok(FuseLowLevelClusterKeyWriter {
            parent: self,
            cluster,
            granule_rows,
            granule_mins_location,
            granule_offsets_location,
            total_rows: 0,
            rows_in_granule: 0,
            granule_mins: Vec::new(),
            first_key: None,
            last_key: None,
        })
    }

    /// Start the logical-column data phase. It may be opened once per block.
    pub fn write_data(mut self) -> Result<FuseLowLevelDataWriter> {
        if self.data_result.is_some() {
            return Err(ErrorCode::BadArguments(
                "FuseLowLevelBlockWriter data have already been written",
            ));
        }
        if self.options.cluster_keys.is_some() && self.cluster_keys_result.is_none() {
            return Err(ErrorCode::BadArguments(
                "FuseLowLevelBlockWriter cluster keys must be finished before data",
            ));
        }

        let write_started = Instant::now();
        let operator = self.options.context.operator.clone();
        let schema = self.options.context.schema.clone();
        let write_settings = &self.options.context.write_settings;
        let writer =
            create_blocking_write(operator.clone(), self.options.block_location.0.clone(), 2);
        let arrow_schema: Arc<arrow_schema::Schema> = Arc::new(schema.as_ref().into());
        let parquet = BulkParquetFileWriter::create(
            writer,
            arrow_schema.clone(),
            self.options.writer_properties.clone(),
        )?;

        let top_n = self
            .options
            .top_n
            .as_ref()
            .map(|(columns, size)| (columns, *size));
        let column_sketches =
            BlockColumnSketchesBuilder::new(&self.options.ndv_columns, top_n, None)?;
        let column_stats = ColumnStatisticsState::new(
            &self.options.statistics.stats_columns,
            &self.options.statistics.distinct_columns,
            &write_settings.col_stats_truncate_lens,
        );
        let index_context = BlockIndexLowLevelWriteContext {
            func_ctx: self.options.context.func_ctx.clone(),
            physical_schema: schema.clone(),
            block_location: self.options.block_location.clone(),
            operator: operator.clone(),
            write_settings: self.options.context.write_settings.clone(),
        };
        let block_indexes = std::mem::take(&mut self.options.block_indexes)
            .into_iter()
            .map(|spec| spec.new_low_level_writer(index_context.clone()))
            .collect::<Result<Vec<_>>>()?;

        let granule_indexes = match self.options.granule_rows() {
            Some(rows) => {
                let options = self
                    .options
                    .granule_indexes
                    .as_mut()
                    .expect("granule options validated");
                Some(GranuleIndexesWrite {
                    rows,
                    writers: std::mem::take(&mut options.writers),
                    output: GranuleIndexLowLevelOutput::default(),
                    writer: GranuleIndexFileWriter::new(
                        rows,
                        schema.to_leaf_column_ids(),
                        options.mins_location.clone(),
                        options.offsets_location.clone(),
                    ),
                })
            }
            None => None,
        };
        let virtual_columns = self.options.virtual_columns.take();
        let granule_rows = self.options.granule_rows();
        let fields = schema.fields().to_vec();
        let serialize_hll = self.options.statistics.serialize_hll;
        Ok(FuseLowLevelDataWriter {
            parent: self,
            parquet: Some(parquet),
            write_started,
            arrow_schema,
            schema,
            serialize_hll,
            fields,
            granule_rows,
            next_field: 0,
            row_count: None,
            block_size: 0,
            column_stats,
            column_sketches,
            block_indexes: Some(block_indexes),
            virtual_columns,
            granule_indexes,
        })
    }

    /// Assemble final block metadata after both required phases have completed.
    pub fn finish(mut self) -> Result<FuseLowLevelBlockWriteOutput> {
        let data = self.data_result.take().ok_or_else(|| {
            ErrorCode::BadArguments("FuseLowLevelBlockWriter data phase has not been written")
        })?;

        if let Some(cluster) = &self.cluster_keys_result
            && cluster.row_count != data.row_count
        {
            return Err(ErrorCode::BadArguments(format!(
                "FuseLowLevelBlockWriter cluster-key rows {} != data rows {}",
                cluster.row_count, data.row_count
            )));
        }

        let granule_index = match self.options.granule_rows() {
            Some(rows) => {
                let offsets = data.offsets_layout.ok_or_else(|| {
                    ErrorCode::Internal("FuseLowLevelBlockWriter finished without granule offsets")
                })?;
                let expected = data.row_count.div_ceil(rows);
                if let Some(cluster) = &self.cluster_keys_result
                    && cluster.granule_count != expected
                {
                    return Err(ErrorCode::Internal(format!(
                        "FuseLowLevelBlockWriter cluster granules {} != data granules {expected}",
                        cluster.granule_count
                    )));
                }
                let mins = match &self.cluster_keys_result {
                    Some(cluster) => cluster.mins_layout.clone(),
                    None => None,
                };
                Some(GranuleIndexLayout {
                    granule_rows: u32::try_from(rows).map_err(|_| {
                        ErrorCode::BadArguments(format!(
                            "FuseLowLevelBlockWriter granule rows {rows} exceed metadata limit"
                        ))
                    })?,
                    mins,
                    offsets,
                })
            }
            None => None,
        };

        let cluster_stats = match self.options.cluster_keys {
            Some(options) => match options.stats {
                Some(stats) => Some(stats),
                None => match self.cluster_keys_result {
                    Some(cluster) => Some(cluster.cluster_stats),
                    None => None,
                },
            },
            None => None,
        };
        let draft_virtual_block_meta = data.draft_virtual_block_meta;
        let column_hlls = data.column_hlls;
        let column_top_n = data.column_top_n;
        let block_meta = BlockMeta {
            row_count: data.row_count as u64,
            block_size: data.block_size as u64,
            file_size: data.file_size,
            col_stats: data.col_stats,
            col_metas: data.col_metas,
            cluster_stats,
            location: self.options.block_location,
            bloom_filter_index_location: data.bloom_index_location,
            bloom_filter_index_size: data.bloom_index_size,
            inverted_index_size: data.inverted_index_size,
            ngram_filter_index_size: data.ngram_index_size,
            vector_index_size: data.vector_index_size,
            vector_index_location: data.vector_index_location,
            spatial_index_size: data.spatial_index_size,
            spatial_index_location: data.spatial_index_location,
            spatial_stats: data.spatial_stats,
            granule_index,
            vector_stats: data.vector_stats,
            virtual_block_meta: None,
            compression: Compression::from(self.options.context.write_settings.table_compression),
            create_on: Some(Utc::now()),
        };
        Ok(FuseLowLevelBlockWriteOutput {
            block_meta,
            column_hlls,
            column_top_n,
            draft_virtual_block_meta,
        })
    }
}

/// Consumes ordered cluster-key batches and owns all active cluster-key state.
pub struct FuseLowLevelClusterKeyWriter {
    parent: FuseLowLevelBlockWriter,
    cluster: FuseLowLevelClusterKeyOptions,
    granule_rows: Option<usize>,
    granule_mins_location: Option<Location>,
    granule_offsets_location: Option<Location>,
    total_rows: usize,
    rows_in_granule: usize,
    granule_mins: Vec<Scalar>,
    first_key: Option<Vec<Scalar>>,
    last_key: Option<Vec<Scalar>>,
}

impl FuseLowLevelClusterKeyWriter {
    pub fn write_columns(&mut self, columns: &[Column]) -> Result<()> {
        if columns.len() != self.cluster.fields.len() {
            return Err(ErrorCode::BadArguments(format!(
                "cluster-key column count {} != expected {}",
                columns.len(),
                self.cluster.fields.len()
            )));
        }
        let rows = columns.first().map_or(0, Column::len);
        if rows == 0 {
            return Ok(());
        }
        for (index, (column, expected)) in columns.iter().zip(&self.cluster.fields).enumerate() {
            if column.len() != rows {
                return Err(ErrorCode::BadArguments(format!(
                    "cluster-key column {index} has {} rows, expected {rows}",
                    column.len()
                )));
            }
            if column.data_type() != *expected {
                return Err(ErrorCode::BadArguments(format!(
                    "cluster-key column {index} has type {:?}, expected {expected:?}",
                    column.data_type()
                )));
            }
        }

        if self.first_key.is_none() {
            self.first_key = Some(tuple_at(columns, 0));
        }
        self.last_key = Some(tuple_at(columns, rows - 1));

        if let Some(granule_rows) = self.granule_rows {
            let mut offset = 0;
            while offset < rows {
                if self.rows_in_granule == 0 {
                    self.granule_mins
                        .push(Scalar::Tuple(tuple_at(columns, offset)));
                }
                let take = (granule_rows - self.rows_in_granule).min(rows - offset);
                offset += take;
                self.rows_in_granule += take;
                if self.rows_in_granule == granule_rows {
                    self.rows_in_granule = 0;
                }
            }
        }
        self.total_rows += rows;
        Ok(())
    }

    pub fn finish(mut self) -> Result<FuseLowLevelBlockWriter> {
        if self.total_rows == 0 {
            return Err(ErrorCode::BadArguments(
                "FuseLowLevelClusterKeyWriter cannot finish without rows",
            ));
        }
        let mut parent = self.parent;
        let mins_state = match self.granule_mins_location {
            Some(location) => Some(GranuleIndexFileWriter::serialize_mins(
                &self.granule_mins,
                &self.cluster.fields,
                location,
            )?),
            None => None,
        };
        if self.granule_rows.is_some() && self.granule_offsets_location.is_none() {
            return Err(ErrorCode::Internal(
                "granule offsets location is not configured",
            ));
        }

        let mins_layout = match mins_state {
            Some(state) => {
                let layout = state.layout.clone();
                parent.write_file(state.data, &state.layout.location)?;
                Some(layout)
            }
            None => None,
        };
        let min = self.first_key.take().expect("non-empty cluster keys");
        let max = self.last_key.take().expect("non-empty cluster keys");
        parent.cluster_keys_result = Some(FuseBlockClusterKeysResult {
            row_count: self.total_rows,
            granule_count: self.granule_mins.len(),
            cluster_stats: ClusterStatistics::new(
                self.cluster.cluster_key_id,
                min,
                max,
                self.cluster.level,
            ),
            mins_layout,
        });
        Ok(parent)
    }
}

fn tuple_at(columns: &[Column], row: usize) -> Vec<Scalar> {
    columns
        .iter()
        .map(|column| unsafe { column.index_unchecked(row) }.to_owned())
        .collect()
}

/// Owns the main Parquet writer and advances through logical table columns.
pub struct FuseLowLevelDataWriter {
    parent: FuseLowLevelBlockWriter,
    parquet: Option<BulkParquetFileWriter<OpenDalBlockingWrite>>,
    write_started: Instant,
    arrow_schema: Arc<arrow_schema::Schema>,
    schema: TableSchemaRef,
    serialize_hll: bool,
    fields: Vec<databend_common_expression::TableField>,
    granule_rows: Option<usize>,
    next_field: usize,
    row_count: Option<usize>,
    block_size: usize,
    column_stats: ColumnStatisticsState,
    column_sketches: BlockColumnSketchesBuilder,
    block_indexes: Option<Vec<Box<dyn BlockIndexLowLevelWriter>>>,
    virtual_columns: Option<VirtualColumnsWrite>,
    granule_indexes: Option<GranuleIndexesWrite>,
}

impl FuseLowLevelDataWriter {
    pub fn has_next_column(&self) -> bool {
        self.next_field < self.fields.len()
    }

    pub fn next_column(mut self) -> Result<FuseLowLevelColumnWriter> {
        if !self.has_next_column() {
            return Err(ErrorCode::BadArguments(
                "FuseLowLevelDataWriter has no remaining logical columns",
            ));
        }
        let field_index = self.next_field;
        let field = self.fields[field_index].clone();
        let arrow_field = self.arrow_schema.fields()[field_index].clone();
        let leaf_write = LeafWriteSettings {
            field: arrow_field,
            granule_rows: self.granule_rows,
        };
        let block_indexes = self
            .block_indexes
            .take()
            .expect("data writer owns block index writers")
            .into_iter()
            .map(|writer| writer.next_column())
            .collect::<Result<Vec<_>>>()?;
        let num_leaves = field.data_type().num_leaf_columns();
        let granule_index = match self.granule_indexes.take() {
            Some(indexes) => Some(indexes.next_column()?),
            None => None,
        };
        if num_leaves == 1 {
            let parquet = self
                .parquet
                .take()
                .expect("data writer owns parquet writer");
            let leaf = parquet.next_leaf()?;
            Ok(FuseLowLevelColumnWriter {
                parent: self,
                field,
                leaf_write,
                block_indexes,
                rows: 0,
                size_state: None,
                rows_in_granule: 0,
                granule_index,
                state: FuseBlockColumnState::Streaming(Some(leaf)),
            })
        } else {
            Ok(FuseLowLevelColumnWriter {
                parent: self,
                field,
                leaf_write,
                block_indexes,
                rows: 0,
                size_state: None,
                rows_in_granule: 0,
                granule_index,
                state: FuseBlockColumnState::Buffered(Vec::new()),
            })
        }
    }

    pub fn finish(mut self) -> Result<FuseLowLevelBlockWriter> {
        if self.has_next_column() {
            return Err(ErrorCode::BadArguments(format!(
                "FuseLowLevelDataWriter wrote {} of {} logical columns",
                self.next_field,
                self.fields.len()
            )));
        }
        let row_count = self.row_count.ok_or_else(|| {
            ErrorCode::BadArguments("FuseLowLevelDataWriter cannot finish without rows")
        })?;
        let parquet = self
            .parquet
            .take()
            .expect("data writer owns parquet writer");
        let (metadata, writer) = parquet.finish()?;
        let file_size = writer.bytes_written();
        metrics_inc_block_write_nums(1);
        metrics_inc_block_write_bytes(file_size);
        metrics_inc_block_write_milliseconds(self.write_started.elapsed().as_millis() as u64);

        let mut parent = self.parent;
        let col_metas = column_parquet_metas(&metadata, &self.schema)?;

        let mut block_indexes = WrittenBlockIndexOutput::default();
        for writer in self
            .block_indexes
            .take()
            .expect("data writer owns block index writers")
        {
            block_indexes.merge(writer.finish()?)?;
        }
        let mut distinct = block_indexes
            .bloom
            .as_ref()
            .map(|state| state.column_distinct_count.clone())
            .unwrap_or_default();
        if let Some(state) = &block_indexes.bloom {
            metrics_inc_block_index_write_nums(1);
            metrics_inc_block_index_write_bytes(state.file.size);
            metrics_inc_block_index_write_milliseconds(
                self.write_started.elapsed().as_millis() as u64
            );
        }
        let column_sketches = self.column_sketches.finalize_sketches()?;
        let (block_hlls, column_top_n) = if let Some(sketches) = column_sketches {
            (
                (!sketches.hll.is_empty()).then_some(sketches.hll),
                (!sketches.top_n.is_empty()).then_some(sketches.top_n),
            )
        } else {
            (None, None)
        };
        if let Some(hlls) = &block_hlls {
            for (column_id, hll) in hlls {
                distinct.entry(*column_id).or_insert_with(|| hll.count());
            }
        }
        let col_stats = self.column_stats.finalize(distinct)?;
        let column_hlls = match block_hlls {
            Some(hlls) if self.serialize_hll => {
                Some(BlockHLLState::Serialized(encode_column_hll(&hlls)?))
            }
            Some(hlls) => Some(BlockHLLState::Deserialized(hlls)),
            None => None,
        };

        let mut inverted_index_size = 0;
        for index in &block_indexes.inverted {
            metrics_inc_block_inverted_index_write_nums(1);
            metrics_inc_block_inverted_index_write_bytes(index.file.size);
            metrics_inc_block_inverted_index_write_milliseconds(
                self.write_started.elapsed().as_millis() as u64,
            );
            inverted_index_size += index.file.size;
        }
        let inverted_index_size = (inverted_index_size > 0).then_some(inverted_index_size);

        let draft_virtual_block_meta = match self.virtual_columns.as_mut() {
            Some(writer) => {
                let state = writer.finish()?;
                let meta = state.draft_virtual_block_meta;
                if meta.virtual_column_size > 0 {
                    let write_started = Instant::now();
                    parent.write_file(state.data, &meta.virtual_location)?;
                    metrics_inc_block_virtual_column_write_nums(1);
                    metrics_inc_block_virtual_column_write_bytes(meta.virtual_column_size);
                    metrics_inc_block_virtual_column_write_milliseconds(
                        write_started.elapsed().as_millis() as u64,
                    );
                }
                Some(meta)
            }
            None => None,
        };

        let vector_index = block_indexes.vector;
        let (vector_index_size, vector_index_location, vector_stats) = match vector_index {
            Some(index) => (
                index.file.as_ref().map(|file| file.size),
                index.file.map(|file| file.location),
                index.statistics,
            ),
            None => (None, None, None),
        };

        let spatial_index = block_indexes.spatial;
        let (spatial_index_size, spatial_index_location, spatial_stats) = match spatial_index {
            Some(index) => (
                index.file.as_ref().map(|file| file.size),
                index.file.map(|file| file.location),
                index.statistics,
            ),
            None => (None, None, None),
        };

        let offsets_layout = match self.granule_indexes {
            Some(indexes) => {
                let state = indexes.finish(&metadata, row_count)?;
                let layout = state.layout.clone();
                parent.write_file(state.data, &state.layout.location)?;
                Some(layout)
            }
            None => None,
        };
        let (bloom_index_location, bloom_index_size, ngram_index_size) = match block_indexes.bloom {
            Some(state) => (Some(state.file.location), state.file.size, state.ngram_size),
            None => (None, 0, None),
        };
        parent.data_result = Some(FuseBlockDataResult {
            row_count,
            block_size: self.block_size,
            file_size,
            col_stats,
            col_metas,
            bloom_index_location,
            bloom_index_size,
            ngram_index_size,
            inverted_index_size,
            vector_index_size,
            vector_index_location,
            vector_stats,
            spatial_index_size,
            spatial_index_location,
            spatial_stats,
            draft_virtual_block_meta,
            column_hlls,
            column_top_n,
            offsets_layout,
        });
        Ok(parent)
    }
}

enum ColumnSizeState {
    Constant(usize),
    Additive(usize),
    Boolean(usize),
    Binary {
        rows: usize,
        bytes: usize,
    },
    String {
        rows: usize,
        bytes: usize,
    },
    Offset {
        rows: usize,
        values: Box<ColumnSizeState>,
    },
    Nullable {
        rows: usize,
        any_valid: bool,
        omit_values_if_all_null: bool,
        values: Box<ColumnSizeState>,
    },
    Tuple(Vec<ColumnSizeState>),
}

impl ColumnSizeState {
    fn from_top_level_column(column: &Column) -> Self {
        Self::from_column(column, true)
    }

    fn from_column(column: &Column, omit_values_if_all_null: bool) -> Self {
        match column {
            Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => {
                Self::Constant(std::mem::size_of::<usize>())
            }
            Column::Boolean(column) => Self::Boolean(column.len()),
            Column::Binary(column)
            | Column::Bitmap(column)
            | Column::Variant(column)
            | Column::Geometry(column) => Self::Binary {
                rows: column.len(),
                bytes: column.total_bytes_len(),
            },
            Column::Geography(column) => Self::Binary {
                rows: column.len(),
                bytes: column.0.total_bytes_len(),
            },
            Column::String(column) => Self::String {
                rows: column.len(),
                bytes: column.total_bytes_len(),
            },
            Column::Array(column) | Column::Map(column) => Self::Offset {
                rows: column.len(),
                values: Box::new(Self::from_column(&column.underlying_column(), false)),
            },
            Column::Nullable(column) => Self::Nullable {
                rows: column.len(),
                any_valid: column.validity.true_count() > 0,
                omit_values_if_all_null,
                values: Box::new(Self::from_column(&column.column, false)),
            },
            Column::Tuple(fields) => Self::Tuple(
                fields
                    .iter()
                    .map(|field| Self::from_column(field, false))
                    .collect(),
            ),
            column => Self::Additive(column.memory_size(true)),
        }
    }

    fn add_column(&mut self, column: &Column) -> Result<()> {
        match (self, column) {
            (
                Self::Constant(_),
                Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. },
            ) => {}
            (Self::Additive(size), column) => *size += column.memory_size(true),
            (Self::Boolean(rows), Column::Boolean(column)) => *rows += column.len(),
            (
                Self::Binary { rows, bytes },
                Column::Binary(column)
                | Column::Bitmap(column)
                | Column::Variant(column)
                | Column::Geometry(column),
            ) => {
                *rows += column.len();
                *bytes += column.total_bytes_len();
            }
            (Self::Binary { rows, bytes }, Column::Geography(column)) => {
                *rows += column.len();
                *bytes += column.0.total_bytes_len();
            }
            (Self::String { rows, bytes }, Column::String(column)) => {
                *rows += column.len();
                *bytes += column.total_bytes_len();
            }
            (Self::Offset { rows, values }, Column::Array(column) | Column::Map(column)) => {
                *rows += column.len();
                values.add_column(&column.underlying_column())?;
            }
            (
                Self::Nullable {
                    rows,
                    any_valid,
                    omit_values_if_all_null: _,
                    values,
                },
                Column::Nullable(column),
            ) => {
                *rows += column.len();
                *any_valid |= column.validity.true_count() > 0;
                values.add_column(&column.column)?;
            }
            (Self::Tuple(states), Column::Tuple(fields)) if states.len() == fields.len() => {
                for (state, field) in states.iter_mut().zip(fields) {
                    state.add_column(field)?;
                }
            }
            _ => {
                return Err(ErrorCode::Internal(
                    "column size state does not match fragment type",
                ));
            }
        }
        Ok(())
    }

    fn finish(self) -> usize {
        match self {
            Self::Constant(size) | Self::Additive(size) => size,
            Self::Boolean(rows) => rows.div_ceil(8),
            Self::Binary { rows, bytes } => bytes + (rows + 1) * 8,
            Self::String { rows, bytes } => bytes + rows * 16,
            Self::Offset { rows, values } => values.finish() + (rows + 1) * 8,
            Self::Nullable {
                rows,
                any_valid,
                omit_values_if_all_null,
                values,
            } => {
                let validity_size = rows.div_ceil(8);
                if any_valid || !omit_values_if_all_null {
                    values.finish() + validity_size
                } else {
                    validity_size
                }
            }
            Self::Tuple(fields) => fields.into_iter().map(Self::finish).sum(),
        }
    }
}

enum FuseBlockColumnState {
    Streaming(Option<BulkParquetLeafWriter<OpenDalBlockingWrite>>),
    Buffered(Vec<Column>),
}

/// Writes one logical table column. Single-leaf columns stream; multi-leaf columns buffer only the
/// current logical column and are replayed leaf by leaf at `finish`.
pub struct FuseLowLevelColumnWriter {
    parent: FuseLowLevelDataWriter,
    field: databend_common_expression::TableField,
    leaf_write: LeafWriteSettings,
    block_indexes: Vec<Box<dyn super::block_index::BlockIndexLowLevelColumnWriter>>,
    rows: usize,
    size_state: Option<ColumnSizeState>,
    rows_in_granule: usize,
    granule_index: Option<GranuleIndexesColumnWrite>,
    state: FuseBlockColumnState,
}

impl FuseLowLevelColumnWriter {
    pub fn write(&mut self, column: &Column) -> Result<()> {
        let expected = DataType::from(self.field.data_type());
        if column.data_type() != expected {
            return Err(ErrorCode::BadArguments(format!(
                "column {} has type {:?}, expected {expected:?}",
                self.field.name(),
                column.data_type()
            )));
        }
        if column.len() == 0 {
            return Ok(());
        }
        self.rows += column.len();
        if let Some(size_state) = self.size_state.as_mut() {
            size_state.add_column(column)?;
        } else {
            self.size_state = Some(ColumnSizeState::from_top_level_column(column));
        }

        let parent = &mut self.parent;
        parent.column_stats.add_column(&self.field, column)?;
        parent
            .column_sketches
            .add_column(parent.next_field, column)?;
        for writer in self.block_indexes.iter_mut() {
            writer.write(column)?;
        }
        if let Some(builder) = parent.virtual_columns.as_mut() {
            builder.add_column(parent.next_field, column)?;
        }

        if let Some(writer) = self.granule_index.as_mut() {
            writer.write(column)?;
        }

        match &mut self.state {
            FuseBlockColumnState::Streaming(leaf) => {
                let leaf = leaf.as_mut().expect("streaming leaf is active");
                self.leaf_write
                    .write_single(leaf, column, &mut self.rows_in_granule)?;
            }
            FuseBlockColumnState::Buffered(chunks) => chunks.push(column.clone()),
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<FuseLowLevelDataWriter> {
        if self.rows == 0 {
            return Err(ErrorCode::BadArguments(format!(
                "column {} cannot finish without rows",
                self.field.name()
            )));
        }
        let mut parent = self.parent;
        let mut block_indexes = Vec::with_capacity(self.block_indexes.len());
        for writer in self.block_indexes {
            block_indexes.push(writer.finish()?);
        }
        parent.block_indexes = Some(block_indexes);
        if let Some(writer) = self.granule_index.take() {
            parent.granule_indexes = Some(writer.finish()?);
        }
        match self.state {
            FuseBlockColumnState::Streaming(mut leaf) => {
                parent.parquet = Some(leaf.take().expect("streaming leaf is active").finish()?);
            }
            FuseBlockColumnState::Buffered(chunks) => {
                let column = Column::concat_columns(chunks.into_iter())?;
                let mut parquet = parent
                    .parquet
                    .take()
                    .expect("data writer owns parquet writer");
                let num_leaves = self.field.data_type().num_leaf_columns();
                for leaf_index in 0..num_leaves {
                    let mut leaf = parquet.next_leaf()?;
                    let mut rows_in_granule = 0;
                    self.leaf_write.write_leaf(
                        &mut leaf,
                        &column,
                        leaf_index,
                        &mut rows_in_granule,
                    )?;
                    parquet = leaf.finish()?;
                }
                parent.parquet = Some(parquet);
            }
        }

        if let Some(expected) = parent.row_count {
            if expected != self.rows {
                return Err(ErrorCode::BadArguments(format!(
                    "column {} has {} rows, expected {expected}",
                    self.field.name(),
                    self.rows
                )));
            }
        } else {
            parent.row_count = Some(self.rows);
        }
        parent.block_size += self
            .size_state
            .take()
            .expect("non-empty column has size state")
            .finish();
        parent.next_field += 1;
        Ok(parent)
    }
}

struct LeafWriteSettings {
    field: Arc<arrow_schema::Field>,
    granule_rows: Option<usize>,
}

impl LeafWriteSettings {
    fn write_single(
        &self,
        leaf: &mut BulkParquetLeafWriter<OpenDalBlockingWrite>,
        column: &Column,
        rows_in_granule: &mut usize,
    ) -> Result<()> {
        let mut offset = 0;
        while offset < column.len() {
            let take = self.rows_to_write(*rows_in_granule, column.len() - offset);
            let part = column.slice(offset..offset + take);
            let array: Arc<dyn arrow_array::Array> = (&part).into();
            let leaves = compute_leaves(&self.field, &array)?;
            if leaves.len() != 1 {
                return Err(ErrorCode::Internal(format!(
                    "single-leaf column produced {} parquet leaves",
                    leaves.len()
                )));
            }
            leaf.write(&leaves[0])?;
            offset += take;
            self.finish_fragment(leaf, rows_in_granule, take)?;
        }
        Ok(())
    }

    fn write_leaf(
        &self,
        leaf: &mut BulkParquetLeafWriter<OpenDalBlockingWrite>,
        column: &Column,
        leaf_index: usize,
        rows_in_granule: &mut usize,
    ) -> Result<()> {
        let mut offset = 0;
        while offset < column.len() {
            let take = self.rows_to_write(*rows_in_granule, column.len() - offset);
            let part = column.slice(offset..offset + take);
            let array: Arc<dyn arrow_array::Array> = (&part).into();
            let leaves = compute_leaves(&self.field, &array)?;
            let encoded = leaves.get(leaf_index).ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "logical column produced {} leaves, expected leaf {leaf_index}",
                    leaves.len()
                ))
            })?;
            leaf.write(encoded)?;
            offset += take;
            self.finish_fragment(leaf, rows_in_granule, take)?;
        }
        Ok(())
    }

    fn rows_to_write(&self, rows_in_granule: usize, remaining: usize) -> usize {
        match self.granule_rows {
            Some(rows) => (rows - rows_in_granule).min(remaining),
            None => remaining,
        }
    }

    fn finish_fragment(
        &self,
        leaf: &mut BulkParquetLeafWriter<OpenDalBlockingWrite>,
        rows_in_granule: &mut usize,
        written: usize,
    ) -> Result<()> {
        let Some(rows) = self.granule_rows else {
            return Ok(());
        };
        *rows_in_granule += written;
        if *rows_in_granule == rows {
            leaf.flush_page()?;
            *rows_in_granule = 0;
        }
        Ok(())
    }
}

fn page_layout_from_metadata(
    metadata: &ParquetMetaData,
) -> Result<Vec<databend_storages_common_blocks::LeafPageLayout>> {
    let row_group = metadata.row_groups().first().ok_or_else(|| {
        ErrorCode::ParquetFileInvalid("FuseLowLevelBlockWriter parquet has no row group")
    })?;
    let offsets = metadata.offset_index().ok_or_else(|| {
        ErrorCode::ParquetFileInvalid("FuseLowLevelBlockWriter parquet has no offset index")
    })?;
    let offset_group = offsets.first().ok_or_else(|| {
        ErrorCode::ParquetFileInvalid(
            "FuseLowLevelBlockWriter parquet has no offset-index row group",
        )
    })?;
    if offset_group.len() != row_group.columns().len() {
        return Err(ErrorCode::ParquetFileInvalid(format!(
            "offset-index columns {} != parquet columns {}",
            offset_group.len(),
            row_group.columns().len()
        )));
    }

    row_group
        .columns()
        .iter()
        .zip(offset_group)
        .map(|(chunk, offsets)| {
            let (chunk_offset, chunk_len) = chunk.byte_range();
            let data_offset = u64::try_from(chunk.data_page_offset())
                .map_err(|_| ErrorCode::ParquetFileInvalid("negative parquet data-page offset"))?;
            let dict_page = chunk
                .dictionary_page_offset()
                .map(|offset| {
                    let offset = u64::try_from(offset).map_err(|_| {
                        ErrorCode::ParquetFileInvalid("negative parquet dictionary-page offset")
                    })?;
                    Ok::<(u64, u64), ErrorCode>((offset, data_offset - offset))
                })
                .transpose()?;
            Ok(databend_storages_common_blocks::LeafPageLayout {
                dict_page,
                chunk_end: chunk_offset + chunk_len,
                data_pages: offsets
                    .page_locations()
                    .iter()
                    .map(|page| {
                        Ok(databend_storages_common_blocks::DataPageOffset {
                            first_row_index: u64::try_from(page.first_row_index).map_err(|_| {
                                ErrorCode::ParquetFileInvalid(
                                    "negative parquet page first-row index",
                                )
                            })?,
                            offset: u64::try_from(page.offset).map_err(|_| {
                                ErrorCode::ParquetFileInvalid("negative parquet page offset")
                            })?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use databend_common_base::runtime::GlobalIORuntime;
    use databend_common_expression::DataBlock;
    use databend_common_expression::DataField;
    use databend_common_expression::DataSchema;
    use databend_common_expression::FromData;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::types::AnyType;
    use databend_common_expression::types::ArrayColumn;
    use databend_common_expression::types::BinaryType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::number::Int32Type;
    use databend_common_expression::types::string::StringType;
    use databend_storages_common_blocks::build_parquet_writer_properties;
    use databend_storages_common_io::ReadSettings;
    use databend_storages_common_table_meta::meta::StatisticsOfColumns;
    use databend_storages_common_table_meta::table::TableCompression;
    use opendal::services::Memory;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;
    use crate::io::OffsetsIndex;
    use crate::io::load_granule_mins;

    fn options(operator: Operator, schema: TableSchemaRef) -> FuseLowLevelBlockWriteOptions {
        let properties = Arc::new(build_parquet_writer_properties(
            TableCompression::None,
            false,
            None::<&StatisticsOfColumns>,
            None,
            0,
            schema.as_ref(),
            None,
            None,
        ));
        let stats_columns = schema
            .leaf_fields()
            .iter()
            .map(|field| (field.column_id(), DataType::from(field.data_type())))
            .collect();
        let context = FuseLowLevelWriteContext::new(
            FunctionContext::default(),
            operator,
            schema,
            WriteSettings {
                table_compression: TableCompression::None,
                index_granularity: Some(2),
                ..Default::default()
            },
        );
        let mut options = FuseLowLevelBlockWriteOptions::new(
            context,
            properties,
            ("block.parquet".to_string(), 0),
        );
        options.set_statistics(FuseLowLevelStatisticsOptions::new(
            stats_columns,
            Vec::new(),
            false,
        ));
        options.set_bloom_indexes(FuseLowLevelBloomIndexOptions::new(
            ("bloom.parquet".to_string(), 0),
            BTreeMap::new(),
            Vec::new(),
        ));
        options.set_granule_indexes(FuseLowLevelGranuleIndexOptions::new(
            Some(("mins.parquet".to_string(), 0)),
            ("offsets.parquet".to_string(), 0),
            Vec::new(),
        ));
        options.set_cluster_keys(
            FuseLowLevelClusterKeyOptions::new(
                7,
                vec![DataType::Nullable(Box::new(DataType::Number(
                    NumberDataType::Int32,
                )))],
                0,
            ),
            None,
        );
        options
    }

    #[test]
    fn test_rejects_parquet_compression_mismatch() {
        crate::test_utils::init_test_globals().unwrap();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "value",
            TableDataType::Number(NumberDataType::Int32),
        )]));
        let mut options = options(operator, schema);
        options.context.write_settings.table_compression = TableCompression::Zstd;

        let error = match FuseLowLevelBlockWriter::create(options) {
            Ok(_) => panic!("compression mismatch must be rejected"),
            Err(error) => error,
        };
        assert!(error.message().contains("does not match write settings"));
    }

    #[test]
    fn test_rejects_parquet_dictionary_with_granule_indexes() {
        crate::test_utils::init_test_globals().unwrap();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "value",
            TableDataType::Number(NumberDataType::Int32),
        )]));
        let mut options = options(operator, schema.clone());
        options.writer_properties = Arc::new(build_parquet_writer_properties(
            TableCompression::None,
            true,
            None::<&StatisticsOfColumns>,
            None,
            2,
            schema.as_ref(),
            Some(2),
            None,
        ));

        let error = match FuseLowLevelBlockWriter::create(options) {
            Ok(_) => panic!("dictionary encoding with granule indexes must be rejected"),
            Err(error) => error,
        };
        assert!(error.message().contains("dictionary encoding"));
    }

    #[test]
    fn test_low_level_bloom_index_writes_directly() {
        crate::test_utils::init_test_globals().unwrap();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let field = TableField::new("value", TableDataType::String);
        let schema = Arc::new(TableSchema::new(vec![field.clone()]));
        let mut write_options = options(operator.clone(), schema.clone());
        write_options.context.write_settings.index_granularity = None;
        write_options.granule_indexes = None;
        write_options.cluster_keys = None;
        write_options.set_bloom_indexes(FuseLowLevelBloomIndexOptions::new(
            ("bloom.parquet".to_string(), 0),
            BTreeMap::from([(0, field)]),
            Vec::new(),
        ));

        let writer = FuseLowLevelBlockWriter::create(write_options).unwrap();
        let mut data = writer.write_data().unwrap();
        let mut column = data.next_column().unwrap();
        column
            .write(&StringType::from_data(vec!["one", "two", "three"]))
            .unwrap();
        data = column.finish().unwrap();
        let result = data.finish().unwrap().finish().unwrap();

        assert_eq!(
            result.block_meta.bloom_filter_index_location,
            Some(("bloom.parquet".to_string(), 0))
        );
        assert!(result.block_meta.bloom_filter_index_size > 0);
        let bloom = GlobalIORuntime::instance()
            .block_on(async {
                operator
                    .read("bloom.parquet")
                    .await
                    .map_err(ErrorCode::from)
            })
            .unwrap();
        assert_eq!(
            bloom.len() as u64,
            result.block_meta.bloom_filter_index_size
        );
    }

    #[test]
    fn test_fragmented_tuple_column_roundtrip() {
        crate::test_utils::init_test_globals().unwrap();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "pair",
            TableDataType::Tuple {
                fields_name: vec!["number".to_string(), "text".to_string()],
                fields_type: vec![
                    TableDataType::Number(NumberDataType::Int32),
                    TableDataType::Nullable(Box::new(TableDataType::String)),
                ],
            },
        )]));
        let tuples = Column::Tuple(vec![
            Int32Type::from_data(vec![1, 2, 3, 4, 5]),
            StringType::from_opt_data(vec![
                Some("one"),
                None,
                Some("three"),
                Some("a string longer than the inline view"),
                None,
            ]),
        ]);
        let mut write_options = options(operator.clone(), schema.clone());
        write_options.context.write_settings.index_granularity = None;
        write_options.granule_indexes = None;
        write_options.cluster_keys = None;

        let writer = FuseLowLevelBlockWriter::create(write_options).unwrap();
        let mut data = writer.write_data().unwrap();
        let mut column = data.next_column().unwrap();
        column.write(&tuples.slice(0..2)).unwrap();
        column.write(&tuples.slice(2..5)).unwrap();
        data = column.finish().unwrap();
        let result = data.finish().unwrap().finish().unwrap();

        assert_eq!(result.block_meta.col_metas.len(), 2);
        assert_eq!(
            result.block_meta.block_size,
            DataBlock::new_from_columns(vec![tuples.clone()]).estimate_block_size(1) as u64
        );
        let bytes = GlobalIORuntime::instance()
            .block_on(async {
                operator
                    .read("block.parquet")
                    .await
                    .map_err(ErrorCode::from)
            })
            .unwrap()
            .to_bytes();
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .with_batch_size(usize::MAX)
            .build()
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        let actual = DataBlock::from_record_batch(&DataSchema::from(schema.as_ref()), &batch)
            .unwrap()
            .get_by_offset(0)
            .to_column();
        assert_eq!(actual, tuples);
    }

    #[test]
    fn test_column_writer_finalizes_partial_granule() {
        struct TrackingGranuleLowLevelWriter {
            finalized: Arc<AtomicUsize>,
            remaining_columns: usize,
            granule_rows: usize,
        }

        struct TrackingGranuleLowLevelColumnWriter {
            parent: Option<Box<TrackingGranuleLowLevelWriter>>,
            rows: usize,
        }

        impl GranuleIndexLowLevelColumnWriter for TrackingGranuleLowLevelColumnWriter {
            fn write(&mut self, column: &Column) -> Result<()> {
                self.rows += column.len();
                Ok(())
            }

            fn finish(mut self: Box<Self>) -> Result<Box<dyn GranuleIndexLowLevelWriter>> {
                let parent = self.parent.take().unwrap();
                parent
                    .finalized
                    .fetch_add(self.rows.div_ceil(parent.granule_rows), Ordering::Relaxed);
                Ok(parent)
            }
        }

        impl GranuleIndexLowLevelWriter for TrackingGranuleLowLevelWriter {
            fn next_column(
                mut self: Box<Self>,
            ) -> Result<Box<dyn GranuleIndexLowLevelColumnWriter>> {
                self.remaining_columns -= 1;
                Ok(Box::new(TrackingGranuleLowLevelColumnWriter {
                    parent: Some(self),
                    rows: 0,
                }))
            }

            fn finish(self: Box<Self>) -> Result<GranuleIndexLowLevelOutput> {
                assert_eq!(self.remaining_columns, 0);
                Ok(GranuleIndexLowLevelOutput::default())
            }
        }

        crate::test_utils::init_test_globals().unwrap();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "value",
            TableDataType::Number(NumberDataType::Int32),
        )]));
        let finalized = Arc::new(AtomicUsize::new(0));
        let mut write_options = options(operator, schema);
        write_options.cluster_keys = None;
        write_options.set_granule_indexes(FuseLowLevelGranuleIndexOptions::new(
            None,
            ("offsets.parquet".to_string(), 0),
            vec![Box::new(TrackingGranuleLowLevelWriter {
                finalized: finalized.clone(),
                remaining_columns: 1,
                granule_rows: 2,
            })],
        ));

        let values = Int32Type::from_data(vec![1, 2, 3, 4, 5]);
        let writer = FuseLowLevelBlockWriter::create(write_options).unwrap();
        let mut data = writer.write_data().unwrap();
        let mut column = data.next_column().unwrap();
        column.write(&values.slice(0..1)).unwrap();
        column.write(&values.slice(1..5)).unwrap();
        data = column.finish().unwrap();
        data.finish().unwrap().finish().unwrap();

        assert_eq!(finalized.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn test_column_size_state_is_fragment_independent() {
        fn assert_size(column: Column, split: usize) {
            let expected = DataBlock::new_from_columns(vec![column.clone()]).estimate_block_size(1);
            let mut state = ColumnSizeState::from_top_level_column(&column.slice(0..split));
            state
                .add_column(&column.slice(split..column.len()))
                .unwrap();
            assert_eq!(
                state.finish(),
                expected,
                "column type: {}",
                column.data_type()
            );
        }

        assert_size(
            StringType::from_data(vec![
                "short",
                "a string longer than the inline view",
                "tail",
            ]),
            1,
        );
        assert_size(
            StringType::from_opt_data(vec![
                Some("one"),
                None,
                Some("a string longer than the inline view"),
                Some("four"),
                None,
            ]),
            2,
        );
        assert_size(
            BinaryType::from_data(vec![vec![1, 2], vec![], vec![3, 4, 5], vec![6]]),
            1,
        );
        assert_size(
            Column::Array(Box::new(ArrayColumn::<AnyType>::new(
                Int32Type::from_data(vec![1, 2, 3, 4, 5]),
                vec![0_u64, 2, 2, 3, 5].into(),
            ))),
            2,
        );
        assert_size(
            Column::Tuple(vec![
                Int32Type::from_data(vec![1, 2, 3, 4, 5]),
                StringType::from_opt_data(vec![Some("a"), None, Some("c"), None, Some("e")]),
            ]),
            2,
        );
    }

    #[test]
    fn test_nullable_columns_and_granules_roundtrip() {
        crate::test_utils::init_test_globals().unwrap();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let schema = Arc::new(TableSchema::new(vec![
            TableField::new(
                "key",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::Int32))),
            ),
            TableField::new(
                "text",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
        ]));
        let keys = Int32Type::from_opt_data(vec![Some(1), Some(2), Some(3), Some(5), None]);
        let strings = StringType::from_opt_data(vec![
            Some("one"),
            None,
            Some("three"),
            Some("four"),
            Some("five"),
        ]);

        let writer =
            FuseLowLevelBlockWriter::create(options(operator.clone(), schema.clone())).unwrap();
        let mut cluster = writer.write_cluster_keys().unwrap();
        cluster.write_columns(&[keys.slice(0..3)]).unwrap();
        cluster.write_columns(&[keys.slice(3..5)]).unwrap();
        let writer = cluster.finish().unwrap();

        let mut data = writer.write_data().unwrap();
        let mut key_writer = data.next_column().unwrap();
        key_writer.write(&keys.slice(0..1)).unwrap();
        key_writer.write(&keys.slice(1..5)).unwrap();
        data = key_writer.finish().unwrap();
        let mut string_writer = data.next_column().unwrap();
        string_writer.write(&strings.slice(0..4)).unwrap();
        string_writer.write(&strings.slice(4..5)).unwrap();
        data = string_writer.finish().unwrap();
        let writer = data.finish().unwrap();
        let result = writer.finish().unwrap();

        assert_eq!(result.block_meta.row_count, 5);
        assert_eq!(
            result.block_meta.block_size,
            DataBlock::new_from_columns(vec![keys.clone(), strings.clone()]).estimate_block_size(2)
                as u64
        );
        assert_eq!(
            result.block_meta.cluster_stats.as_ref().unwrap().min.len(),
            1
        );
        let granule = result.block_meta.granule_index.as_ref().unwrap();
        assert_eq!(granule.granule_rows, 2);
        assert!(granule.mins.is_some());

        let bytes = GlobalIORuntime::instance()
            .block_on(async {
                operator
                    .read("block.parquet")
                    .await
                    .map_err(ErrorCode::from)
            })
            .unwrap()
            .to_bytes();
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .with_batch_size(usize::MAX)
            .build()
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        let data_schema = DataSchema::from(schema.as_ref());
        let actual = DataBlock::from_record_batch(&data_schema, &batch).unwrap();
        assert_eq!(actual.get_by_offset(0).to_column(), keys);
        assert_eq!(actual.get_by_offset(1).to_column(), strings);

        let settings = ReadSettings {
            max_gap_size: 48,
            max_range_size: 1024 * 1024,
            parquet_fast_read_bytes: 0,
        };
        let mins = load_granule_mins(
            &operator,
            &settings,
            granule.mins.as_ref().unwrap(),
            &[DataType::Nullable(Box::new(DataType::Number(
                NumberDataType::Int32,
            )))],
            3,
        )
        .unwrap();
        assert_eq!(mins, vec![
            Scalar::Tuple(vec![Scalar::Number(1i32.into())]),
            Scalar::Tuple(vec![Scalar::Number(3i32.into())]),
            Scalar::Tuple(vec![Scalar::Null]),
        ]);
        OffsetsIndex::load(
            &operator,
            &settings,
            &granule.offsets,
            granule.granule_rows as usize,
            result.block_meta.row_count as usize,
            &result.block_meta.col_metas,
        )
        .unwrap();
    }

    #[test]
    fn test_inverted_indexes_can_share_a_fragmented_source_column() {
        crate::test_utils::init_test_globals().unwrap();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "text",
            TableDataType::String,
        )]));
        let index_schema = DataSchema::new(vec![DataField::new("text", DataType::String)]);
        let builders = vec![
            InvertedIndexBuilder {
                name: "first".to_string(),
                version: "v1".to_string(),
                schema: index_schema.clone(),
                options: BTreeMap::new(),
            },
            InvertedIndexBuilder {
                name: "second".to_string(),
                version: "v1".to_string(),
                schema: index_schema,
                options: BTreeMap::new(),
            },
        ];
        let block_location = (
            "root/_b/0123456789abcdef0123456789abcdef_v0.parquet".to_string(),
            0,
        );
        let locations = builders
            .iter()
            .map(|builder| builder.gen_inverted_index_location(&block_location))
            .collect::<Vec<_>>();
        let mut write_options = options(operator.clone(), schema);
        write_options.block_location = block_location;
        write_options.context.write_settings.index_granularity = None;
        write_options.granule_indexes = None;
        write_options.cluster_keys = None;
        write_options.set_inverted_indexes(builders);

        let text = StringType::from_data(vec!["one", "two", "three"]);
        let writer = FuseLowLevelBlockWriter::create(write_options).unwrap();
        let mut data = writer.write_data().unwrap();
        let mut column = data.next_column().unwrap();
        column.write(&text.slice(0..1)).unwrap();
        column.write(&text.slice(1..3)).unwrap();
        data = column.finish().unwrap();
        let result = data.finish().unwrap().finish().unwrap();

        let sizes = GlobalIORuntime::instance()
            .block_on(async {
                let mut sizes = Vec::with_capacity(locations.len());
                for location in &locations {
                    sizes.push(
                        operator
                            .stat(location)
                            .await
                            .map_err(ErrorCode::from)?
                            .content_length(),
                    );
                }
                Ok::<_, ErrorCode>(sizes)
            })
            .unwrap();
        assert!(sizes.iter().all(|size| *size > 0));
        assert_eq!(
            result.block_meta.inverted_index_size,
            Some(sizes.iter().sum())
        );
    }
}
