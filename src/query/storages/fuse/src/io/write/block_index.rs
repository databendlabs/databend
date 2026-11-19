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

//! Block-index specs provide full-block, column-oriented and optional merge writers.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::AbortChecker;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableSchemaRef;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::RowMapping;
use databend_storages_common_index::SourceRows;
use databend_storages_common_io::BLOCKING_WRITE_MAX_CHUNKS;
use databend_storages_common_io::OpenDalBlockingWrite;
use databend_storages_common_io::create_blocking_write;
use databend_storages_common_table_meta::meta::BlockIndexMeta;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::StatisticsOfSpatialColumns;
use databend_storages_common_table_meta::meta::StatisticsOfVectorColumns;
use opendal::Buffer;
use opendal::Operator;

use super::BloomIndexWriteSpec;
use super::SpatialIndexBuilder;
use super::VectorIndexBuilder;
use super::WriteSettings;
use super::create_inverted_index_builders;
use crate::FuseTable;
use crate::io::TableMetaLocationGenerator;

#[derive(Clone)]
pub struct BlockIndexWriteContext {
    pub func_ctx: FunctionContext,
    pub physical_schema: TableSchemaRef,
    pub block_location: Location,
    pub meta_locations: TableMetaLocationGenerator,
    pub bloom_location: Location,
    pub operator: Operator,
    pub write_settings: WriteSettings,
}

impl BlockIndexWriteContext {
    pub fn create_write(&self, location: &Location) -> OpenDalBlockingWrite {
        create_blocking_write(
            self.operator.clone(),
            location.0.clone(),
            BLOCKING_WRITE_MAX_CHUNKS,
        )
    }
}

#[derive(Debug)]
pub struct PendingIndexFile {
    pub location: Location,
    /// Uploaded during the asynchronous write phase.
    pub data: Buffer,
}

impl PendingIndexFile {
    pub fn size(&self) -> u64 {
        self.data.len() as u64
    }

    pub async fn write(self, operator: &Operator) -> Result<u64> {
        let size = self.size();
        operator.write(&self.location.0, self.data).await?;
        Ok(size)
    }
}

#[derive(Debug)]
pub struct WrittenIndexFile {
    pub location: Location,
    pub size: u64,
}

#[derive(Debug)]
pub struct PendingBloomIndex {
    pub file: PendingIndexFile,
    pub ngram_size: Option<u64>,
    pub column_distinct_count: HashMap<ColumnId, usize>,
}

#[derive(Debug)]
pub struct WrittenBloomIndex {
    pub file: WrittenIndexFile,
    pub ngram_size: Option<u64>,
    pub column_distinct_count: HashMap<ColumnId, usize>,
}

#[derive(Debug)]
pub struct WrittenInvertedIndex {
    pub index_name: String,
    pub index_version: String,
    pub location: Location,
    /// Bundle object only; readers size their tail read from it.
    pub bundle_size: u64,
    /// Bundle plus sibling objects.
    pub total_size: u64,
}

impl WrittenInvertedIndex {
    pub fn to_block_index_meta(&self) -> BlockIndexMeta {
        BlockIndexMeta {
            index_name: self.index_name.clone(),
            location: self.location.clone(),
            size: self.bundle_size,
            index_version: self.index_version.clone(),
        }
    }
}

/// Keep index metadata ordered by name.
pub fn collect_inverted_index_metas(
    metas: impl IntoIterator<Item = BlockIndexMeta>,
) -> Vec<BlockIndexMeta> {
    let mut metas = metas.into_iter().collect::<Vec<_>>();
    metas.sort_unstable_by(|left, right| left.index_name.cmp(&right.index_name));
    metas
}

#[derive(Debug)]
pub struct PendingVectorIndex {
    pub files: Vec<(BlockIndexMeta, PendingIndexFile)>,
    pub statistics: Option<StatisticsOfVectorColumns>,
}

#[derive(Debug)]
pub struct WrittenVectorIndex {
    /// One bundle file per index, already written.
    pub files: Vec<BlockIndexMeta>,
    pub statistics: Option<StatisticsOfVectorColumns>,
}

#[derive(Debug)]
pub struct PendingSpatialIndex {
    pub file: Option<PendingIndexFile>,
    pub statistics: Option<StatisticsOfSpatialColumns>,
}

#[derive(Debug)]
pub struct WrittenSpatialIndex {
    pub file: Option<WrittenIndexFile>,
    pub statistics: Option<StatisticsOfSpatialColumns>,
}

/// Full-block writer output.
#[derive(Debug, Default)]
pub struct PendingBlockIndexOutput {
    pub bloom: Option<PendingBloomIndex>,
    pub inverted: Vec<WrittenInvertedIndex>,
    pub vector: Option<PendingVectorIndex>,
    pub spatial: Option<PendingSpatialIndex>,
}

impl PendingBlockIndexOutput {
    pub fn merge(&mut self, other: Self) -> Result<()> {
        merge_singleton(&mut self.bloom, other.bloom, "pending bloom index")?;
        merge_inverted(&mut self.inverted, other.inverted, "pending inverted index")?;
        merge_singleton(&mut self.vector, other.vector, "pending vector index")?;
        merge_singleton(&mut self.spatial, other.spatial, "pending spatial index")?;
        Ok(())
    }
}

/// Direct-I/O writer and merger output.
#[derive(Debug, Default)]
pub struct WrittenBlockIndexOutput {
    pub bloom: Option<WrittenBloomIndex>,
    pub inverted: Vec<WrittenInvertedIndex>,
    pub vector: Option<WrittenVectorIndex>,
    pub spatial: Option<WrittenSpatialIndex>,
}

impl WrittenBlockIndexOutput {
    pub fn merge(&mut self, other: Self) -> Result<()> {
        merge_singleton(&mut self.bloom, other.bloom, "written bloom index")?;
        merge_inverted(&mut self.inverted, other.inverted, "written inverted index")?;
        merge_singleton(&mut self.vector, other.vector, "written vector index")?;
        merge_singleton(&mut self.spatial, other.spatial, "written spatial index")?;
        Ok(())
    }
}

fn merge_inverted<T>(target: &mut Vec<T>, source: Vec<T>, name: &str) -> Result<()>
where T: InvertedIndexOutput {
    for output in source {
        if target
            .iter()
            .any(|existing| existing.index_name() == output.index_name())
        {
            return Err(ErrorCode::Internal(format!(
                "duplicate {name} output {}",
                output.index_name()
            )));
        }
        target.push(output);
    }
    Ok(())
}

trait InvertedIndexOutput {
    fn index_name(&self) -> &str;
}

impl InvertedIndexOutput for WrittenInvertedIndex {
    fn index_name(&self) -> &str {
        &self.index_name
    }
}

fn merge_singleton<T>(target: &mut Option<T>, source: Option<T>, name: &str) -> Result<()> {
    if let Some(source) = source
        && target.replace(source).is_some()
    {
        return Err(ErrorCode::Internal(format!("duplicate {name} output")));
    }
    Ok(())
}

pub struct BlockIndexMergeSource<'a> {
    pub num_rows: u32,
    pub indexes: &'a [BlockIndexMeta],
}

/// Consecutive rows of one source, in the order they appear in an output.
pub type BlockIndexSourceRows = SourceRows;

/// Default size of a merge partition, in bytes of index data read for it.
pub const BLOCK_INDEX_MERGE_PARTITION_BYTES: u64 = 1 << 20;

/// What a merge needs from the table and the query.
#[derive(Clone)]
pub struct BlockIndexMergeContext {
    pub operator: Operator,
    pub locations: TableMetaLocationGenerator,
    pub abort_checker: AbortChecker,
    /// Work per partition; see [`BLOCK_INDEX_MERGE_PARTITION_BYTES`].
    pub partition_bytes: u64,
}

/// The part of one partition that belongs to `output`. What it holds is the index kind's
/// business; the pipeline only routes it.
pub struct BlockIndexSlice {
    pub output: u32,
    data: Box<dyn Any + Send + Sync>,
}

impl BlockIndexSlice {
    pub fn new<T: Any + Send + Sync>(output: u32, data: T) -> Self {
        Self {
            output,
            data: Box::new(data),
        }
    }

    pub fn downcast<T: Any>(self) -> Result<T> {
        match self.data.downcast::<T>() {
            Ok(data) => Ok(*data),
            Err(_) => Err(ErrorCode::Internal(format!(
                "block index slice is not a {}",
                std::any::type_name::<T>()
            ))),
        }
    }
}

impl fmt::Debug for BlockIndexSlice {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BlockIndexSlice")
            .field("output", &self.output)
            .finish_non_exhaustive()
    }
}

/// A merge of one index across the sources of a recluster task, with its sources open and its
/// work cut into ordered partitions. Partitions are read on several threads at once; each
/// output then replays its slices in partition order into one writer.
pub trait BlockIndexMerger: Send + Sync {
    fn num_partitions(&self) -> usize;

    /// Partition `partition` across every source, its rows mapped to their outputs: one slice
    /// per output, in output order.
    fn partition(&self, mapping: &RowMapping, partition: usize) -> Result<Vec<BlockIndexSlice>>;

    /// The writer merging `output`; it receives the output's slices in partition order.
    fn merge_writer(
        &self,
        mapping: Arc<RowMapping>,
        output: usize,
    ) -> Result<Box<dyn BlockIndexMergeWriter>>;
}

/// Writes one output of one index from its slices, appended in partition order.
pub trait BlockIndexMergeWriter: Send {
    fn append(&mut self, slice: BlockIndexSlice) -> Result<()>;

    /// Writes the index and records it in the output's block metadata.
    fn finish(self: Box<Self>, block: &mut BlockMeta) -> Result<()>;
}

pub fn create_block_index_specs(
    table: &FuseTable,
    schema: TableSchemaRef,
) -> Result<Vec<Arc<dyn BlockIndexSpec>>> {
    let columns = table
        .bloom_index_cols
        .bloom_index_fields(schema.clone(), BloomIndex::supported_type)?;
    let indexes = &table.table_info.meta.indexes;
    let ngram_args = FuseTable::create_ngram_index_args(indexes, &table.schema(), true)?;
    let mut specs: Vec<Arc<dyn BlockIndexSpec>> =
        vec![Arc::new(BloomIndexWriteSpec::new(columns, ngram_args))];
    for builder in create_inverted_index_builders(table)? {
        specs.push(Arc::new(builder.into_write_spec()));
    }
    if let Some(builder) = VectorIndexBuilder::try_create(indexes, schema.clone(), true)? {
        specs.push(Arc::new(builder.into_write_spec()));
    }
    if let Some(builder) = SpatialIndexBuilder::try_create(indexes, schema, true) {
        specs.push(Arc::new(builder.into_write_spec()));
    }
    Ok(specs)
}

pub trait BlockIndexSpec: Send + Sync {
    fn index_name(&self) -> Option<&str> {
        None
    }

    /// Whether this index can be merged from `sources` instead of rebuilt. Metadata only;
    /// called while the pipeline is built.
    fn can_merge(&self, _sources: &[BlockIndexMergeSource<'_>]) -> Result<bool> {
        Ok(false)
    }

    /// Opens `sources` and cuts the merge into partitions. Called while the pipeline runs, for
    /// indexes `can_merge` accepted; an error here must not fall back to rebuilding.
    fn new_merger(
        &self,
        _sources: &[BlockIndexMergeSource<'_>],
        _context: &BlockIndexMergeContext,
    ) -> Result<Arc<dyn BlockIndexMerger>> {
        Err(ErrorCode::Internal("this block index kind does not merge"))
    }

    fn new_writer(&self, context: BlockIndexWriteContext) -> Result<Box<dyn BlockIndexWriter>>;

    fn new_low_level_writer(
        &self,
        context: BlockIndexWriteContext,
    ) -> Result<Box<dyn BlockIndexLowLevelWriter>>;
}

pub trait BlockIndexWriter: Send {
    fn write(&mut self, block: &DataBlock) -> Result<()>;

    fn finish(self: Box<Self>) -> Result<PendingBlockIndexOutput>;
}

/// Column-oriented direct-I/O writer.
pub trait BlockIndexLowLevelWriter: Send {
    fn next_column(self: Box<Self>) -> Result<Box<dyn BlockIndexLowLevelColumnWriter>>;

    fn finish(self: Box<Self>) -> Result<WrittenBlockIndexOutput>;
}

/// Accumulates one column and returns its parent writer on finish.
pub trait BlockIndexLowLevelColumnWriter: Send {
    fn write(&mut self, column: &Column) -> Result<()>;

    fn finish(self: Box<Self>) -> Result<Box<dyn BlockIndexLowLevelWriter>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_non_merge_index_spec_defaults_to_rebuild() {
        use std::collections::BTreeMap;

        use super::super::bloom_index_writer::BloomIndexWriteSpec;

        let spec: Box<dyn BlockIndexSpec> =
            Box::new(BloomIndexWriteSpec::new(BTreeMap::new(), vec![]));
        let sources = [BlockIndexMergeSource {
            num_rows: 10,
            indexes: &[],
        }];
        assert!(!spec.can_merge(&sources).unwrap());
    }

    #[test]
    fn test_outputs_reject_duplicate_inverted_names() {
        let pending = |location: &str| WrittenInvertedIndex {
            index_name: "duplicate".to_string(),
            index_version: "v1".to_string(),
            location: (location.to_string(), 0),
            bundle_size: 0,
            total_size: 0,
        };
        let mut output = PendingBlockIndexOutput {
            inverted: vec![pending("first")],
            ..Default::default()
        };
        let error = output
            .merge(PendingBlockIndexOutput {
                inverted: vec![pending("second")],
                ..Default::default()
            })
            .unwrap_err();
        assert!(error.message().contains("duplicate pending inverted index"));
    }

    #[test]
    fn test_outputs_reject_duplicate_singletons() {
        let vector = || PendingVectorIndex {
            files: vec![(
                BlockIndexMeta {
                    index_name: "vector".to_owned(),
                    index_version: "v1".to_owned(),
                    location: ("location".to_owned(), 1),
                    size: 7,
                },
                PendingIndexFile {
                    location: ("location".to_string(), 1),
                    data: Buffer::from("payload"),
                },
            )],
            statistics: None,
        };
        let mut output = PendingBlockIndexOutput {
            vector: Some(vector()),
            ..Default::default()
        };
        assert_eq!(output.vector.as_ref().unwrap().files[0].1.size(), 7);
        assert!(
            output
                .merge(PendingBlockIndexOutput {
                    vector: Some(vector()),
                    ..Default::default()
                })
                .is_err()
        );

        let written = WrittenBlockIndexOutput {
            vector: Some(WrittenVectorIndex {
                files: vec![BlockIndexMeta {
                    index_name: "vector".to_owned(),
                    index_version: "v1".to_owned(),
                    location: ("written".to_owned(), 1),
                    size: 11,
                }],
                statistics: None,
            }),
            ..Default::default()
        };
        assert_eq!(written.vector.unwrap().files[0].size, 11);
    }
}
