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

use std::collections::HashMap;
use std::io;
use std::ops::Range;
use std::sync::Arc;

use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableSchemaRef;
use databend_storages_common_index::BloomIndex;
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
    pub file: Option<PendingIndexFile>,
    pub statistics: Option<StatisticsOfVectorColumns>,
}

#[derive(Debug)]
pub struct WrittenVectorIndex {
    pub file: Option<WrittenIndexFile>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockIndexSourceRows {
    pub source: u32,
    pub rows: Range<u32>,
}

pub type BlockIndexMergeCheck = Box<dyn Fn() -> io::Result<()> + Send + Sync>;

/// The caller verifies complete lineage; each output batch preserves source row order.
pub struct BlockIndexMergeContext {
    pub operator: Operator,
    pub locations: TableMetaLocationGenerator,
    pub outputs: Vec<Vec<BlockIndexSourceRows>>,
    pub check_interrupt: BlockIndexMergeCheck,
}

/// Prepared merge capability of a block-index spec.
pub trait BlockIndexMerge: Send + Sync {
    fn index_name(&self) -> &str;

    fn merge(&self, context: BlockIndexMergeContext) -> Result<Vec<WrittenBlockIndexOutput>>;

    fn apply_output(&self, block: &mut BlockMeta, output: WrittenBlockIndexOutput) -> Result<()>;
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
    for builder in create_inverted_index_builders(&table.table_info.meta) {
        specs.push(Arc::new(builder.into_write_spec()));
    }
    if let Some(builder) = VectorIndexBuilder::try_create(indexes, schema.clone(), true) {
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

    /// None means rebuild; errors from a selected merge must not fall back to rebuilding.
    fn prepare_merge(
        &self,
        _sources: &[BlockIndexMergeSource<'_>],
    ) -> Result<Option<Arc<dyn BlockIndexMerge>>> {
        Ok(None)
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
        assert!(spec.prepare_merge(&sources).unwrap().is_none());
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
    fn test_default_and_low_level_outputs_have_separate_file_states() {
        let mut pending = PendingBlockIndexOutput {
            vector: Some(PendingVectorIndex {
                file: Some(PendingIndexFile {
                    location: ("pending".to_string(), 0),
                    data: Buffer::from("payload"),
                }),
                statistics: None,
            }),
            ..Default::default()
        };
        assert_eq!(
            pending
                .vector
                .as_ref()
                .unwrap()
                .file
                .as_ref()
                .unwrap()
                .size(),
            7
        );
        assert!(
            pending
                .merge(PendingBlockIndexOutput {
                    vector: Some(PendingVectorIndex {
                        file: Some(PendingIndexFile {
                            location: ("duplicate".to_string(), 0),
                            data: Buffer::new(),
                        }),
                        statistics: None,
                    }),
                    ..Default::default()
                })
                .is_err()
        );

        let written = WrittenBlockIndexOutput {
            vector: Some(WrittenVectorIndex {
                file: Some(WrittenIndexFile {
                    location: ("written".to_string(), 0),
                    size: 11,
                }),
                statistics: None,
            }),
            ..Default::default()
        };
        assert_eq!(written.vector.unwrap().file.unwrap().size, 11);
    }
}
