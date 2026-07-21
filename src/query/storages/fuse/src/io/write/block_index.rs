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

//! Write protocols for block-level indexes produced with a FUSE data block.
//!
//! The default and low-level protocols are intentionally separate. They share immutable index
//! specifications and index-specific construction algorithms, but not mutable state, serialization
//! orchestration, payload ownership, or finish output types.
//!
//! A spec is immutable configuration. `new_writer` creates a writer that consumes complete
//! `DataBlock`s and retains serialized payloads for later upload. `new_low_level_writer` creates an
//! independent writer that consumes logical columns in order and writes payloads directly. Neither
//! implementation adapts or emulates the other.

use std::collections::HashMap;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableSchemaRef;
use databend_storages_common_io::OpenDalBlockingWrite;
use databend_storages_common_io::create_blocking_write;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::StatisticsOfSpatialColumns;
use databend_storages_common_table_meta::meta::StatisticsOfVectorColumns;
use opendal::Buffer;
use opendal::Operator;

use super::WriteSettings;

#[derive(Clone)]
pub struct BlockIndexWriteContext {
    pub func_ctx: FunctionContext,
    pub physical_schema: TableSchemaRef,
    pub block_location: Location,
    pub write_settings: WriteSettings,
}

#[derive(Clone)]
pub struct BlockIndexLowLevelWriteContext {
    pub func_ctx: FunctionContext,
    pub physical_schema: TableSchemaRef,
    pub block_location: Location,
    pub operator: Operator,
    pub write_settings: WriteSettings,
}

impl BlockIndexLowLevelWriteContext {
    /// Low-level component writers create their lazy blocking outputs at construction time.
    pub fn create_write(&self, location: &Location) -> OpenDalBlockingWrite {
        create_blocking_write(self.operator.clone(), location.0.clone(), 2)
    }
}

#[derive(Debug)]
pub struct PendingIndexFile {
    /// Final object location; the payload is not uploaded until the asynchronous write-down phase.
    pub location: Location,
    /// Serialized in-memory payload owned exclusively by the default writer.
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
    /// Closed object location produced by a low-level writer.
    pub location: Location,
    /// Bytes written through the blocking output; no serialized payload is retained.
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
pub struct PendingInvertedIndex {
    pub index_name: String,
    pub file: PendingIndexFile,
}

#[derive(Debug)]
pub struct WrittenInvertedIndex {
    pub index_name: String,
    pub file: WrittenIndexFile,
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

/// Union-all pending output produced only by writers that consume complete `DataBlock`s.
#[derive(Debug, Default)]
pub struct PendingBlockIndexOutput {
    pub bloom: Option<PendingBloomIndex>,
    pub inverted: Vec<PendingInvertedIndex>,
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

/// Union-all written output produced only by low-level direct-I/O writers.
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

impl InvertedIndexOutput for PendingInvertedIndex {
    fn index_name(&self) -> &str {
        &self.index_name
    }
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

pub trait BlockIndexSpec: Send + Sync {
    fn new_writer(&self, context: BlockIndexWriteContext) -> Result<Box<dyn BlockIndexWriter>>;

    fn new_low_level_writer(
        &self,
        context: BlockIndexLowLevelWriteContext,
    ) -> Result<Box<dyn BlockIndexLowLevelWriter>>;
}

pub trait BlockIndexWriter: Send {
    fn write(&mut self, block: &DataBlock) -> Result<()>;

    fn finish(self: Box<Self>) -> Result<PendingBlockIndexOutput>;
}

/// A column-oriented direct-I/O index writer. It retains only input required by its algorithm.
pub trait BlockIndexLowLevelWriter: Send {
    fn next_column(self: Box<Self>) -> Result<Box<dyn BlockIndexLowLevelColumnWriter>>;

    fn finish(self: Box<Self>) -> Result<WrittenBlockIndexOutput>;
}

/// Temporary state for one logical physical-schema column. It returns the concrete parent; it does
/// not expose a component output to the FUSE coordinator.
pub trait BlockIndexLowLevelColumnWriter: Send {
    fn write(&mut self, column: &Column) -> Result<()>;

    fn finish(self: Box<Self>) -> Result<Box<dyn BlockIndexLowLevelWriter>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_outputs_reject_duplicate_inverted_names() {
        let pending = |location: &str| PendingInvertedIndex {
            index_name: "duplicate".to_string(),
            file: PendingIndexFile {
                location: (location.to_string(), 0),
                data: Buffer::new(),
            },
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
