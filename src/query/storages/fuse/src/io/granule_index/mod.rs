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

//! Granule-level indexes narrow a block's sparse-index survivor ranges before data-page reads.

mod bloom;

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::UInt64Type;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableIndexType;
use databend_storages_common_index::BloomIndexType;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::Location;
use opendal::Buffer;
use opendal::Operator;

use crate::io::GranulePruningReadContext;

pub struct GranuleMark {
    pub field: TableField,
    pub values: Column,
}

impl GranuleMark {
    pub fn create(name: &str, values: Vec<u64>) -> Self {
        Self {
            field: TableField::new(name, TableDataType::Number(NumberDataType::UInt64)),
            values: UInt64Type::from_data(values),
        }
    }
}

#[derive(Debug)]
pub struct PendingGranuleIndexPayload {
    pub location: Location,
    pub data: Buffer,
}

#[derive(Default)]
pub struct PendingGranuleIndexOutput {
    pub marks: Vec<GranuleMark>,
    pub pending_payloads: Vec<PendingGranuleIndexPayload>,
}

impl PendingGranuleIndexOutput {
    pub fn merge(&mut self, other: PendingGranuleIndexOutput) -> Result<()> {
        merge_marks(&mut self.marks, other.marks)?;
        self.pending_payloads.extend(other.pending_payloads);
        Ok(())
    }
}

#[derive(Default)]
pub struct GranuleIndexLowLevelOutput {
    pub marks: Vec<GranuleMark>,
}

impl GranuleIndexLowLevelOutput {
    pub fn merge(&mut self, other: GranuleIndexLowLevelOutput) -> Result<()> {
        merge_marks(&mut self.marks, other.marks)
    }
}

fn merge_marks(target: &mut Vec<GranuleMark>, marks: Vec<GranuleMark>) -> Result<()> {
    for mark in marks {
        if target
            .iter()
            .any(|existing| existing.field.name() == mark.field.name())
        {
            return Err(ErrorCode::Internal(format!(
                "duplicate granule mark {}",
                mark.field.name()
            )));
        }
        target.push(mark);
    }
    Ok(())
}

/// Granule-index writer driven by complete `DataBlock` inputs. The caller seals granules at
/// exactly the same boundaries as the main Parquet writer.
pub trait GranuleIndexWriter: Send {
    fn write(&mut self, block: &DataBlock, range: Range<usize>) -> Result<()>;

    fn finish_granule(&mut self) -> Result<()>;

    fn finish(self: Box<Self>) -> Result<PendingGranuleIndexOutput>;
}

/// Writes one logical column of a low-level granule-index writer. It temporarily owns its parent
/// writer and returns that parent after committing the completed column state internally.
pub trait GranuleIndexLowLevelColumnWriter: Send {
    fn write(&mut self, column: &Column) -> Result<()>;

    fn finish(self: Box<Self>) -> Result<Box<dyn GranuleIndexLowLevelWriter>>;
}

/// Low-level granule-index writer. Opening a column consumes the parent, which makes it impossible
/// to open two columns or finish the index while a column is active.
pub trait GranuleIndexLowLevelWriter: Send {
    fn next_column(self: Box<Self>) -> Result<Box<dyn GranuleIndexLowLevelColumnWriter>>;

    fn finish(self: Box<Self>) -> Result<GranuleIndexLowLevelOutput>;
}

pub(super) struct NoopGranuleIndexWriter;

impl GranuleIndexWriter for NoopGranuleIndexWriter {
    fn write(&mut self, _block: &DataBlock, _range: Range<usize>) -> Result<()> {
        Ok(())
    }

    fn finish_granule(&mut self) -> Result<()> {
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<PendingGranuleIndexOutput> {
        Ok(PendingGranuleIndexOutput::default())
    }
}

pub(super) struct NoopGranuleIndexLowLevelColumnWriter {
    parent: Option<Box<dyn GranuleIndexLowLevelWriter>>,
}

impl NoopGranuleIndexLowLevelColumnWriter {
    fn new(parent: Box<dyn GranuleIndexLowLevelWriter>) -> Self {
        Self {
            parent: Some(parent),
        }
    }
}

impl GranuleIndexLowLevelColumnWriter for NoopGranuleIndexLowLevelColumnWriter {
    fn write(&mut self, _column: &Column) -> Result<()> {
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<Box<dyn GranuleIndexLowLevelWriter>> {
        self.parent
            .take()
            .ok_or_else(|| ErrorCode::Internal("no-op granule index column writer has no parent"))
    }
}

pub(super) struct NoopGranuleIndexLowLevelWriter {
    remaining_columns: usize,
}

impl NoopGranuleIndexLowLevelWriter {
    pub(super) fn new(columns: usize) -> Self {
        Self {
            remaining_columns: columns,
        }
    }
}

impl GranuleIndexLowLevelWriter for NoopGranuleIndexLowLevelWriter {
    fn next_column(mut self: Box<Self>) -> Result<Box<dyn GranuleIndexLowLevelColumnWriter>> {
        if self.remaining_columns == 0 {
            return Err(ErrorCode::Internal(
                "granule index low-level writer has no remaining columns",
            ));
        }
        self.remaining_columns -= 1;
        Ok(Box::new(NoopGranuleIndexLowLevelColumnWriter::new(self)))
    }

    fn finish(self: Box<Self>) -> Result<GranuleIndexLowLevelOutput> {
        if self.remaining_columns != 0 {
            return Err(ErrorCode::Internal(format!(
                "granule index low-level writer has {} unconsumed columns",
                self.remaining_columns
            )));
        }
        Ok(GranuleIndexLowLevelOutput::default())
    }
}

pub const GRANULE_BLOOM_INDEX_NAME: &str = "bloom";

pub trait GranuleIndexPruner: Send + Sync {
    fn name(&self) -> &'static str;

    fn required_marks(&self) -> Vec<String>;

    fn prune_granules(
        &self,
        block_meta: &BlockMeta,
        input: &[Range<usize>],
        read_ctx: &GranulePruningReadContext,
    ) -> Result<Vec<Range<usize>>>;
}

/// Factory shared by granule-index write and read paths.
pub trait GranuleIndexSpec: Send + Sync {
    /// Default writers bind stable IDs and return compressed pending payloads. Low-level writers
    /// additionally bind lazy blocking outputs and write payloads directly.
    fn new_writer(
        &self,
        func_ctx: FunctionContext,
        physical_schema: &TableSchema,
        block_location: &str,
    ) -> Result<Box<dyn GranuleIndexWriter>>;

    fn new_low_level_writer(
        &self,
        func_ctx: FunctionContext,
        physical_schema: &TableSchema,
        block_location: &str,
        dal: Operator,
        granule_rows: usize,
    ) -> Result<Box<dyn GranuleIndexLowLevelWriter>>;

    fn new_pruner(
        &self,
        func_ctx: FunctionContext,
        schema: &TableSchemaRef,
        filter_expr: Option<&Expr<String>>,
        dal: Operator,
        settings: ReadSettings,
    ) -> Result<Option<Arc<dyn GranuleIndexPruner>>>;
}

pub fn build_granule_index_specs(
    indexes: &BTreeMap<String, TableIndex>,
    schema: &TableSchema,
    bloom_index_type: BloomIndexType,
) -> Result<Vec<Arc<dyn GranuleIndexSpec>>> {
    let mut specs: Vec<Arc<dyn GranuleIndexSpec>> = Vec::new();
    for (name, index) in indexes.iter() {
        if index.index_type == TableIndexType::Bloom {
            if let Some(spec) =
                bloom::BloomGranuleIndexSpec::try_create(name, index, schema, bloom_index_type)?
            {
                specs.push(Arc::new(spec));
            }
        }
    }
    Ok(specs)
}
