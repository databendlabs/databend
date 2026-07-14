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

#[derive(Default)]
pub struct GranuleIndexBuildOutput {
    pub marks: Vec<GranuleMark>,
}

impl GranuleIndexBuildOutput {
    pub fn merge(&mut self, other: GranuleIndexBuildOutput) -> Result<()> {
        for mark in other.marks {
            if self
                .marks
                .iter()
                .any(|existing| existing.field.name() == mark.field.name())
            {
                return Err(ErrorCode::Internal(format!(
                    "duplicate granule mark {}",
                    mark.field.name()
                )));
            }
            self.marks.push(mark);
        }
        Ok(())
    }
}

/// The caller must finalize each granule boundary independently of `push_rows` slice boundaries.
pub trait GranuleIndexBuilder: Send {
    fn push_rows(&mut self, block: &DataBlock, range: Range<usize>) -> Result<()>;

    fn finalize_granule(&mut self) -> Result<()>;

    fn finalize(self: Box<Self>) -> Result<GranuleIndexBuildOutput>;
}

pub(super) struct NoopGranuleIndexBuilder;

impl GranuleIndexBuilder for NoopGranuleIndexBuilder {
    fn push_rows(&mut self, _block: &DataBlock, _range: Range<usize>) -> Result<()> {
        Ok(())
    }

    fn finalize_granule(&mut self) -> Result<()> {
        Ok(())
    }

    fn finalize(self: Box<Self>) -> Result<GranuleIndexBuildOutput> {
        Ok(GranuleIndexBuildOutput::default())
    }
}

pub const GRANULE_BLOOM_INDEX_NAME: &str = "bloom";

#[async_trait::async_trait]
pub trait GranuleIndexPruner: Send + Sync {
    fn name(&self) -> &'static str;

    fn required_marks(&self) -> Vec<String>;

    async fn prune_granules(
        &self,
        block_meta: &BlockMeta,
        input: &[Range<usize>],
        read_ctx: &GranulePruningReadContext,
    ) -> Result<Vec<Range<usize>>>;
}

/// Factory shared by granule-index write and read paths.
pub trait GranuleIndexSpec: Send + Sync {
    /// Builders bind stable IDs against the physical write schema. Payload locations must be known
    /// at construction because builders stream data before `finalize`.
    fn new_builder(
        &self,
        func_ctx: FunctionContext,
        physical_schema: &TableSchema,
        block_location: &str,
        dal: Operator,
    ) -> Result<Box<dyn GranuleIndexBuilder>>;

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
