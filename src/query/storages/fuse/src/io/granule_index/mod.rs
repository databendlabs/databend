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

//! Granule-level skip-index framework: a skip index that narrows within a block at granule
//! granularity, layered on the sparse granule index. On write, each index emits one payload parquet
//! file per indexed column plus per-granule offset columns in the block's `_pidx` sidecar; at prune
//! time the offsets locate each granule's payload page directly.

mod bloom;

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableIndexType;
use databend_storages_common_index::BloomIndexType;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockMeta;
use opendal::Buffer;
use opendal::Operator;

/// One indexed column's payload file: the parquet bytes plus the location they must be written to.
#[derive(Debug)]
pub struct GranuleIndexPayload {
    pub location: String,
    pub data: Buffer,
}

/// Output of a finalized builder for one block: per-column payload files plus the offset columns to
/// append to the `_pidx` sidecar. `sidecar_fields`/`sidecar_columns` are paired; each column has
/// `num_granules` rows and a name chosen by the implementation.
#[derive(Default)]
pub struct GranuleIndexBuildOutput {
    pub payloads: Vec<GranuleIndexPayload>,
    pub sidecar_fields: Vec<TableField>,
    pub sidecar_columns: Vec<Column>,
}

impl GranuleIndexBuildOutput {
    /// Fold another index's output into this one; the offsets sidecar concatenates every index's
    /// columns, and payload files are collected across all indexes.
    pub fn merge(&mut self, other: GranuleIndexBuildOutput) {
        self.payloads.extend(other.payloads);
        self.sidecar_fields.extend(other.sidecar_fields);
        self.sidecar_columns.extend(other.sidecar_columns);
    }
}

/// Builds a granule-level index for one block. Granule boundaries are independent of `push_rows`
/// slice boundaries; the caller must call `finalize_granule` at each granule boundary, matching the
/// sparse granule index.
pub trait GranuleIndexBuilder: Send {
    fn push_rows(&mut self, block: &DataBlock, range: Range<usize>) -> Result<()>;

    /// Seal the current granule (flush its payload page, record its offset) and reset for the next.
    fn finalize_granule(&mut self) -> Result<()>;

    /// Finish the block, returning payload files and sidecar offset columns. `block_location` derives
    /// each payload's path; it is only known at finish on the streaming write path.
    fn finalize(self: Box<Self>, block_location: &str) -> Result<GranuleIndexBuildOutput>;
}

/// Read-side counterpart of [`GranuleIndexBuilder`]. `block_pruner` folds every active pruner over
/// the survivor set without knowing the concrete type.
#[async_trait::async_trait]
pub trait GranuleIndexPruner: Send + Sync {
    /// Narrow `input` (survivor granule runs, or `None` = all granules) for `block_meta`. `None` =
    /// index does not apply (leave `input` unchanged); `Some(ranges)` = narrowed set, `Some(vec![])`
    /// = drop the block. Must degrade to `None` on any load/decode error.
    async fn prune_granules(
        &self,
        block_meta: &BlockMeta,
        input: Option<&[Range<usize>]>,
    ) -> Option<Vec<Range<usize>>>;
}

/// Factory for one granule-level index, resolved from a `TableMeta.indexes` entry. The decoupling
/// seam: write/read paths ask each spec for a builder/pruner without knowing the index kind.
pub trait GranuleIndexSpec: Send + Sync {
    fn new_builder(&self, func_ctx: FunctionContext) -> Result<Box<dyn GranuleIndexBuilder>>;

    /// Per-query pruner, or `None` when this index cannot narrow the given filter.
    fn new_pruner(
        &self,
        func_ctx: FunctionContext,
        schema: &TableSchemaRef,
        filter_expr: Option<&Expr<String>>,
        dal: Operator,
        settings: ReadSettings,
    ) -> Result<Option<Arc<dyn GranuleIndexPruner>>>;
}

/// Resolve every granule-level index in `indexes` into a factory spec — the single place mapping a
/// `TableIndexType` to a concrete implementation. Called by both the write and read paths so one
/// index declaration drives both sides.
pub fn build_granule_index_specs(
    indexes: &BTreeMap<String, TableIndex>,
    schema: &TableSchema,
    bloom_index_type: BloomIndexType,
) -> Result<Vec<Arc<dyn GranuleIndexSpec>>> {
    let mut specs: Vec<Arc<dyn GranuleIndexSpec>> = Vec::new();
    for (name, index) in indexes.iter() {
        // Only bloom is granule-level today; other index types are ignored here.
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
