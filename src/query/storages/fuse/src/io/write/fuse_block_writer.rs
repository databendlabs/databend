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

//! `FuseBlockWriter`: the fuse-layer block serializer shared by the batch (`BlockBuilder`) and
//! streaming (`StreamBlockBuilder`) paths. It owns a `ParquetFileWriter` and drives all granule-level
//! work — page slicing, secondary-index builders (bloom), and cluster-key mins — from one write loop.

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_sql::evaluator::BlockOperator;
use databend_storages_common_blocks::ParquetFileWriter;
use databend_storages_common_blocks::SerializedParquet;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::Location;
use opendal::Buffer;
use parquet::file::properties::WriterPropertiesPtr;

use crate::io::granule_index::GranuleIndexBuildOutput;
use crate::io::granule_index::GranuleIndexBuilder;
use crate::io::write::GranuleIndexState;
use crate::io::write::GranuleIndexWriter;
use crate::operations::column_parquet_metas;

pub(super) struct FuseBlockOutput {
    pub(super) data: Buffer,
    pub(super) col_metas: HashMap<ColumnId, ColumnMeta>,
    pub(super) granule_index_state: Option<GranuleIndexState>,
}

pub(super) struct FuseBlockWriter {
    inner: ParquetFileWriter,
    schema: TableSchemaRef,
    /// `Some(rows)` forces a page boundary every `rows` and builds a granule index; `None` writes
    /// the block in one shot with no granule work.
    granule_rows: Option<usize>,
    /// Rows in the current not-yet-sealed granule, tracked across `write` calls.
    written: usize,
    granule_index_builders: Vec<Box<dyn GranuleIndexBuilder>>,
    /// `Some` only for a clustered sorted write; `None` = offset-only index.
    mins: Option<GranuleMins>,
    leaf_column_ids: Vec<ColumnId>,
    cluster_key_id: Option<u32>,
}

/// Records each granule's cluster-key min. The block is cluster-sorted upstream, so a granule's min
/// is just its first row's key — no aggregation, no overlap check. Expression cluster keys
/// (`CLUSTER BY (a+1)`) are re-derived via `operators`, since the serializer pops the computed
/// columns first.
pub(super) struct GranuleMins {
    cluster_key_index: Vec<usize>,
    operators: Vec<BlockOperator>,
    func_ctx: FunctionContext,
    mins: Vec<Scalar>,
}

impl GranuleMins {
    pub(super) fn new(
        cluster_key_index: Vec<usize>,
        operators: Vec<BlockOperator>,
        func_ctx: FunctionContext,
    ) -> Self {
        Self {
            cluster_key_index,
            operators,
            func_ctx,
            mins: Vec::new(),
        }
    }

    fn add_granule(&mut self, block: &DataBlock, row: usize) -> Result<()> {
        let evaluated = self
            .operators
            .iter()
            .try_fold(block.clone(), |input, op| op.execute(&self.func_ctx, input))?;

        let key = self
            .cluster_key_index
            .iter()
            .map(|&i| evaluated.get_by_offset(i).index(row).unwrap().to_owned())
            .collect::<Vec<_>>();

        self.mins.push(Scalar::Tuple(key));

        Ok(())
    }
}

impl FuseBlockWriter {
    pub(super) fn new(
        props: WriterPropertiesPtr,
        schema: TableSchemaRef,
        granule_rows: Option<usize>,
        granule_index_builders: Vec<Box<dyn GranuleIndexBuilder>>,
        mins: Option<GranuleMins>,
        cluster_key_id: Option<u32>,
    ) -> Self {
        let arrow_schema = Arc::new(schema.as_ref().into());
        let mut inner = ParquetFileWriter::new(arrow_schema, props);
        if granule_rows.is_some() {
            inner.enable_page_layout();
        }
        let leaf_column_ids = schema.to_leaf_column_ids();
        Self {
            inner,
            schema,
            granule_rows,
            written: 0,
            granule_index_builders,
            mins,
            leaf_column_ids,
            cluster_key_id,
        }
    }

    pub(super) fn write(&mut self, block: DataBlock) -> Result<()> {
        let Some(granule_rows) = self.granule_rows else {
            self.inner.write_block(block)?;
            return Ok(());
        };

        let mut offset = 0;
        let num_rows = block.num_rows();

        while offset < num_rows {
            if self.written == 0 {
                if let Some(mins) = self.mins.as_mut() {
                    mins.add_granule(&block, offset)?;
                }
            }

            let take = (granule_rows - self.written).min(num_rows - offset);
            let range = offset..offset + take;

            for b in self.granule_index_builders.iter_mut() {
                b.push_rows(&block, range.clone())?;
            }

            self.inner.write_block(block.slice(range.clone()))?;

            offset += take;
            self.written += take;

            if self.written == granule_rows {
                self.written = 0;

                for b in self.granule_index_builders.iter_mut() {
                    b.finalize_granule()?;
                }

                self.inner.flush_page()?;
            }
        }
        Ok(())
    }

    pub(super) fn compressed_size(&self) -> usize {
        self.inner.compressed_size()
    }

    pub(super) fn finish(
        self,
        mins_location: Location,
        offsets_location: Location,
    ) -> Result<FuseBlockOutput> {
        let granule_rows = self.granule_rows;
        let cluster_key_id = self.cluster_key_id;
        let leaf_column_ids = self.leaf_column_ids;
        let schema = self.schema;

        let granule_mins = self.mins.map(|m| m.mins).unwrap_or_default();

        let mut granule_output = GranuleIndexBuildOutput::default();

        for b in self.granule_index_builders {
            granule_output.merge(b.finalize()?);
        }

        let serialized = self.inner.finish()?;
        let col_metas = column_parquet_metas(&serialized.metadata, &schema)?;
        let data = Buffer::from(serialized.payload);

        let granule_index_state = match serialized.page_layout {
            Some(page_layout) => {
                let writer = GranuleIndexWriter::new(
                    cluster_key_id,
                    granule_rows.expect("granule_rows set when page_layout captured"),
                    leaf_column_ids,
                );

                let state = writer.build_with_extra_columns(
                    &page_layout,
                    &granule_mins,
                    mins_location,
                    offsets_location,
                    granule_output.sidecar_fields,
                    granule_output.sidecar_columns,
                )?;
                Some(state)
            }
            // No page layout means granule indexing was off, so there are no builders and nothing
            // was streamed — no sidecar to build.
            None => None,
        };

        Ok(FuseBlockOutput {
            data,
            col_metas,
            granule_index_state,
        })
    }

    /// Plain (no-granule) finish returning the raw `SerializedParquet`, for callers that need the
    /// parquet footer directly (e.g. virtual columns) or have no sidecar locations to pass `finish`.
    pub(super) fn finish_plain(self) -> Result<SerializedParquet> {
        assert!(
            self.granule_rows.is_none()
                && self.granule_index_builders.is_empty()
                && self.mins.is_none(),
            "finish_plain called on a granule-index writer; use finish"
        );
        self.inner.finish()
    }
}
