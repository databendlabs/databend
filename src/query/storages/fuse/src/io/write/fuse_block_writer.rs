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

//! Shared Parquet block writer for batch and streaming FUSE writes.

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
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

pub(super) struct GranuleWriteSettings {
    rows: usize,
    builders: Vec<Box<dyn GranuleIndexBuilder>>,
    mins: Option<GranuleMins>,
    cluster_key_id: Option<u32>,
}

impl GranuleWriteSettings {
    pub(super) fn new(
        rows: usize,
        builders: Vec<Box<dyn GranuleIndexBuilder>>,
        mins: Option<GranuleMins>,
        cluster_key_id: Option<u32>,
    ) -> Self {
        Self {
            rows,
            builders,
            mins,
            cluster_key_id,
        }
    }
}

pub(super) struct FuseBlockWriter {
    inner: ParquetFileWriter,
    schema: TableSchemaRef,
    granule: Option<GranuleWriteSettings>,
    total_rows: usize,
    written: usize,
    leaf_column_ids: Vec<ColumnId>,
}

/// Collects the first cluster-key value of each sorted granule.
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
        granule: Option<GranuleWriteSettings>,
    ) -> Self {
        let arrow_schema = Arc::new(schema.as_ref().into());
        let mut inner = ParquetFileWriter::new(arrow_schema, props);
        if granule.is_some() {
            inner.enable_page_layout();
        }
        let leaf_column_ids = schema.to_leaf_column_ids();
        Self {
            inner,
            schema,
            granule,
            total_rows: 0,
            written: 0,
            leaf_column_ids,
        }
    }

    pub(super) fn write(&mut self, block: DataBlock) -> Result<()> {
        let Some(granule) = self.granule.as_mut() else {
            self.inner.write_block(block)?;
            return Ok(());
        };

        let mut offset = 0;
        let num_rows = block.num_rows();
        self.total_rows += num_rows;

        while offset < num_rows {
            if self.written == 0 {
                if let Some(mins) = granule.mins.as_mut() {
                    mins.add_granule(&block, offset)?;
                }
            }

            let take = (granule.rows - self.written).min(num_rows - offset);
            let range = offset..offset + take;

            for builder in granule.builders.iter_mut() {
                builder.push_rows(&block, range.clone())?;
            }

            self.inner.write_block(block.slice(range.clone()))?;

            offset += take;
            self.written += take;

            if self.written == granule.rows {
                self.written = 0;

                for builder in granule.builders.iter_mut() {
                    builder.finalize_granule()?;
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
        mut self,
        mins_location: Location,
        offsets_location: Location,
    ) -> Result<FuseBlockOutput> {
        let granule = self.granule.take();
        let serialized = self.inner.finish()?;
        let granule_index_state = match granule {
            Some(granule) => {
                let num_granules = self.total_rows.div_ceil(granule.rows);
                let granule_mins = granule.mins.map(|mins| mins.mins).unwrap_or_default();
                let mut output = GranuleIndexBuildOutput::default();
                for builder in granule.builders {
                    output.merge(builder.finalize()?)?;
                }

                let page_layout = serialized.page_layout.as_ref().ok_or_else(|| {
                    ErrorCode::Internal(
                        "granule page layout was not captured with granule write settings",
                    )
                })?;
                let writer = GranuleIndexWriter::new(
                    granule.cluster_key_id,
                    granule.rows,
                    self.leaf_column_ids,
                );
                Some(writer.build_with_extra_marks(
                    page_layout,
                    num_granules,
                    &granule_mins,
                    mins_location,
                    offsets_location,
                    output.marks,
                )?)
            }
            None => None,
        };
        let col_metas = column_parquet_metas(&serialized.metadata, &self.schema)?;
        let data = Buffer::from(serialized.payload);

        Ok(FuseBlockOutput {
            data,
            col_metas,
            granule_index_state,
        })
    }

    pub(super) fn finish_plain(self) -> Result<SerializedParquet> {
        assert!(
            self.granule.is_none(),
            "finish_plain called with granule write settings; use finish"
        );
        self.inner.finish()
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::FromData;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::number::Int32Type;
    use databend_storages_common_blocks::build_parquet_writer_properties;
    use databend_storages_common_table_meta::table::TableCompression;

    use super::*;

    #[test]
    fn test_finish_without_granule_settings() {
        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "a",
            TableDataType::Number(NumberDataType::Int32),
        )]));
        let props = Arc::new(build_parquet_writer_properties(
            TableCompression::None,
            false,
            None::<&databend_storages_common_table_meta::meta::StatisticsOfColumns>,
            None,
            3,
            &schema,
            None,
            None,
        ));
        let mut writer = FuseBlockWriter::new(props, schema.clone(), None);
        writer
            .write(DataBlock::new_from_columns(vec![Int32Type::from_data(
                vec![1, 2, 3],
            )]))
            .unwrap();

        let output = writer
            .finish(
                ("unused-mins".to_string(), 0),
                ("unused-offsets".to_string(), 0),
            )
            .unwrap();

        assert!(!output.data.is_empty());
        assert_eq!(output.col_metas.len(), schema.to_leaf_column_ids().len());
        assert!(output.granule_index_state.is_none());
    }
}
