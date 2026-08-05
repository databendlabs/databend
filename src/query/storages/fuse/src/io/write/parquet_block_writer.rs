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

//! Shared Parquet block writer for complete `DataBlock` FUSE writes.

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_storages_common_blocks::ParquetFileWriter;
use databend_storages_common_blocks::SerializedParquet;
use databend_storages_common_table_meta::meta::ColumnMeta;
use opendal::Buffer;
use parquet::file::properties::WriterPropertiesPtr;

use crate::io::granule_index::GranuleIndexWriter;
use crate::io::granule_index::PendingGranuleIndexOutput;
use crate::io::write::GranuleIndexFileWriter;
use crate::io::write::GranuleIndexState;
use crate::operations::column_parquet_metas;

pub(super) struct ParquetBlockOutput {
    pub(super) data: Buffer,
    pub(super) col_metas: HashMap<ColumnId, ColumnMeta>,
    pub(super) granule_index: Option<GranuleIndexState>,
    pub(super) granule_payloads: Vec<crate::io::granule_index::PendingGranuleIndexPayload>,
}

pub(super) struct GranuleWriteSettings {
    rows: usize,
    writers: Vec<Box<dyn GranuleIndexWriter>>,
    mins: Option<(
        Vec<databend_common_expression::Column>,
        databend_storages_common_table_meta::meta::Location,
    )>,
    offsets_location: databend_storages_common_table_meta::meta::Location,
}

impl GranuleWriteSettings {
    pub(super) fn new(
        rows: usize,
        writers: Vec<Box<dyn GranuleIndexWriter>>,
        mins: Option<(
            Vec<databend_common_expression::Column>,
            databend_storages_common_table_meta::meta::Location,
        )>,
        offsets_location: databend_storages_common_table_meta::meta::Location,
    ) -> Self {
        Self {
            rows,
            writers,
            mins,
            offsets_location,
        }
    }
}

pub(super) struct ParquetBlockWriter {
    inner: ParquetFileWriter,
    schema: TableSchemaRef,
    granule: Option<GranuleWriteSettings>,
    total_rows: usize,
    written: usize,
    leaf_column_ids: Vec<ColumnId>,
}

impl ParquetBlockWriter {
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
            let take = (granule.rows - self.written).min(num_rows - offset);
            let range = offset..offset + take;

            self.inner.write_block(block.slice(range.clone()))?;
            for writer in &mut granule.writers {
                writer.write(&block, range.clone())?;
            }

            offset += take;
            self.written += take;

            if self.written == granule.rows {
                self.written = 0;

                self.inner.flush_page()?;
                for writer in &mut granule.writers {
                    writer.finish_granule()?;
                }
            }
        }
        Ok(())
    }

    pub(super) fn compressed_size(&self) -> usize {
        self.inner.compressed_size()
    }

    pub(super) fn finish(mut self) -> Result<ParquetBlockOutput> {
        let granule = self.granule.take();
        let serialized = self.inner.finish()?;
        let granule_index = match granule {
            Some(granule) => {
                let num_granules = self.total_rows.div_ceil(granule.rows);
                let mut output = PendingGranuleIndexOutput::default();
                for writer in granule.writers {
                    output.merge(writer.finish()?)?;
                }

                let page_layout = serialized.page_layout.as_ref().ok_or_else(|| {
                    ErrorCode::Internal(
                        "granule page layout was not captured with granule write settings",
                    )
                })?;
                let (mins, mins_location) = match granule.mins {
                    Some((columns, location)) => (Some(columns), Some(location)),
                    None => (None, None),
                };
                let writer = GranuleIndexFileWriter::new(
                    granule.rows,
                    self.leaf_column_ids,
                    mins_location,
                    granule.offsets_location,
                );
                let state =
                    writer.build_with_extra_marks(page_layout, num_granules, mins, output.marks)?;
                (Some(state), output.pending_payloads)
            }
            None => (None, Vec::new()),
        };
        let col_metas = column_parquet_metas(&serialized.metadata, &self.schema)?;
        let data = Buffer::from(serialized.payload);

        Ok(ParquetBlockOutput {
            data,
            col_metas,
            granule_index: granule_index.0,
            granule_payloads: granule_index.1,
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
    use databend_storages_common_io::BufferReader;
    use databend_storages_common_table_meta::table::TableCompression;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

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
        let mut writer = ParquetBlockWriter::new(props, schema.clone(), None);
        writer
            .write(DataBlock::new_from_columns(vec![Int32Type::from_data(
                vec![1, 2, 3],
            )]))
            .unwrap();

        let output = writer.finish().unwrap();

        assert!(!output.data.is_empty());
        assert_eq!(output.col_metas.len(), schema.to_leaf_column_ids().len());
        assert!(output.granule_index.is_none());
    }

    #[test]
    fn test_finish_with_granule_settings_disables_dictionary() {
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
            Some(2),
            None,
        ));
        let granule =
            GranuleWriteSettings::new(2, Vec::new(), None, ("offsets.parquet".to_string(), 0));
        let mut writer = ParquetBlockWriter::new(props, schema, Some(granule));
        writer
            .write(DataBlock::new_from_columns(vec![Int32Type::from_data(
                vec![1, 2, 3],
            )]))
            .unwrap();

        let output = writer.finish().unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(BufferReader(output.data)).unwrap();
        let encodings = builder
            .metadata()
            .row_group(0)
            .column(0)
            .encodings()
            .collect::<Vec<_>>();
        assert!(!encodings.contains(&parquet::basic::Encoding::RLE_DICTIONARY));
        assert!(!encodings.contains(&parquet::basic::Encoding::PLAIN_DICTIONARY));
    }
}
