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

use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_sources::AsyncSource;
use common_pipeline_sources::AsyncSourcer;
use common_storages_parquet::ParquetPart;
use common_storages_parquet::ParquetRSReader;

use crate::partition::IcebergPartInfo;

pub struct IcebergTableSource {
    ctx: Arc<dyn TableContext>,
    output_schema: DataSchemaRef,

    /// The reader to read [`DataBlock`]s from parquet files.
    parquet_reader: Arc<ParquetRSReader>,
}

impl IcebergTableSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        output_schema: DataSchemaRef,
        parquet_reader: Arc<ParquetRSReader>,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.clone(), output, IcebergTableSource {
            ctx,
            output_schema,
            parquet_reader,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for IcebergTableSource {
    const NAME: &'static str = "IcebergSource";

    #[async_trait::unboxed_simple]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if let Some(part) = self.ctx.get_partition() {
            let iceberg_part = IcebergPartInfo::from_part(&part)?;
            let block = match iceberg_part {
                IcebergPartInfo::Parquet(ParquetPart::ParquetRSFile(parquet_part)) => {
                    self.parquet_reader
                        .read_block(self.ctx.clone(), &parquet_part.location)
                        .await?
                }
                _ => unreachable!(),
            };
            let block = check_block_schema(&self.output_schema, block)?;
            Ok(Some(block))
        } else {
            // No more partition, finish this source.
            Ok(None)
        }
    }
}

fn check_block_schema(schema: &DataSchema, mut block: DataBlock) -> Result<DataBlock> {
    // Check if the schema of the data block is matched with the schema of the table.
    if block.num_columns() != schema.num_fields() {
        return Err(ErrorCode::TableSchemaMismatch(format!(
            "Data schema mismatched. Data columns length: {}, schema fields length: {}",
            block.num_columns(),
            schema.num_fields()
        )));
    }

    for (col, field) in block.columns_mut().iter_mut().zip(schema.fields().iter()) {
        // If the actual data is nullable, the field must be nullbale.
        if col.data_type.is_nullable_or_null() && !field.is_nullable() {
            return Err(ErrorCode::TableSchemaMismatch(format!(
                "Data schema mismatched (col name: {}). Data column is nullable, but schema field is not nullable",
                field.name()
            )));
        }
        // The inner type of the data and field should be the same.
        let data_type = col.data_type.remove_nullable();
        let schema_type = field.data_type().remove_nullable();
        if data_type != schema_type {
            return Err(ErrorCode::TableSchemaMismatch(format!(
                "Data schema mismatched (col name: {}). Data column type is {:?}, but schema field type is {:?}",
                field.name(),
                col.data_type,
                field.data_type()
            )));
        }
        // If the field is nullable but the actual data is not nullable,
        // we should wrap nullable for the data.
        if field.is_nullable() && !col.data_type.is_nullable_or_null() {
            col.data_type = col.data_type.wrap_nullable();
            col.value = col.value.clone().wrap_nullable(None);
        }
    }

    Ok(block)
}
