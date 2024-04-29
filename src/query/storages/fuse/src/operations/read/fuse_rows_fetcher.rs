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

use databend_common_arrow::parquet::metadata::ColumnDescriptor;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::Value;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_pipeline_transforms::processors::AsyncTransformer;

use super::native_rows_fetcher::NativeRowsFetcher;
use super::parquet_rows_fetcher::ParquetRowsFetcher;
use crate::io::ReadSettings;
use crate::FuseStorageFormat;
use crate::FuseTable;

pub fn build_row_fetcher_pipeline(
    ctx: Arc<dyn TableContext>,
    pipeline: &mut Pipeline,
    row_id_col_offset: usize,
    source: &DataSourcePlan,
    projection: Projection,
) -> Result<()> {
    let table = ctx.build_table_from_source_plan(source)?;
    let fuse_table = table
        .as_any()
        .downcast_ref::<FuseTable>()
        .ok_or_else(|| ErrorCode::Internal("Row fetcher is only supported by Fuse engine"))?
        .to_owned();
    let fuse_table = Arc::new(fuse_table);
    let block_reader =
        fuse_table.create_block_reader(ctx.clone(), projection.clone(), false, false, true)?;
    let max_threads = ctx.get_settings().get_max_threads()? as usize;

    match &fuse_table.storage_format {
        FuseStorageFormat::Native => {
            let mut column_leaves = Vec::with_capacity(block_reader.project_column_nodes.len());
            for column_node in &block_reader.project_column_nodes {
                let leaves: Vec<ColumnDescriptor> = column_node
                    .leaf_indices
                    .iter()
                    .map(|i| block_reader.parquet_schema_descriptor.columns()[*i].clone())
                    .collect::<Vec<_>>();
                column_leaves.push(leaves);
            }
            let column_leaves = Arc::new(column_leaves);
            pipeline.add_transform(|input, output| {
                Ok(if block_reader.support_blocking_api() {
                    TransformRowsFetcher::create(
                        input,
                        output,
                        row_id_col_offset,
                        NativeRowsFetcher::<true>::create(
                            fuse_table.clone(),
                            projection.clone(),
                            block_reader.clone(),
                            column_leaves.clone(),
                            max_threads,
                        ),
                    )
                } else {
                    TransformRowsFetcher::create(
                        input,
                        output,
                        row_id_col_offset,
                        NativeRowsFetcher::<false>::create(
                            fuse_table.clone(),
                            projection.clone(),
                            block_reader.clone(),
                            column_leaves.clone(),
                            max_threads,
                        ),
                    )
                })
            })
        }
        FuseStorageFormat::Parquet => {
            let read_settings = ReadSettings::from_ctx(&ctx)?;
            pipeline.add_transform(|input, output| {
                Ok(if block_reader.support_blocking_api() {
                    TransformRowsFetcher::create(
                        input,
                        output,
                        row_id_col_offset,
                        ParquetRowsFetcher::<true>::create(
                            fuse_table.clone(),
                            projection.clone(),
                            block_reader.clone(),
                            read_settings,
                            max_threads,
                        ),
                    )
                } else {
                    TransformRowsFetcher::create(
                        input,
                        output,
                        row_id_col_offset,
                        ParquetRowsFetcher::<false>::create(
                            fuse_table.clone(),
                            projection.clone(),
                            block_reader.clone(),
                            read_settings,
                            max_threads,
                        ),
                    )
                })
            })
        }
    }
}

#[async_trait::async_trait]
pub trait RowsFetcher {
    async fn on_start(&mut self) -> Result<()>;
    async fn fetch(&mut self, row_ids: &[u64]) -> Result<DataBlock>;
    fn schema(&self) -> DataSchema;
}

pub struct TransformRowsFetcher<F: RowsFetcher> {
    row_id_col_offset: usize,
    fetcher: F,
}

#[async_trait::async_trait]
impl<F> AsyncTransform for TransformRowsFetcher<F>
where F: RowsFetcher + Send + Sync + 'static
{
    const NAME: &'static str = "TransformRowsFetcher";

    #[async_backtrace::framed]
    async fn on_start(&mut self) -> Result<()> {
        self.fetcher.on_start().await
    }

    #[async_backtrace::framed]
    async fn transform(&mut self, mut data: DataBlock) -> Result<DataBlock> {
        let num_rows = data.num_rows();
        if num_rows == 0 {
            // Although the data block is empty, we need to add empty columns to align the schema.
            let fetched_schema = self.fetcher.schema();
            for f in fetched_schema.fields().iter() {
                let builder = ColumnBuilder::with_capacity(f.data_type(), 0);
                let col = builder.build();
                data.add_column(BlockEntry::new(f.data_type().clone(), Value::Column(col)));
            }
            return Ok(data);
        }

        let row_id_column = data.columns()[self.row_id_col_offset]
            .value
            .convert_to_full_column(&DataType::Number(NumberDataType::UInt64), num_rows)
            .into_number()
            .unwrap()
            .into_u_int64()
            .unwrap();

        let fetched_block = self.fetcher.fetch(&row_id_column).await?;

        for col in fetched_block.columns().iter() {
            data.add_column(col.clone());
        }

        Ok(data)
    }
}

impl<F> TransformRowsFetcher<F>
where F: RowsFetcher + Send + Sync + 'static
{
    fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        row_id_col_offset: usize,
        fetcher: F,
    ) -> ProcessorPtr {
        ProcessorPtr::create(AsyncTransformer::create(input, output, Self {
            row_id_col_offset,
            fetcher,
        }))
    }
}
