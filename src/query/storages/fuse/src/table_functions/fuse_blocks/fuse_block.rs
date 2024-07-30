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

use databend_common_catalog::catalog_kind::CATALOG_DEFAULT;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_exception::Result;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::Value;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::io::SegmentsIO;
use crate::sessions::TableContext;
use crate::table_functions::common::location_snapshot;
use crate::table_functions::common::CommonArgs;
use crate::table_functions::parse_db_tb_opt_args;
use crate::table_functions::SimpleTableFunc;
use crate::FuseTable;

const FUSE_FUNC_BLOCK: &str = "fuse_block";

pub struct FuseBlock {
    // pub limit: Option<usize>,
    pub args: CommonArgs,
}

impl FuseBlock {
    #[async_backtrace::framed]
    pub async fn get_blocks(
        &self,
        ctx: &Arc<dyn TableContext>,
        tbl: &FuseTable,
        limit: Option<usize>,
    ) -> Result<DataBlock> {
        if let Some(snapshot) = location_snapshot(tbl, &self.args).await? {
            return self.to_block(ctx, tbl, snapshot, limit).await;
        } else {
            Ok(DataBlock::empty_with_schema(Arc::new(
                Self::schema().into(),
            )))
        }
    }

    #[async_backtrace::framed]
    async fn to_block(
        &self,
        ctx: &Arc<dyn TableContext>,
        tbl: &FuseTable,
        snapshot: Arc<TableSnapshot>,
        limit: Option<usize>,
    ) -> Result<DataBlock> {
        let limit = limit.unwrap_or(usize::MAX);
        let len = std::cmp::min(snapshot.summary.block_count as usize, limit);

        let snapshot_id = snapshot.snapshot_id.simple().to_string();
        let timestamp = snapshot.timestamp.unwrap_or_default().timestamp_micros();
        let mut block_location = StringColumnBuilder::with_capacity(len, len);
        let mut block_size = Vec::with_capacity(len);
        let mut file_size = Vec::with_capacity(len);
        let mut row_count = Vec::with_capacity(len);
        let mut bloom_filter_location = vec![];
        let mut bloom_filter_size = Vec::with_capacity(len);
        let mut inverted_index_size = Vec::with_capacity(len);

        let segments_io = SegmentsIO::create(ctx.clone(), tbl.operator.clone(), tbl.schema());

        let mut row_num = 0;
        let mut end_flag = false;
        let chunk_size =
            std::cmp::min(ctx.get_settings().get_max_threads()? as usize * 4, len).max(1);
        'FOR: for chunk in snapshot.segments.chunks(chunk_size) {
            let segments = segments_io
                .read_segments::<SegmentInfo>(chunk, true)
                .await?;
            for segment in segments {
                let segment = segment?;

                for block in segment.blocks.iter() {
                    let block = block.as_ref();
                    block_location.put_str(&block.location.0);
                    block_location.commit_row();
                    block_size.push(block.block_size);
                    file_size.push(block.file_size);
                    row_count.push(block.row_count);
                    bloom_filter_location.push(
                        block
                            .bloom_filter_index_location
                            .as_ref()
                            .map(|s| s.0.clone()),
                    );
                    bloom_filter_size.push(block.bloom_filter_index_size);
                    inverted_index_size.push(block.inverted_index_size);

                    row_num += 1;
                    if row_num >= limit {
                        end_flag = true;
                        break;
                    }
                }

                if end_flag {
                    break 'FOR;
                }
            }
        }

        Ok(DataBlock::new(
            vec![
                BlockEntry::new(DataType::String, Value::Scalar(Scalar::String(snapshot_id))),
                BlockEntry::new(
                    DataType::Timestamp,
                    Value::Scalar(Scalar::Timestamp(timestamp)),
                ),
                BlockEntry::new(
                    DataType::String,
                    Value::Column(Column::String(block_location.build())),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Column(UInt64Type::from_data(block_size)),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Column(UInt64Type::from_data(file_size)),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Column(UInt64Type::from_data(row_count)),
                ),
                BlockEntry::new(
                    DataType::String.wrap_nullable(),
                    Value::Column(StringType::from_opt_data(bloom_filter_location)),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Column(UInt64Type::from_data(bloom_filter_size)),
                ),
                BlockEntry::new(
                    DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64))),
                    Value::Column(UInt64Type::from_opt_data(inverted_index_size)),
                ),
            ],
            row_num,
        ))
    }

    pub fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("snapshot_id", TableDataType::String),
            TableField::new("timestamp", TableDataType::Timestamp),
            TableField::new("block_location", TableDataType::String),
            TableField::new("block_size", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("file_size", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("row_count", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new(
                "bloom_filter_location",
                TableDataType::String.wrap_nullable(),
            ),
            TableField::new(
                "bloom_filter_size",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "inverted_index_size",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
        ])
    }
}

#[async_trait::async_trait]
impl SimpleTableFunc for FuseBlock {
    fn table_args(&self) -> Option<TableArgs> {
        Some((&self.args).into())
    }

    fn schema(&self) -> TableSchemaRef {
        Self::schema()
    }

    async fn apply(
        &self,
        ctx: &Arc<dyn TableContext>,
        plan: &DataSourcePlan,
    ) -> Result<Option<DataBlock>> {
        let tenant_id = ctx.get_tenant();
        let tbl = ctx
            .get_catalog(CATALOG_DEFAULT)
            .await?
            .get_table(
                &tenant_id,
                self.args.arg_database_name.as_str(),
                self.args.arg_table_name.as_str(),
            )
            .await?;
        let limit = plan.push_downs.as_ref().and_then(|x| x.limit);
        let tbl = FuseTable::try_from_table(tbl.as_ref())?;
        Ok(Some(self.get_blocks(ctx, tbl, limit).await?))
    }

    fn create(table_args: TableArgs) -> Result<Self>
    where Self: Sized {
        let (arg_database_name, arg_table_name, arg_snapshot_id) =
            parse_db_tb_opt_args(&table_args, FUSE_FUNC_BLOCK)?;
        Ok(Self {
            args: CommonArgs {
                arg_database_name,
                arg_table_name,
                arg_snapshot_id,
            },
        })
    }
}
