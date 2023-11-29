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

use std::any::Any;
use std::cmp::max;
use std::sync::Arc;

use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_args::TableArgs;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::Int64Type;
use common_expression::types::NumberDataType;
use common_expression::types::StringType;
use common_expression::types::UInt64Type;
use common_expression::types::ValueType;
use common_expression::BlockEntry;
use common_expression::DataBlock;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_expression::TableSchemaRefExt;
use common_expression::Value;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_pipeline_core::processors::ProcessorPtr;
use common_pipeline_core::Pipeline;
use common_pipeline_sources::AsyncSource;
use common_pipeline_sources::AsyncSourcer;
use common_sql::binder::resolve_stage_location;
use common_storage::init_stage_operator;
use common_storage::read_metadata_async;
use common_storage::StageFilesInfo;
use common_storages_fuse::table_functions::string_literal;

use crate::pipelines::processors::OutputPort;
use crate::sessions::TableContext;
use crate::table_functions::TableFunction;

const INSPECT_PARQUET: &str = "inspect_parquet";

pub struct InspectParquetTable {
    uri: String,
    table_info: TableInfo,
}

impl InspectParquetTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let args = table_args.expect_all_positioned(table_func_name, Some(1))?;
        let file_path = String::from_utf8(
            args[0]
                .clone()
                .into_string()
                .map_err(|_| ErrorCode::BadArguments("Expected string argument."))?,
        )?;
        if !file_path.starts_with('@') {
            return Err(ErrorCode::BadArguments(format!(
                "stage path must start with @, but got {}",
                file_path
            )));
        }

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: TableMeta {
                schema: Self::schema(),
                engine: INSPECT_PARQUET.to_owned(),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(Self {
            uri: file_path,
            table_info,
        }))
    }

    pub fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("created_by", TableDataType::String),
            TableField::new("num_columns", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("num_rows", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new(
                "num_row_groups",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "serialized_size",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "max_row_groups_size_compressed",
                TableDataType::Number(NumberDataType::Int64),
            ),
            TableField::new(
                "max_row_groups_size_uncompressed",
                TableDataType::Number(NumberDataType::Int64),
            ),
        ])
    }
}

#[async_trait::async_trait]
impl Table for InspectParquetTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        Ok((PartStatistics::default(), Partitions::default()))
    }

    fn table_args(&self) -> Option<TableArgs> {
        Some(TableArgs::new_positioned(vec![string_literal(
            self.uri.as_str(),
        )]))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        pipeline.add_source(
            |output| InspectParquetSource::create(ctx.clone(), output, self.uri.clone()),
            1,
        )?;
        Ok(())
    }
}

impl TableFunction for InspectParquetTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

struct InspectParquetSource {
    is_finished: bool,
    ctx: Arc<dyn TableContext>,
    uri: String,
}

impl InspectParquetSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        uri: String,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.clone(), output, InspectParquetSource {
            is_finished: false,
            ctx,
            uri,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for InspectParquetSource {
    const NAME: &'static str = INSPECT_PARQUET;

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.is_finished {
            return Ok(None);
        }
        self.is_finished = true;
        let uri = self.uri.strip_prefix('@').unwrap().to_string();
        let (stage_info, path) = resolve_stage_location(&self.ctx, &uri).await?;
        let enable_stage_udf_priv_check =
            self.ctx.get_settings().get_enable_stage_udf_priv_check()?;
        if enable_stage_udf_priv_check {
            let visibility_checker = self.ctx.get_visibility_checker().await?;
            if !stage_info.is_from_uri
                && !visibility_checker.check_stage_read_visibility(&stage_info.stage_name)
            {
                return Err(ErrorCode::PermissionDenied(format!(
                    "Permission denied, privilege READ is required on stage {} for user {}",
                    stage_info.stage_name.clone(),
                    &self.ctx.get_current_user()?.identity(),
                )));
            }
        }

        let operator = init_stage_operator(&stage_info)?;

        let file_info = StageFilesInfo {
            path: path.clone(),
            files: None,
            pattern: None,
        };

        let first_file = file_info.first_file(&operator).await?;

        let parquet_schema =
            read_metadata_async(&first_file.path, &operator, Some(first_file.size)).await?;
        let created = match parquet_schema.file_metadata().created_by() {
            Some(user) => user.to_owned(),
            None => String::from("NULL"),
        };
        let serialized_size: u64 = first_file.size;
        let num_columns: u64 = if parquet_schema.num_row_groups() > 0 {
            parquet_schema.row_group(0).num_columns() as u64
        } else {
            0
        };
        let mut max_compressed: i64 = 0;
        let mut max_uncompressed: i64 = 0;
        for grp in parquet_schema.row_groups().iter() {
            let mut grp_compressed_size: i64 = 0;
            let mut grp_uncompressed_size: i64 = 0;
            for col in grp.columns().iter() {
                grp_compressed_size += col.compressed_size();
                grp_uncompressed_size += col.uncompressed_size();
            }
            max_compressed = max(max_compressed, grp_compressed_size);
            max_uncompressed = max(max_uncompressed, grp_uncompressed_size);
        }
        let block = DataBlock::new(
            vec![
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(StringType::upcast_scalar(created.into())),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Scalar(UInt64Type::upcast_scalar(num_columns)),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Scalar(UInt64Type::upcast_scalar(
                        parquet_schema.file_metadata().num_rows() as u64,
                    )),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Scalar(UInt64Type::upcast_scalar(
                        parquet_schema.num_row_groups() as u64
                    )),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Scalar(UInt64Type::upcast_scalar(serialized_size)),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::Int64),
                    Value::Scalar(Int64Type::upcast_scalar(max_compressed)),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::Int64),
                    Value::Scalar(Int64Type::upcast_scalar(max_uncompressed)),
                ),
            ],
            1,
        );
        Ok(Some(block))
    }
}
