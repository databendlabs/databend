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
use std::sync::Arc;

use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_args::TableArgs;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::BooleanType;
use common_expression::types::NumberDataType;
use common_expression::types::StringType;
use common_expression::types::UInt64Type;
use common_expression::DataBlock;
use common_expression::FromData;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_expression::TableSchemaRefExt;
use common_meta_app::principal::StageFileFormatType;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_sources::AsyncSource;
use common_pipeline_sources::AsyncSourcer;
use common_sql::binder::parse_stage_location;
use common_storage::init_stage_operator;
use common_storage::read_parquet_schema_async;
use common_storage::StageFilesInfo;

use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::Pipeline;
use crate::sessions::TableContext;
use crate::table_functions::infer_schema::table_args::InferSchemaArgsParsed;
use crate::table_functions::TableFunction;

const INFER_SCHEMA: &str = "infer_schema";

pub struct InferSchemaTable {
    table_info: TableInfo,
    args_parsed: InferSchemaArgsParsed,
    table_args: TableArgs,
}

impl InferSchemaTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let args_parsed = InferSchemaArgsParsed::parse(&table_args)?;
        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: TableMeta {
                schema: Self::schema(),
                engine: INFER_SCHEMA.to_owned(),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(Self {
            table_info,
            args_parsed,
            table_args,
        }))
    }

    pub fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("column_name", TableDataType::String),
            TableField::new("type", TableDataType::String),
            TableField::new("nullable", TableDataType::Boolean),
            TableField::new("order_id", TableDataType::Number(NumberDataType::UInt64)),
        ])
    }
}

#[async_trait::async_trait]
impl Table for InferSchemaTable {
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
    ) -> Result<(PartStatistics, Partitions)> {
        Ok((PartStatistics::default(), Partitions::default()))
    }

    fn table_args(&self) -> Option<TableArgs> {
        Some(self.table_args.clone())
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        pipeline.add_source(
            |output| InferSchemaSource::create(ctx.clone(), output, self.args_parsed.clone()),
            1,
        )?;
        Ok(())
    }
}

impl TableFunction for InferSchemaTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

struct InferSchemaSource {
    is_finished: bool,
    ctx: Arc<dyn TableContext>,
    args_parsed: InferSchemaArgsParsed,
}

impl InferSchemaSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        args_parsed: InferSchemaArgsParsed,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.clone(), output, InferSchemaSource {
            is_finished: false,
            ctx,
            args_parsed,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for InferSchemaSource {
    const NAME: &'static str = INFER_SCHEMA;

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.is_finished {
            return Ok(None);
        }
        self.is_finished = true;

        let (stage_info, path) =
            parse_stage_location(&self.ctx, &self.args_parsed.location).await?;
        let files_info = StageFilesInfo {
            path,
            ..self.args_parsed.files_info.clone()
        };
        let operator = init_stage_operator(&stage_info)?;

        let first_file = files_info.first_file(&operator).await?;
        let file_format_params = match &self.args_parsed.file_format {
            Some(f) => self.ctx.get_file_format(f).await?,
            None => stage_info.file_format_params.clone(),
        };
        let schema = match file_format_params.get_type() {
            StageFileFormatType::Parquet => {
                let arrow_schema = read_parquet_schema_async(&operator, &first_file.path).await?;
                TableSchema::from(&arrow_schema)
            }
            _ => {
                return Err(ErrorCode::BadArguments(
                    "infer_schema is currently limited to format Parquet",
                ));
            }
        };

        let mut names: Vec<Vec<u8>> = vec![];
        let mut types: Vec<Vec<u8>> = vec![];
        let mut nulls: Vec<bool> = vec![];

        for field in schema.fields().iter() {
            names.push(field.name().to_string().as_bytes().to_vec());

            let non_null_type = field.data_type().remove_recursive_nullable();
            types.push(non_null_type.sql_name().as_bytes().to_vec());
            nulls.push(field.is_nullable());
        }

        let order_ids = (0..schema.fields().len() as u64).collect::<Vec<_>>();

        let block = DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            StringType::from_data(types),
            BooleanType::from_data(nulls),
            UInt64Type::from_data(order_ids),
        ]);
        Ok(Some(block))
    }
}
