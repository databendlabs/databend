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
use common_catalog::table_context::TableContext;
use common_catalog::table_function::TableFunction;
use common_exception::Result;
use common_expression::types::NumberDataType;
use common_expression::types::StringType;
use common_expression::types::UInt64Type;
use common_expression::DataBlock;
use common_expression::FromData;
use common_expression::FromOptData;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_expression::TableSchemaRefExt;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::Pipeline;
use common_pipeline_sources::AsyncSource;
use common_pipeline_sources::AsyncSourcer;
use common_sql::binder::parse_stage_location;
use common_storage::StageFilesInfo;
use common_storages_stage::StageTable;

use crate::table_functions::list_stage::table_args::ListStageArgsParsed;

const LIST_STAGE: &str = "list_stage";

pub struct ListStageTable {
    args_parsed: ListStageArgsParsed,
    table_args: TableArgs,
    table_info: TableInfo,
}

impl ListStageTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let args_parsed = ListStageArgsParsed::parse(&table_args)?;
        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: TableMeta {
                schema: Self::schema(),
                engine: LIST_STAGE.to_owned(),
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

    fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("name", TableDataType::String),
            TableField::new("size", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new(
                "md5",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new("last_modified", TableDataType::String),
            TableField::new(
                "creator",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
        ])
    }
}

#[async_trait::async_trait]
impl Table for ListStageTable {
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
            |output| ListStagesSource::create(ctx.clone(), output, self.args_parsed.clone()),
            1,
        )?;
        Ok(())
    }
}

impl TableFunction for ListStageTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

struct ListStagesSource {
    is_finished: bool,
    ctx: Arc<dyn TableContext>,
    args_parsed: ListStageArgsParsed,
}

impl ListStagesSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        args_parsed: ListStageArgsParsed,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.clone(), output, ListStagesSource {
            is_finished: false,
            ctx,
            args_parsed,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for ListStagesSource {
    const NAME: &'static str = LIST_STAGE;

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.is_finished {
            return Ok(None);
        }

        self.is_finished = true;

        let (stage_info, path) =
            parse_stage_location(&self.ctx, &self.args_parsed.location).await?;
        let op = StageTable::get_op(&stage_info)?;

        let files_info = StageFilesInfo {
            path,
            files: self.args_parsed.files_info.files.clone(),
            pattern: self.args_parsed.files_info.pattern.clone(),
        };

        let files = files_info.list(&op, false, None).await?;

        let names: Vec<Vec<u8>> = files
            .iter()
            .map(|file| file.path.to_string().into_bytes())
            .collect();

        let sizes: Vec<u64> = files.iter().map(|file| file.size).collect();
        let etags: Vec<Option<Vec<u8>>> = files
            .iter()
            .map(|file| file.etag.as_ref().map(|f| f.to_string().into_bytes()))
            .collect();
        let last_modifieds: Vec<Vec<u8>> = files
            .iter()
            .map(|file| {
                file.last_modified
                    .format("%Y-%m-%d %H:%M:%S.%3f %z")
                    .to_string()
                    .into_bytes()
            })
            .collect();
        let creators: Vec<Option<Vec<u8>>> = files
            .iter()
            .map(|file| file.creator.as_ref().map(|c| c.to_string().into_bytes()))
            .collect();

        let block = DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            UInt64Type::from_data(sizes),
            StringType::from_opt_data(etags),
            StringType::from_data(last_modifieds),
            StringType::from_opt_data(creators),
        ]);

        Ok(Some(block))
    }
}
