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
use std::collections::BTreeMap;
use std::sync::Arc;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::UInt64Type;
use databend_common_meta_app::schema::TableCopiedFileInfo;
use databend_common_meta_app::schema::TableInfo;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_meta_types::MetaId;

use crate::sessions::QueryContext;
use crate::table_functions::TableFunction;

pub struct CopyHistoryTable {
    table_info: TableInfo,
    args: TableArgs,
}

impl CopyHistoryTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: MetaId,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let schema = Self::schema();

        let table_info = TableInfo {
            ident: databend_common_meta_app::schema::TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: databend_common_meta_app::schema::TableMeta {
                schema: schema.clone(),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(Self {
            table_info,
            args: table_args,
        }))
    }

    fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("file_name", TableDataType::String),
            TableField::new(
                "content_length",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "last_modified",
                TableDataType::Nullable(Box::new(TableDataType::Timestamp)),
            ),
            TableField::new(
                "etag",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
        ])
    }

    fn parse_args(&self) -> Result<String> {
        let args = &self.args.expect_all_positioned("COPY_HISTORY", Some(1))?;

        if args.is_empty() {
            return Err(ErrorCode::BadArguments(
                "COPY_HISTORY function requires TABLE_NAME argument",
            ));
        }

        // Parse TABLE_NAME
        let table_name = match &args[0] {
            Scalar::String(s) => s.clone(),
            _ => return Err(ErrorCode::BadArguments("TABLE_NAME must be a string")),
        };

        Ok(table_name)
    }
}

#[async_trait::async_trait]
impl Table for CopyHistoryTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn has_exact_total_row_count(&self) -> bool {
        true
    }

    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        Ok((PartStatistics::default(), Partitions::default()))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        let table_name = self.parse_args()?;

        pipeline.add_source(
            |output| CopyHistorySource::create(ctx.clone(), output, table_name.clone()),
            1,
        )?;

        Ok(())
    }

    async fn truncate(&self, _ctx: Arc<dyn TableContext>, _pipeline: &mut Pipeline) -> Result<()> {
        Err(ErrorCode::Unimplemented(
            "Cannot truncate copy_history table function",
        ))
    }

    fn table_args(&self) -> Option<TableArgs> {
        Some(self.args.clone())
    }
}

impl TableFunction for CopyHistoryTable {
    fn function_name(&self) -> &str {
        "copy_history"
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

enum State {
    NotStarted,
    Finished,
}

struct CopyHistorySource {
    state: State,
    ctx: Arc<dyn TableContext>,
    table_name: String,
    copied_files: Option<BTreeMap<String, TableCopiedFileInfo>>,
}

impl CopyHistorySource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        table_name: String,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.get_scan_progress(), output, CopyHistorySource {
            state: State::NotStarted,
            ctx,
            table_name,
            copied_files: None,
        })
    }

    async fn do_get_copied_files(&mut self) -> Result<BTreeMap<String, TableCopiedFileInfo>> {
        let ctx = self
            .ctx
            .as_any()
            .downcast_ref::<QueryContext>()
            .ok_or_else(|| ErrorCode::Internal("Invalid context type"))?;

        // Get current database and catalog
        let current_database = ctx.get_current_database();
        let current_catalog = ctx.get_current_catalog();

        // Parse full table name (database.table or just table)
        let (db_name, tbl_name) = if self.table_name.contains('.') {
            let parts: Vec<&str> = self.table_name.split('.').collect();
            if parts.len() != 2 {
                return Err(ErrorCode::BadArguments("Invalid table name format"));
            }
            (parts[0].to_string(), parts[1].to_string())
        } else {
            (current_database, self.table_name.clone())
        };

        let table = ctx.get_table(&current_catalog, &db_name, &tbl_name).await?;
        let table_id = table.get_id();
        let catalog = ctx.get_default_catalog().unwrap();
        let copied_files = catalog
            .list_table_copied_file_info(&ctx.get_tenant(), &db_name, table_id)
            .await?
            .file_info;

        Ok(copied_files)
    }
}

fn make_copy_history_block(files: &BTreeMap<String, TableCopiedFileInfo>) -> DataBlock {
    let mut file_names = Vec::new();
    let mut content_lengths = Vec::new();
    let mut last_modifieds = Vec::new();
    let mut etags = Vec::new();

    for (file_name, file_info) in files.iter() {
        file_names.push(file_name.clone());
        content_lengths.push(file_info.content_length);
        last_modifieds.push(file_info.last_modified.map(|dt| dt.timestamp_micros()));
        etags.push(file_info.etag.clone());
    }

    DataBlock::new_from_columns(vec![
        StringType::from_data(file_names),
        UInt64Type::from_data(content_lengths),
        TimestampType::from_opt_data(last_modifieds),
        StringType::from_opt_data(etags),
    ])
}

#[async_trait::async_trait]
impl AsyncSource for CopyHistorySource {
    const NAME: &'static str = "copy_history";

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        match &self.state {
            State::Finished => {
                return Ok(None);
            }
            State::NotStarted => {
                let copied_files = self.do_get_copied_files().await?;
                self.copied_files = Some(copied_files);
                self.state = State::Finished;
            }
        }

        if let Some(ref copied_files) = self.copied_files {
            Ok(Some(make_copy_history_block(copied_files)))
        } else {
            Ok(None)
        }
    }
}
