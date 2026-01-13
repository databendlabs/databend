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

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_args::parse_table_name;
use databend_common_catalog::table_args::string_literal;
use databend_common_catalog::table_args::string_value;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::UInt64Type;
use databend_common_storages_fuse::table_functions::SimpleTableFunc;

pub struct CopyHistoryFunc {
    args: CopyHistoryArgs,
}

pub struct CopyHistoryArgs {
    table_name: String,
}

impl From<&CopyHistoryArgs> for TableArgs {
    fn from(args: &CopyHistoryArgs) -> Self {
        TableArgs::new_positioned(vec![string_literal(args.table_name.as_str())])
    }
}

#[async_trait::async_trait]
impl SimpleTableFunc for CopyHistoryFunc {
    fn table_args(&self) -> Option<TableArgs> {
        Some((&self.args).into())
    }

    fn schema(&self) -> Arc<TableSchema> {
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

    async fn apply(
        &self,
        ctx: &Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
    ) -> Result<Option<DataBlock>> {
        // Get current database and catalog
        let current_database = ctx.get_current_database();
        let current_catalog = ctx.get_current_catalog();
        let (name, branch_name) = parse_table_name(&self.args.table_name)?;
        // Parse full table name (database.table or just table)
        let (database_name, table_name) = if name.contains('.') {
            let parts: Vec<&str> = name.split('.').collect();
            if parts.len() != 2 {
                return Err(ErrorCode::BadArguments("Invalid table name format"));
            }
            (parts[0].to_string(), parts[1].to_string())
        } else {
            (current_database, name.clone())
        };
        let table = ctx
            .get_table_with_batch(
                &current_catalog,
                &database_name,
                &table_name,
                branch_name.as_deref(),
                None,
            )
            .await?;
        let unique_id = table.get_unique_id();
        let catalog = ctx.get_default_catalog().unwrap();
        let copied_files = catalog
            .list_table_copied_file_info(&ctx.get_tenant(), &database_name, unique_id)
            .await?
            .file_info;
        let mut file_names = Vec::new();
        let mut content_lengths = Vec::new();
        let mut last_modifieds = Vec::new();
        let mut etags = Vec::new();

        for (file_name, file_info) in copied_files.iter() {
            file_names.push(file_name.clone());
            content_lengths.push(file_info.content_length);
            last_modifieds.push(file_info.last_modified.map(|dt| dt.timestamp_micros()));
            etags.push(file_info.etag.clone());
        }

        Ok(Some(DataBlock::new_from_columns(vec![
            StringType::from_data(file_names),
            UInt64Type::from_data(content_lengths),
            TimestampType::from_opt_data(last_modifieds),
            StringType::from_opt_data(etags),
        ])))
    }

    fn create(func_name: &str, table_args: TableArgs) -> Result<Self>
    where Self: Sized {
        let args = &table_args.expect_all_positioned(func_name, Some(1))?;
        if args.is_empty() {
            return Err(ErrorCode::BadArguments(
                "COPY_HISTORY function requires TABLE_NAME argument",
            ));
        }

        // Parse TABLE_NAME
        let table_name = string_value(&args[0])?;
        Ok(Self {
            args: CopyHistoryArgs { table_name },
        })
    }
}
