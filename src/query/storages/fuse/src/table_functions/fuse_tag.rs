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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_meta_app::schema::ListTableTagsReq;

use crate::operations::check_table_ref_access;
use crate::sessions::TableContext;
use crate::table_functions::SimpleTableFunc;
use crate::table_functions::parse_db_tb_args;
use crate::table_functions::string_literal;

struct FuseTagArgs {
    database_name: String,
    table_name: String,
}

impl From<&FuseTagArgs> for TableArgs {
    fn from(args: &FuseTagArgs) -> Self {
        TableArgs::new_positioned(vec![
            string_literal(args.database_name.as_str()),
            string_literal(args.table_name.as_str()),
        ])
    }
}

pub struct FuseTagFunc {
    args: FuseTagArgs,
}

#[async_trait::async_trait]
impl SimpleTableFunc for FuseTagFunc {
    fn table_args(&self) -> Option<TableArgs> {
        Some((&self.args).into())
    }

    fn schema(&self) -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("name", TableDataType::String),
            TableField::new("snapshot_location", TableDataType::String),
            TableField::new("expire_at", TableDataType::Timestamp.wrap_nullable()),
        ])
    }

    async fn apply(
        &self,
        ctx: &Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
    ) -> Result<Option<DataBlock>> {
        check_table_ref_access(ctx.as_ref())?;

        let catalog = ctx.get_current_catalog();
        let tenant = ctx.get_tenant();
        let tbl = ctx
            .get_catalog(&catalog)
            .await?
            .get_table(&tenant, &self.args.database_name, &self.args.table_name)
            .await?;
        if tbl.engine() != "FUSE" {
            return Err(ErrorCode::TableEngineNotSupported(
                "Invalid table engine, only FUSE table supports fuse_tag",
            ));
        }
        let table_id = tbl.get_id();

        let tags = ctx
            .get_catalog(&catalog)
            .await?
            .list_table_tags(ListTableTagsReq {
                table_id,
                include_expired: true,
            })
            .await?;

        if tags.is_empty() {
            return Ok(Some(DataBlock::empty_with_schema(&self.schema().into())));
        }

        let mut names: Vec<String> = Vec::with_capacity(tags.len());
        let mut snapshot_locations: Vec<String> = Vec::with_capacity(tags.len());
        let mut expire_ats: Vec<Option<i64>> = Vec::with_capacity(tags.len());

        for (tag_name, seq_tag) in &tags {
            names.push(tag_name.clone());
            snapshot_locations.push(seq_tag.data.snapshot_loc.clone());
            expire_ats.push(seq_tag.data.expire_at.map(|t| t.timestamp_micros()));
        }

        Ok(Some(DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            StringType::from_data(snapshot_locations),
            TimestampType::from_opt_data(expire_ats),
        ])))
    }

    fn create(func_name: &str, table_args: TableArgs) -> Result<Self>
    where Self: Sized {
        let (arg_database_name, arg_table_name) = parse_db_tb_args(&table_args, func_name)?;
        Ok(Self {
            args: FuseTagArgs {
                database_name: arg_database_name,
                table_name: arg_table_name,
            },
        })
    }
}
