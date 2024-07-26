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
use databend_common_catalog::table_args::TableArgs;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::StringType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::get_license_manager;
use databend_common_storages_fuse::table_functions::parse_db_tb_args;
use databend_common_storages_fuse::table_functions::string_literal;
use databend_common_storages_fuse::table_functions::SimpleTableFunc;
use databend_common_storages_fuse::FuseTable;
use databend_enterprise_vacuum_handler::get_vacuum_handler;
use databend_enterprise_vacuum_handler::VacuumHandlerWrapper;

use crate::sessions::TableContext;
const FUSE_VACUUM2_ENGINE_NAME: &str = "fuse_vacuum2_table";
struct Vacuum2TableArgs {
    arg_database_name: String,
    arg_table_name: String,
}

impl From<&Vacuum2TableArgs> for TableArgs {
    fn from(value: &Vacuum2TableArgs) -> Self {
        TableArgs::new_positioned(vec![
            string_literal(value.arg_database_name.as_str()),
            string_literal(value.arg_table_name.as_str()),
        ])
    }
}

pub struct FuseVacuum2Table {
    args: Vacuum2TableArgs,
    handler: Arc<VacuumHandlerWrapper>,
}

#[async_trait::async_trait]
impl SimpleTableFunc for FuseVacuum2Table {
    fn table_args(&self) -> Option<TableArgs> {
        Some((&self.args).into())
    }

    fn schema(&self) -> TableSchemaRef {
        TableSchemaRefExt::create(vec![TableField::new("vacuumed", TableDataType::String)])
    }

    async fn apply(&self, ctx: &Arc<dyn TableContext>) -> Result<Option<DataBlock>> {
        let license_manager = get_license_manager();
        license_manager
            .manager
            .check_enterprise_enabled(ctx.get_license_key(), Feature::Vacuum)?;
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

        let tbl = FuseTable::try_from_table(tbl.as_ref()).map_err(|_| {
            ErrorCode::StorageOther("Invalid table engine, only fuse table is supported")
        })?;

        self.handler.do_vacuum2(tbl, ctx.clone()).await?;

        let col: Vec<String> = vec!["Ok".to_owned()];

        Ok(Some(DataBlock::new_from_columns(vec![
            StringType::from_data(col),
        ])))
    }

    fn create(table_args: TableArgs) -> Result<Self>
    where Self: Sized {
        let (arg_database_name, arg_table_name) =
            parse_db_tb_args(&table_args, FUSE_VACUUM2_ENGINE_NAME)?;
        Ok(Self {
            args: Vacuum2TableArgs {
                arg_database_name,
                arg_table_name,
            },
            handler: get_vacuum_handler(),
        })
    }
}
