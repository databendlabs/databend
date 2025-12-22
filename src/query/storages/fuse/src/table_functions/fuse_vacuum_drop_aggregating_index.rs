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

use chrono::Duration;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::UInt64Type;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use log::info;

use super::string_literal;
use super::string_value;
use crate::sessions::TableContext;
use crate::table_functions::SimpleTableFunc;
use crate::table_functions::TableArgs;
pub struct FuseVacuumDropAggregatingIndex {
    args: FuseVacuumDropAggregatingIndexArgs,
}

struct FuseVacuumDropAggregatingIndexArgs {
    database_table: Option<(String, String)>,
}

impl From<&FuseVacuumDropAggregatingIndexArgs> for TableArgs {
    fn from(args: &FuseVacuumDropAggregatingIndexArgs) -> Self {
        let mut table_args = vec![];
        if let Some((database, table)) = &args.database_table {
            table_args.push(string_literal(database));
            table_args.push(string_literal(table));
        }
        TableArgs::new_positioned(table_args)
    }
}

#[async_trait::async_trait]
impl SimpleTableFunc for FuseVacuumDropAggregatingIndex {
    fn get_engine_name(&self) -> String {
        "fuse_vacuum_drop_aggregating_index".to_owned()
    }

    fn table_args(&self) -> Option<TableArgs> {
        Some((&self.args).into())
    }

    fn schema(&self) -> TableSchemaRef {
        TableSchemaRefExt::create(vec![
            TableField::new("table_id", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("index_id", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new(
                "num_removed_files",
                TableDataType::Number(NumberDataType::UInt64),
            ),
        ])
    }

    async fn apply(
        &self,
        ctx: &Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
    ) -> Result<Option<DataBlock>> {
        let mut table_ids = Vec::new();
        let mut index_ids = Vec::new();
        let mut num_removed_files = Vec::new();
        let catalog = ctx.get_default_catalog()?;
        let duration = Duration::days(ctx.get_settings().get_data_retention_time_in_days()? as i64);
        let retention_time = chrono::Utc::now() - duration;
        let tenant = ctx.get_tenant();
        let table = match &self.args.database_table {
            Some((database_name, table_name)) => Some(
                catalog
                    .get_table(&tenant, database_name, table_name)
                    .await?,
            ),
            None => None,
        };
        let table_id = table.map(|t| t.get_id());

        let reply = catalog
            .list_marked_deleted_indexes(&tenant, table_id)
            .await?;

        info!(
            "duration: {:?}, retention_time: {:?}, table_id: {:?}, marked_deleted_indexes: {:?}",
            duration, retention_time, table_id, reply
        );

        for (table_id, indexes) in reply.table_indexes {
            let Some(table_meta) = catalog.get_table_meta_by_id(table_id).await? else {
                // Skip vacuuming indexes of dropped tables - this will be handled by the vacuum drop table operation
                info!("skip vacuuming indexes of dropped table: {}", table_id);
                continue;
            };
            let table_info = TableInfo::new(
                Default::default(),
                Default::default(),
                TableIdent::new(table_id, table_meta.seq),
                table_meta.data,
            );
            let table = catalog.get_table_by_info(&table_info)?;
            let indexes_to_be_vacuumed = indexes
                .into_iter()
                .filter(|(_, index_meta)| index_meta.dropped_on < retention_time)
                .map(|(index_id, _)| index_id)
                .collect::<Vec<_>>();
            info!(
                "indexes_to_be_vacuumed for table: {:?}, indexes: {:?}",
                table_id, indexes_to_be_vacuumed
            );
            for index_id in &indexes_to_be_vacuumed {
                let n = table
                    .remove_aggregating_index_files(ctx.clone(), *index_id)
                    .await?;
                table_ids.push(table_id);
                index_ids.push(*index_id);
                num_removed_files.push(n);
            }
            catalog
                .remove_marked_deleted_index_ids(&tenant, table_id, &indexes_to_be_vacuumed)
                .await?;
        }

        Ok(Some(DataBlock::new_from_columns(vec![
            UInt64Type::from_data(table_ids),
            UInt64Type::from_data(index_ids),
            UInt64Type::from_data(num_removed_files),
        ])))
    }

    fn create(func_name: &str, table_args: TableArgs) -> Result<Self>
    where Self: Sized {
        let args = table_args.expect_all_positioned(func_name, None)?;
        let database_table = match args.len() {
            2 => {
                let database_name = string_value(&args[0])?;
                let table_name = string_value(&args[1])?;
                Some((database_name, table_name))
            }
            0 => None,
            _ => {
                return Err(ErrorCode::BadArguments(format!(
                    "expecting (<database_name>, <table_name>) or no args, but got {:?}",
                    args
                )));
            }
        };
        Ok(Self {
            args: FuseVacuumDropAggregatingIndexArgs { database_table },
        })
    }
}
