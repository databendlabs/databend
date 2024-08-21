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

use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::utils::FromData;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_users::UserApiProvider;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub struct DatabasesTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for DatabasesTable {
    const NAME: &'static str = "system.databases";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();

        let catalogs = CatalogManager::instance();
        let catalogs: Vec<(String, Arc<dyn Catalog>)> = catalogs
            .list_catalogs(&tenant, ctx.session_state())
            .await?
            .iter()
            .map(|e| (e.name(), e.clone()))
            .collect();

        let user_api = UserApiProvider::instance();
        let mut catalog_names = vec![];
        let mut db_names = vec![];
        let mut db_id = vec![];
        let mut owners: Vec<Option<String>> = vec![];

        let visibility_checker = ctx.get_visibility_checker().await?;

        for (ctl_name, catalog) in catalogs.into_iter() {
            let databases = catalog.list_databases(&tenant).await?;
            let final_dbs = databases
                .into_iter()
                .filter(|db| {
                    visibility_checker.check_database_visibility(
                        &ctl_name,
                        db.name(),
                        db.get_db_info().database_id.db_id,
                    )
                })
                .collect::<Vec<_>>();

            for db in final_dbs {
                catalog_names.push(ctl_name.clone());
                let db_name = db.name().to_string();
                db_names.push(db_name);
                let id = db.get_db_info().database_id.db_id;
                db_id.push(id);
                owners.push(
                    user_api
                        .get_ownership(&tenant, &OwnershipObject::Database {
                            catalog_name: ctl_name.to_string(),
                            db_id: id,
                        })
                        .await
                        .ok()
                        .and_then(|ownership| ownership.map(|o| o.role.clone())),
                );
            }
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(catalog_names),
            StringType::from_data(db_names),
            UInt64Type::from_data(db_id),
            StringType::from_opt_data(owners),
        ]))
    }
}

impl DatabasesTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("catalog", TableDataType::String),
            TableField::new("name", TableDataType::String),
            TableField::new("database_id", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new(
                "owner",
                TableDataType::Nullable(Box::from(TableDataType::String)),
            ),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'databases'".to_string(),
            name: "databases".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemDatabases".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(DatabasesTable { table_info })
    }
}
