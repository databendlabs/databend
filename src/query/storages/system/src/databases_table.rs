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

use common_catalog::catalog::Catalog;
use common_catalog::catalog::CatalogManager;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::types::NumberDataType;
use common_expression::types::StringType;
use common_expression::types::UInt64Type;
use common_expression::utils::FromData;
use common_expression::DataBlock;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchemaRefExt;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;

use crate::columns_table::GrantObjectVisibilityChecker;
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
            .list_catalogs(&tenant)
            .await?
            .iter()
            .map(|e| (e.name(), e.clone()))
            .collect();

        let mut catalog_names = vec![];
        let mut db_names = vec![];
        let mut db_id = vec![];

        let user = ctx.get_current_user()?;
        let roles = ctx.get_current_available_roles().await?;
        let visibility_checker = GrantObjectVisibilityChecker::new(&user, &roles);

        for (ctl_name, catalog) in catalogs.into_iter() {
            let databases = catalog.list_databases(tenant.as_str()).await?;
            let final_dbs = databases
                .into_iter()
                .filter(|db| visibility_checker.check_database_visibility(&ctl_name, db.name()))
                .collect::<Vec<_>>();

            for db in final_dbs {
                catalog_names.push(ctl_name.clone().into_bytes());
                let db_name = db.name().to_string().into_bytes();
                db_names.push(db_name);
                let id = db.get_db_info().ident.db_id;
                db_id.push(id);
            }
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(catalog_names),
            StringType::from_data(db_names),
            UInt64Type::from_data(db_id),
        ]))
    }
}

impl DatabasesTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("catalog", TableDataType::String),
            TableField::new("name", TableDataType::String),
            TableField::new("database_id", TableDataType::Number(NumberDataType::UInt64)),
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
