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

use databend_common_catalog::{catalog::CatalogManager, plan::PushDownInfo, table::Table};
use databend_common_expression::{types::StringType, DataBlock, FromData, TableDataType, TableField, TableSchemaRefExt};
use databend_common_meta_app::schema::{ListDictionaryReq, TableIdent, TableInfo, TableMeta};
use databend_common_catalog::table_context::TableContext;
use databend_common_users::UserApiProvider;

use crate::table::{AsyncOneBlockSystemTable, AsyncSystemTable};

pub struct DictionariesTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for DictionariesTable {
    const NAME: &'static str = "system.dictionaries";

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
        let catalog_name = ctx.get_current_catalog();
        let catalog = ctx.get_catalog(catalog_name.as_str()).await?;

        let db_name = ctx.get_current_database();
        let database = catalog.get_database(&tenant, db_name.as_str()).await?;
        let db_id = database.get_db_info().database_id;
        let req = ListDictionaryReq {
            tenant: tenant.clone(),
            db_id
        };
        let dict_metas = catalog.list_dictionaries(req).await?;
        let dict_names: Vec<String> = dict_metas.iter().map(|(string, _)| *string).collect();
        return Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(dict_names),
        ]))
    }
}

impl DictionariesTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("name", TableDataType::String),
        ]);

        let table_info = TableInfo {
            desc: "dictionaries".to_string(),
            name: "dictionaries".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(DictionariesTable { table_info })
    }
}