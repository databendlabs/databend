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
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::dictionary_name_ident::DictionaryNameIdent;
use databend_common_meta_app::schema::DictionaryIdentity;
use databend_common_meta_app::schema::ListDictionaryReq;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

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

        let catalogs = CatalogManager::instance();
        let catalogs: Vec<(String, Arc<dyn Catalog>)> = catalogs
            .list_catalogs(&tenant, ctx.session_state())
            .await?
            .iter()
            .map(|e| (e.name(), e.clone()))
            .collect();
        let mut catalog_names = vec![];
        let mut db_names = vec![];
        let mut dict_names = vec![];
        let mut dict_ids = vec![];

        for (ctl_name, catalog) in catalogs.into_iter() {
            let databases = catalog.list_databases(&tenant).await?;

            for db in databases {
                catalog_names.push(ctl_name.clone());
                let db_name = db.name().to_string();
                db_names.push(db_name);
                let db_id = db.get_db_info().database_id.db_id;
                let req = ListDictionaryReq {
                    tenant: tenant.clone(),
                    db_id,
                };
                let dict_metas = catalog.list_dictionaries(req).await?;
                for dict_meta in dict_metas.iter() {
                    let (dict_name, _) = dict_meta;
                    dict_names.push(dict_name.clone());
                    let get_req = DictionaryNameIdent::new(
                        tenant.clone(),
                        DictionaryIdentity::new(db_id, dict_name),
                    );
                    if let Some(reply) = catalog.get_dictionary(get_req).await? {
                        dict_ids.push(reply.dictionary_id);
                    }
                }
            }
        }

        return Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(catalog_names),
            StringType::from_data(db_names),
            StringType::from_data(dict_names),
            UInt64Type::from_data(dict_ids),
        ]));
    }
}

impl DictionariesTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("catalog", TableDataType::String),
            TableField::new("database", TableDataType::String),
            TableField::new("name", TableDataType::String),
            TableField::new(
                "dictionary_id",
                TableDataType::Number(NumberDataType::UInt64),
            ),
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
