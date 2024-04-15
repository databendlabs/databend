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

use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_storages_result_cache::gen_result_cache_prefix;
use databend_common_storages_result_cache::ResultCacheMetaManager;
use databend_common_users::UserApiProvider;
use itertools::Itertools;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub struct QueryCacheTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for QueryCacheTable {
    const NAME: &'static str = "system.query_cache";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let meta_client = UserApiProvider::instance().get_meta_store_client();
        let result_cache_mgr = ResultCacheMetaManager::create(meta_client, 0);
        let tenant = ctx.get_tenant();
        let prefix = gen_result_cache_prefix(tenant.tenant_name());

        let cached_values = result_cache_mgr.list(prefix.as_str()).await?;

        let mut sql_vec: Vec<&str> = Vec::with_capacity(cached_values.len());
        let mut query_id_vec: Vec<&str> = Vec::with_capacity(cached_values.len());
        let mut result_size_vec = Vec::with_capacity(cached_values.len());
        let mut num_rows_vec = Vec::with_capacity(cached_values.len());
        let mut partitions_sha_vec = Vec::with_capacity(cached_values.len());
        let mut location_vec = Vec::with_capacity(cached_values.len());
        let mut active_result_scan: Vec<bool> = Vec::with_capacity(cached_values.len());

        cached_values.iter().for_each(|x| {
            sql_vec.push(x.sql.as_str());
            query_id_vec.push(x.query_id.as_str());
            result_size_vec.push(x.result_size as u64);
            num_rows_vec.push(x.num_rows as u64);
            partitions_sha_vec.push(x.partitions_shas.clone());
            location_vec.push(x.location.as_str());
        });

        let active_query_ids = ctx.get_query_id_history();

        for qid in query_id_vec.iter() {
            if active_query_ids.contains(*qid) {
                active_result_scan.push(true)
            } else {
                active_result_scan.push(false)
            }
        }

        let partitions_sha_vec: Vec<String> = partitions_sha_vec
            .into_iter()
            .map(|part| part.into_iter().join(", "))
            .collect();

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(sql_vec),
            StringType::from_data(query_id_vec),
            UInt64Type::from_data(result_size_vec),
            UInt64Type::from_data(num_rows_vec),
            StringType::from_data(
                partitions_sha_vec
                    .iter()
                    .map(|part_sha| part_sha.as_str())
                    .collect::<Vec<_>>(),
            ),
            StringType::from_data(location_vec),
            BooleanType::from_data(active_result_scan),
        ]))
    }
}

impl QueryCacheTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("sql", TableDataType::String),
            TableField::new("query_id", TableDataType::String),
            TableField::new("result_size", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("num_rows", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("partitions_sha", TableDataType::String),
            TableField::new("location", TableDataType::String),
            TableField::new("active_result_scan", TableDataType::Boolean),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'query_cache'".to_string(),
            name: "query_cache".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemQueryCache".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(QueryCacheTable { table_info })
    }
}
