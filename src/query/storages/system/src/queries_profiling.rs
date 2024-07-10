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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::types::VariantType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline_core::PlanProfile;

use crate::SyncOneBlockSystemTable;
use crate::SyncSystemTable;
use crate::SystemLogElement;
use crate::SystemLogQueue;

pub struct QueriesProfilingTable {
    table_info: TableInfo,
}

impl SyncSystemTable for QueriesProfilingTable {
    const NAME: &'static str = "system.queries_profiling";

    const IS_LOCAL: bool = false;

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(&self, ctx: Arc<dyn TableContext>) -> Result<DataBlock> {
        let queries_profiles = ctx.get_queries_profile();

        let cache_queue = ProfilesCacheQueue::instance()?;
        let cache_profiles: Vec<ProfilesCacheElement> = cache_queue
            .data
            .read()
            .event_queue
            .iter()
            .flatten()
            .cloned()
            .collect();

        let local_id = ctx.get_cluster().local_id.clone();
        let total_size =
            queries_profiles.values().map(Vec::len).sum::<usize>() + cache_profiles.len();

        let mut node: Vec<String> = Vec::with_capacity(total_size);
        let mut queries_id: Vec<String> = Vec::with_capacity(total_size);
        let mut plan_id: Vec<Option<u32>> = Vec::with_capacity(total_size);
        let mut parent_id: Vec<Option<u32>> = Vec::with_capacity(total_size);
        let mut plan_name: Vec<Option<String>> = Vec::with_capacity(total_size);
        let mut statistics = Vec::with_capacity(total_size);

        for (query_id, query_profiles) in queries_profiles {
            for query_plan_profile in query_profiles {
                node.push(local_id.clone());
                queries_id.push(query_id.clone());
                plan_id.push(query_plan_profile.id);
                parent_id.push(query_plan_profile.parent_id);
                plan_name.push(query_plan_profile.name.clone());

                let mut statistics_map =
                    HashMap::with_capacity(query_plan_profile.statistics.len());
                for (idx, item_value) in query_plan_profile.statistics.iter().enumerate() {
                    statistics_map
                        .insert(ProfileStatisticsName::from(idx).to_string(), *item_value);
                }

                statistics.push(serde_json::to_vec(&statistics_map).unwrap());
            }
        }

        for cache_element in cache_profiles {
            let query_id = cache_element.query_id;
            for query_plan_profile in cache_element.profiles {
                node.push(local_id.clone());
                queries_id.push(query_id.clone());
                plan_id.push(query_plan_profile.id);
                parent_id.push(query_plan_profile.parent_id);
                plan_name.push(query_plan_profile.name.clone());

                let mut statistics_map =
                    HashMap::with_capacity(query_plan_profile.statistics.len());
                for (idx, item_value) in query_plan_profile.statistics.iter().enumerate() {
                    statistics_map
                        .insert(ProfileStatisticsName::from(idx).to_string(), *item_value);
                }

                statistics.push(serde_json::to_vec(&statistics_map).unwrap());
            }
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(node),
            StringType::from_data(queries_id),
            UInt32Type::from_opt_data(plan_id),
            UInt32Type::from_opt_data(parent_id),
            StringType::from_opt_data(plan_name),
            VariantType::from_data(statistics),
        ]))
    }
}

impl QueriesProfilingTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("node", TableDataType::String),
            TableField::new("query_id", TableDataType::String),
            TableField::new(
                "plan_id",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt32))),
            ),
            TableField::new(
                "parent_plan_id",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt32))),
            ),
            TableField::new(
                "plan_name",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new("statistics", TableDataType::Variant),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'queries_profiling'".to_string(),
            ident: TableIdent::new(table_id, 0),
            name: "queries_profiling".to_string(),
            meta: TableMeta {
                schema,
                engine: "QueriesProfilingTable".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        SyncOneBlockSystemTable::create(Self { table_info })
    }
}

#[derive(Clone)]
pub struct ProfilesCacheElement {
    pub query_id: String,
    pub profiles: Vec<PlanProfile>,
}

impl SystemLogElement for ProfilesCacheElement {
    const TABLE_NAME: &'static str = "profiles_cache_not_table";
    fn schema() -> TableSchemaRef {
        unreachable!()
    }
    fn fill_to_data_block(
        &self,
        _: &mut Vec<ColumnBuilder>,
    ) -> databend_common_exception::Result<()> {
        unreachable!()
    }
}

pub type ProfilesCacheQueue = SystemLogQueue<ProfilesCacheElement>;
