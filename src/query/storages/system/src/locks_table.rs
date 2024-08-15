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

use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::number::UInt64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::utils::FromData;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::ListLocksReq;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;
use crate::util::find_eq_filter;

pub struct LocksTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for LocksTable {
    const NAME: &'static str = "system.locks";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();
        let catalog_mgr = CatalogManager::instance();
        let ctls = catalog_mgr.list_catalogs(&tenant, ctx.txn_mgr()).await?;

        let mut lock_table_id = Vec::new();
        let mut lock_revision = Vec::new();
        let mut lock_type = Vec::new();
        let mut lock_status = Vec::new();
        let mut lock_user = Vec::new();
        let mut lock_node = Vec::new();
        let mut lock_query_id = Vec::new();
        let mut lock_created_on = Vec::new();
        let mut lock_acquired_on = Vec::new();
        let mut lock_extra_info = Vec::new();
        for ctl in ctls.into_iter() {
            let mut table_ids = Vec::new();
            if let Some(push_downs) = &push_downs {
                if let Some(filter) = push_downs.filters.as_ref().map(|f| &f.filter) {
                    let expr = filter.as_expr(&BUILTIN_FUNCTIONS);
                    find_eq_filter(&expr, &mut |col_name, scalar| {
                        if col_name == "table_id" {
                            if let Scalar::Number(s) = scalar {
                                if let Some(v) = s.as_u_int64() {
                                    if !table_ids.contains(v) {
                                        table_ids.push(*v);
                                    }
                                }
                            }
                        }
                        Ok(())
                    });
                }
            }

            let req = if table_ids.is_empty() {
                ListLocksReq::create(&tenant)
            } else {
                ListLocksReq::create_with_table_ids(&tenant, table_ids)
            };
            let lock_infos = ctl.list_locks(req).await?;
            for info in lock_infos {
                lock_table_id.push(info.table_id);
                lock_revision.push(info.revision);
                lock_type.push(info.meta.lock_type.to_string().clone());
                if info.meta.acquired_on.is_some() {
                    lock_status.push("HOLDING");
                } else {
                    lock_status.push("WAITING");
                }
                lock_user.push(info.meta.user.clone());
                lock_node.push(info.meta.node.clone());
                lock_query_id.push(info.meta.query_id.clone());
                lock_created_on.push(info.meta.created_on.timestamp_micros());
                lock_acquired_on.push(info.meta.acquired_on.map(|v| v.timestamp_micros()));
                if info.meta.extra_info.is_empty() {
                    lock_extra_info.push("".to_string());
                } else {
                    lock_extra_info.push(format!("{:?}", info.meta.extra_info));
                }
            }
        }
        Ok(DataBlock::new_from_columns(vec![
            UInt64Type::from_data(lock_table_id),
            UInt64Type::from_data(lock_revision),
            StringType::from_data(lock_type),
            StringType::from_data(lock_status),
            StringType::from_data(lock_user),
            StringType::from_data(lock_node),
            StringType::from_data(lock_query_id),
            TimestampType::from_data(lock_created_on),
            TimestampType::from_opt_data(lock_acquired_on),
            StringType::from_data(lock_extra_info),
        ]))
    }
}

impl LocksTable {
    pub fn schema() -> TableSchemaRef {
        TableSchemaRefExt::create(vec![
            TableField::new("table_id", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("revision", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("type", TableDataType::String),
            TableField::new("status", TableDataType::String),
            TableField::new("user", TableDataType::String),
            TableField::new("node", TableDataType::String),
            TableField::new("query_id", TableDataType::String),
            TableField::new("created_on", TableDataType::Timestamp),
            TableField::new(
                "acquired_on",
                TableDataType::Nullable(Box::new(TableDataType::Timestamp)),
            ),
            TableField::new("extra_info", TableDataType::String),
        ])
    }

    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let table_info = TableInfo {
            desc: "'system'.'locks'".to_string(),
            name: "locks".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema: LocksTable::schema(),
                engine: "SystemLocks".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        AsyncOneBlockSystemTable::create(LocksTable { table_info })
    }
}
