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

use databend_common_catalog::session_type::SessionType;
use databend_common_catalog::table::DistributionLevel;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::UInt64Type;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_storages_system::SyncOneBlockSystemTable;
use databend_common_storages_system::SyncSystemTable;

use crate::servers::http::v1::ClientSessionManager;
use crate::sessions::SessionManager;

pub struct TemporaryTablesTable {
    table_info: TableInfo,
}

impl SyncSystemTable for TemporaryTablesTable {
    const NAME: &'static str = "system.temporary_tables";

    const DISTRIBUTION_LEVEL: DistributionLevel = DistributionLevel::Cluster;

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(&self, ctx: Arc<dyn TableContext>) -> Result<DataBlock> {
        let node_id = ctx.get_cluster().local_id.clone();

        let mut dbs = Vec::new();
        let mut names = Vec::new();
        let mut table_ids = Vec::new();
        let mut engines = Vec::new();
        let mut users = Vec::new();
        let mut session_ids = Vec::new();
        let mut session_types = Vec::new();
        let mut is_current_sessions = Vec::new();
        let mut created_ons = Vec::new();
        let mut updated_ons = Vec::new();
        let mut compressed_data_bytes_col = Vec::new();
        let mut index_bytes_col = Vec::new();
        let mut number_of_rows_col = Vec::new();
        let mut number_of_blocks_col = Vec::new();
        let mut number_of_segments_col = Vec::new();

        let mysql_temp_tables = {
            let session_manager = SessionManager::instance();
            session_manager
                .get_all_temp_tables()?
                .into_iter()
                .filter(|(_, typ, _)| typ == &SessionType::MySQL)
                .collect::<Vec<_>>()
        };

        let http_client_temp_tables = {
            let client_session_manager = ClientSessionManager::instance();
            client_session_manager
                .get_all_temp_tables()?
                .into_iter()
                .map(|(session_key, table)| (session_key, SessionType::HTTPQuery, table))
                .collect::<Vec<_>>()
        };

        let all_temp_tables = mysql_temp_tables
            .into_iter()
            .chain(http_client_temp_tables)
            .collect::<Vec<_>>();

        let current_session_type = ctx.get_session_type();

        // currently http clients have their own client side session ids, besides server side session ids
        let current_client_session_id = ctx.get_current_client_session_id();
        let current_session_id = ctx.get_current_session_id();
        log::info!("current_client_session_id: {:?}", current_client_session_id);

        for (session_key, typ, table) in all_temp_tables {
            log::info!("session_key: {:?}", session_key);
            let desc = table.desc;
            let db_name = desc
                .split('.')
                .next()
                .and_then(|s| {
                    if s.starts_with('\'') && s.ends_with('\'') {
                        Some(&s[1..s.len() - 1])
                    } else {
                        None
                    }
                })
                .ok_or_else(|| format!("Invalid table desc: {}", desc))?;

            let user = session_key.split('/').next().unwrap().to_string();
            let session_id = session_key.split('/').nth(1).unwrap().to_string();
            let is_current_session = {
                if current_session_type == SessionType::HTTPQuery {
                    current_client_session_id
                        .as_ref()
                        .map(|id| id == &session_id)
                        .unwrap_or(false)
                } else {
                    current_session_id == session_id
                }
            };

            let meta = table.meta;
            dbs.push(db_name.to_string());
            names.push(table.name);
            table_ids.push(table.ident.table_id);
            engines.push(meta.engine);
            users.push(user);
            session_ids.push(session_id);
            session_types.push(typ.to_string());
            is_current_sessions.push(is_current_session);
            created_ons.push(meta.created_on.timestamp_micros());
            updated_ons.push(meta.updated_on.timestamp_micros());
            compressed_data_bytes_col.push(meta.statistics.compressed_data_bytes);
            index_bytes_col.push(meta.statistics.index_data_bytes);
            number_of_rows_col.push(meta.statistics.number_of_rows);
            number_of_segments_col.push(meta.statistics.number_of_segments);
            number_of_blocks_col.push(meta.statistics.number_of_blocks);
        }

        let mut block = DataBlock::new_from_columns(vec![
            StringType::from_data(dbs),
            StringType::from_data(names),
            UInt64Type::from_data(table_ids),
            StringType::from_data(engines),
            StringType::from_data(users),
            StringType::from_data(session_ids),
            StringType::from_data(session_types),
            BooleanType::from_data(is_current_sessions),
            TimestampType::from_data(created_ons),
            TimestampType::from_data(updated_ons),
            UInt64Type::from_data(compressed_data_bytes_col),
            UInt64Type::from_data(index_bytes_col),
            UInt64Type::from_data(number_of_rows_col),
            UInt64Type::from_opt_data(number_of_segments_col),
            UInt64Type::from_opt_data(number_of_blocks_col),
        ]);

        block.add_const_column(Scalar::String(node_id), DataType::String);
        Ok(block)
    }
}

impl TemporaryTablesTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("database", TableDataType::String),
            TableField::new("name", TableDataType::String),
            TableField::new("table_id", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("engine", TableDataType::String),
            TableField::new("user", TableDataType::String),
            TableField::new("session_id", TableDataType::String),
            TableField::new("session_type", TableDataType::String),
            TableField::new("is_current_session", TableDataType::Boolean),
            TableField::new("created_on", TableDataType::Timestamp),
            TableField::new("updated_on", TableDataType::Timestamp),
            TableField::new(
                "compressed_data_bytes",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "index_data_bytes",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new("num_rows", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new(
                "num_segments",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "num_blocks",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new("node", TableDataType::String),
        ]);
        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: "system.temporary_tables".to_string(),
            name: "temporary_tables".to_string(),
            meta: TableMeta {
                schema,
                engine: "SystemTemporaryTables".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        SyncOneBlockSystemTable::create(Self { table_info })
    }
}
