// Copyright 2021 Datafuse Labs.
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

use std::any::Any;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_planners::ReadDataSourcePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::sessions::QueryContext;
use crate::storages::Table;

pub struct QueryLogTable {
    table_info: TableInfo,
}

impl QueryLogTable {
    pub fn create(table_id: u64) -> Self {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("type", DataType::Int8, false),
            DataField::new("tenant_id", DataType::String, false),
            DataField::new("cluster_id", DataType::String, false),
            DataField::new("sql_user", DataType::String, false),
            DataField::new("sql_user_privileges", DataType::String, false),
            DataField::new("sql_user_quota", DataType::String, false),
            DataField::new("client_address", DataType::String, false),
            DataField::new("query_id", DataType::String, false),
            DataField::new("query_text", DataType::String, false),
            DataField::new("query_start_time", DataType::DateTime32(None), false),
            DataField::new("query_end_time", DataType::DateTime32(None), false),
            DataField::new("written_rows", DataType::UInt64, false),
            DataField::new("written_bytes", DataType::UInt64, false),
            DataField::new("read_rows", DataType::UInt64, false),
            DataField::new("read_bytes", DataType::UInt64, false),
            DataField::new("result_rows", DataType::UInt64, false),
            DataField::new("result_result", DataType::UInt64, false),
            DataField::new("memory_usage", DataType::UInt64, false),
            DataField::new("cpu_usage", DataType::UInt32, false),
            DataField::new("exception_code", DataType::Int32, false),
            DataField::new("exception", DataType::String, false),
            DataField::new("client_info", DataType::String, false),
            DataField::new("current_database", DataType::String, false),
            DataField::new("databases", DataType::String, false),
            DataField::new("columns", DataType::String, false),
            DataField::new("projections", DataType::String, false),
            DataField::new("server_version", DataType::String, false),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'query_log'".to_string(),
            name: "query_log".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemQueryLog".to_string(),
                ..Default::default()
            },
        };
        QueryLogTable { table_info }
    }
}

#[async_trait::async_trait]
impl Table for QueryLogTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read(
        &self,
        ctx: Arc<QueryContext>,
        _plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let query_logs = ctx
            .get_sessions_manager()
            .get_query_log_memory_store()
            .list_query_logs();

        let query_log_type_vec: Vec<i8> = query_logs.iter().map(|x| x.query_log_type).collect();
        let tenant_id_vec: Vec<&str> = query_logs.iter().map(|x| x.tenant_id.as_str()).collect();
        let cluster_id_vev: Vec<&str> = query_logs.iter().map(|x| x.cluster_id.as_str()).collect();
        let sql_user_vec: Vec<&str> = query_logs.iter().map(|x| x.sql_user.as_str()).collect();
        let sql_user_privileges_vec: Vec<&str> = query_logs
            .iter()
            .map(|x| x.sql_user_privileges.as_str())
            .collect();
        let sql_user_quota_vec: Vec<&str> = query_logs
            .iter()
            .map(|x| x.sql_user_quota.as_str())
            .collect();
        let client_address_vec: Vec<&str> = query_logs
            .iter()
            .map(|x| x.client_address.as_str())
            .collect();
        let query_id_vec: Vec<&str> = query_logs.iter().map(|x| x.query_id.as_str()).collect();
        let query_text_vec: Vec<&str> = query_logs.iter().map(|x| x.query_text.as_str()).collect();
        let query_start_time_vec: Vec<u32> =
            query_logs.iter().map(|x| x.query_start_time).collect();
        let query_end_time_vec: Vec<u32> = query_logs.iter().map(|x| x.query_end_time).collect();
        let written_rows_vec: Vec<u64> = query_logs.iter().map(|x| x.written_rows).collect();
        let written_bytes_vec: Vec<u64> = query_logs.iter().map(|x| x.written_bytes).collect();
        let read_rows_vec: Vec<u64> = query_logs.iter().map(|x| x.read_rows).collect();
        let read_bytes_vec: Vec<u64> = query_logs.iter().map(|x| x.read_bytes).collect();
        let result_rows_vec: Vec<u64> = query_logs.iter().map(|x| x.result_rows).collect();
        let result_result_vec: Vec<u64> = query_logs.iter().map(|x| x.result_result).collect();
        let memory_usage_vec: Vec<u64> = query_logs.iter().map(|x| x.memory_usage).collect();
        let cpu_usage_vec: Vec<u32> = query_logs.iter().map(|x| x.cpu_usage).collect();
        let exception_code_vec: Vec<i32> = query_logs.iter().map(|x| x.exception_code).collect();
        let exception_vec: Vec<&str> = query_logs.iter().map(|x| x.exception.as_str()).collect();
        let client_info_vec: Vec<&str> =
            query_logs.iter().map(|x| x.client_info.as_str()).collect();
        let current_database_vec: Vec<&str> = query_logs
            .iter()
            .map(|x| x.current_database.as_str())
            .collect();
        let databases_vec: Vec<&str> = query_logs.iter().map(|x| x.databases.as_str()).collect();
        let columns_vec: Vec<&str> = query_logs.iter().map(|x| x.columns.as_str()).collect();
        let projections_vec: Vec<&str> =
            query_logs.iter().map(|x| x.projections.as_str()).collect();
        let server_version_vec: Vec<&str> = query_logs
            .iter()
            .map(|x| x.server_version.as_str())
            .collect();
        let block = DataBlock::create_by_array(self.table_info.schema(), vec![
            Series::new(query_log_type_vec),
            Series::new(tenant_id_vec),
            Series::new(cluster_id_vev),
            Series::new(sql_user_vec),
            Series::new(sql_user_privileges_vec),
            Series::new(sql_user_quota_vec),
            Series::new(client_address_vec),
            Series::new(query_id_vec),
            Series::new(query_text_vec),
            Series::new(query_start_time_vec),
            Series::new(query_end_time_vec),
            Series::new(written_rows_vec),
            Series::new(written_bytes_vec),
            Series::new(read_rows_vec),
            Series::new(read_bytes_vec),
            Series::new(result_rows_vec),
            Series::new(result_result_vec),
            Series::new(memory_usage_vec),
            Series::new(cpu_usage_vec),
            Series::new(exception_code_vec),
            Series::new(exception_vec),
            Series::new(client_info_vec),
            Series::new(current_database_vec),
            Series::new(databases_vec),
            Series::new(columns_vec),
            Series::new(projections_vec),
            Series::new(server_version_vec),
        ]);
        Ok(Box::pin(DataBlockStream::create(
            self.table_info.schema(),
            None,
            vec![block],
        )))
    }
}
