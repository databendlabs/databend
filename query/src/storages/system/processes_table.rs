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
use std::net::SocketAddr;
use std::sync::Arc;

use common_base::ProgressValues;
use common_dal_context::DalMetrics;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_meta_types::UserInfo;
use common_planners::ReadDataSourcePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::sessions::QueryContext;
use crate::storages::Table;

pub struct ProcessesTable {
    table_info: TableInfo,
}

impl ProcessesTable {
    pub fn create(table_id: u64) -> Self {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("id", Vu8::to_data_type()),
            DataField::new("type", Vu8::to_data_type()),
            DataField::new_nullable("host", Vu8::to_data_type()),
            DataField::new_nullable("user", Vu8::to_data_type()),
            DataField::new("state", Vu8::to_data_type()),
            DataField::new("database", Vu8::to_data_type()),
            DataField::new_nullable("extra_info", Vu8::to_data_type()),
            DataField::new_nullable("memory_usage", i64::to_data_type()),
            DataField::new_nullable("dal_metrics_read_bytes", u64::to_data_type()),
            DataField::new_nullable("dal_metrics_write_bytes", u64::to_data_type()),
            DataField::new_nullable("scan_progress_read_rows", u64::to_data_type()),
            DataField::new_nullable("scan_progress_read_bytes", u64::to_data_type()),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'processes'".to_string(),
            name: "processes".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemProcesses".to_string(),

                ..Default::default()
            },
        };
        ProcessesTable { table_info }
    }

    fn process_host(client_address: &Option<SocketAddr>) -> Option<Vec<u8>> {
        client_address.as_ref().map(|s| s.to_string().into_bytes())
    }

    fn process_user_info(user_info: &Option<UserInfo>) -> Option<Vec<u8>> {
        user_info.as_ref().map(|s| s.name.clone().into_bytes())
    }

    fn process_extra_info(session_extra_info: &Option<String>) -> Option<Vec<u8>> {
        session_extra_info.clone().map(|s| s.into_bytes())
    }

    fn process_dal_metrics(dal_metrics_opt: &Option<DalMetrics>) -> (Option<u64>, Option<u64>) {
        if dal_metrics_opt.is_some() {
            let dal_metrics = dal_metrics_opt.as_ref().unwrap();
            (
                Some(dal_metrics.read_bytes as u64),
                Some(dal_metrics.write_bytes as u64),
            )
        } else {
            (None, None)
        }
    }

    fn process_scan_progress_values(
        scan_progress_opt: &Option<ProgressValues>,
    ) -> (Option<u64>, Option<u64>) {
        if scan_progress_opt.is_some() {
            let scan_progress = scan_progress_opt.as_ref().unwrap();
            (
                Some(scan_progress.read_rows as u64),
                Some(scan_progress.read_bytes as u64),
            )
        } else {
            (None, None)
        }
    }
}

#[async_trait::async_trait]
impl Table for ProcessesTable {
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
        let processes_info = ctx.get_processes_info();

        let mut processes_id = Vec::with_capacity(processes_info.len());
        let mut processes_type = Vec::with_capacity(processes_info.len());
        let mut processes_host = Vec::with_capacity(processes_info.len());
        let mut processes_user = Vec::with_capacity(processes_info.len());
        let mut processes_state = Vec::with_capacity(processes_info.len());
        let mut processes_database = Vec::with_capacity(processes_info.len());
        let mut processes_extra_info = Vec::with_capacity(processes_info.len());
        let mut processes_memory_usage = Vec::with_capacity(processes_info.len());
        let mut processes_dal_metrics_read_bytes = Vec::with_capacity(processes_info.len());
        let mut processes_dal_metrics_write_bytes = Vec::with_capacity(processes_info.len());
        let mut processes_scan_progress_read_rows = Vec::with_capacity(processes_info.len());
        let mut processes_scan_progress_read_bytes = Vec::with_capacity(processes_info.len());

        for process_info in &processes_info {
            processes_id.push(process_info.id.clone().into_bytes());
            processes_type.push(process_info.typ.clone().into_bytes());
            processes_state.push(process_info.state.clone().into_bytes());
            processes_database.push(process_info.database.clone().into_bytes());
            processes_host.push(ProcessesTable::process_host(&process_info.client_address));
            processes_user.push(ProcessesTable::process_user_info(&process_info.user));
            processes_extra_info.push(ProcessesTable::process_extra_info(
                &process_info.session_extra_info,
            ));
            processes_memory_usage.push(process_info.memory_usage);
            let (dal_metrics_read_bytes, dal_metrics_write_bytes) =
                ProcessesTable::process_dal_metrics(&process_info.dal_metrics);
            processes_dal_metrics_read_bytes.push(dal_metrics_read_bytes);
            processes_dal_metrics_write_bytes.push(dal_metrics_write_bytes);
            let (scan_progress_read_rows, scan_progress_read_bytes) =
                ProcessesTable::process_scan_progress_values(&process_info.scan_progress_value);
            processes_scan_progress_read_rows.push(scan_progress_read_rows);
            processes_scan_progress_read_bytes.push(scan_progress_read_bytes);
        }

        let schema = self.table_info.schema();
        let block = DataBlock::create(schema.clone(), vec![
            Series::from_data(processes_id),
            Series::from_data(processes_type),
            Series::from_data(processes_host),
            Series::from_data(processes_user),
            Series::from_data(processes_state),
            Series::from_data(processes_database),
            Series::from_data(processes_extra_info),
            Series::from_data(processes_memory_usage),
            Series::from_data(processes_dal_metrics_read_bytes),
            Series::from_data(processes_dal_metrics_write_bytes),
            Series::from_data(processes_scan_progress_read_rows),
            Series::from_data(processes_scan_progress_read_bytes),
        ]);

        Ok(Box::pin(DataBlockStream::create(schema, None, vec![block])))
    }
}
