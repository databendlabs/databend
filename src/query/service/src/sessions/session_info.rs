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
use std::time::SystemTime;

use databend_common_base::base::ProgressValues;
pub use databend_common_catalog::table_context::ProcessInfo;
use databend_common_catalog::table_context::ProcessInfoState;
use databend_common_storage::StorageMetrics;

use crate::sessions::Session;
use crate::sessions::SessionContext;
use crate::sessions::SessionType;

impl Session {
    pub fn process_info(self: &Arc<Self>) -> ProcessInfo {
        self.to_process_info()
    }

    fn to_process_info(self: &Arc<Self>) -> ProcessInfo {
        let session_ctx = self.session_ctx.as_ref();

        let mut memory_usage = 0;

        let shared_query_context = &session_ctx.get_query_context_shared();
        if let Some(shared) = shared_query_context {
            if let Some(runtime) = shared.get_runtime() {
                let mem_stat = runtime.get_tracker();
                memory_usage = mem_stat.get_memory_usage();
            }
        }

        ProcessInfo {
            id: self.id.clone(),
            typ: self.get_type().to_string(),
            state: self.process_state(session_ctx),
            database: session_ctx.get_current_database(),
            user: session_ctx.get_current_user(),
            settings: self.get_settings(),
            client_address: session_ctx.get_client_host(),
            session_extra_info: self.process_extra_info(session_ctx),
            memory_usage,
            data_metrics: Self::query_data_metrics(session_ctx),
            scan_progress_value: Self::query_scan_progress_value(session_ctx),
            mysql_connection_id: self.mysql_connection_id,
            created_time: Self::query_created_time(session_ctx),
            status_info: shared_query_context
                .as_ref()
                .map(|qry_ctx| qry_ctx.get_status_info()),
            current_query_id: self.get_current_query_id(),
        }
    }

    fn process_state(self: &Arc<Self>, status: &SessionContext) -> ProcessInfoState {
        match status.get_query_context_shared() {
            _ if status.get_abort() => ProcessInfoState::Aborting,
            None => ProcessInfoState::Idle,
            Some(_) => ProcessInfoState::Query,
        }
    }

    fn process_extra_info(self: &Arc<Self>, status: &SessionContext) -> Option<String> {
        match self.get_type() {
            SessionType::FlightRPC => Session::rpc_extra_info(status),
            _ => Session::query_extra_info(status),
        }
    }

    fn rpc_extra_info(status: &SessionContext) -> Option<String> {
        status
            .get_query_context_shared()
            .map(|_| String::from("Partial cluster query stage"))
    }

    fn query_extra_info(status: &SessionContext) -> Option<String> {
        status
            .get_query_context_shared()
            .as_ref()
            .map(|context_shared| context_shared.get_query_str())
    }

    fn query_data_metrics(status: &SessionContext) -> Option<StorageMetrics> {
        status
            .get_query_context_shared()
            .as_ref()
            .map(|context_shared| context_shared.get_data_metrics())
    }

    fn query_scan_progress_value(status: &SessionContext) -> Option<ProgressValues> {
        status
            .get_query_context_shared()
            .as_ref()
            .map(|context_shared| context_shared.scan_progress.get_values())
    }

    fn query_created_time(status: &SessionContext) -> SystemTime {
        match status.get_query_context_shared() {
            None => SystemTime::now(),
            Some(v) => v.created_time,
        }
    }
}
