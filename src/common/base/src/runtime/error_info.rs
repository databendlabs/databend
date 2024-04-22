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

use databend_common_exception::ErrorCode;
use parking_lot::Mutex;

#[derive(Clone)]
pub enum NodeErrorType {
    ScheduleEventError(ErrorCode),
    SyncProcessError(ErrorCode),
    AsyncProcessError(ErrorCode),
    LocalError(ErrorCode),
}

impl NodeErrorType {
    pub fn get_error_code(&self) -> ErrorCode {
        match self {
            NodeErrorType::ScheduleEventError(error) => error,
            NodeErrorType::SyncProcessError(error) => error,
            NodeErrorType::AsyncProcessError(error) => error,
            NodeErrorType::LocalError(error) => error,
        }
        .clone()
    }
}

pub struct ErrorInfo {
    // processor id
    pub pid: usize,
    // processor name
    pub p_name: String,
    // plan id
    pub plan_id: Option<u32>,

    pub error: Mutex<Option<NodeErrorType>>,
}

impl ErrorInfo {
    pub fn create(pid: usize, p_name: String, plan_id: Option<u32>) -> Arc<Self> {
        Arc::new(ErrorInfo {
            pid,
            p_name,
            plan_id,
            error: Mutex::new(None),
        })
    }

    pub fn get_error(&self) -> Option<NodeErrorReport> {
        let guard = self.error.lock();
        let error = guard.clone();
        error.as_ref()?;
        Some(NodeErrorReport {
            pid: self.pid,
            p_name: self.p_name.clone(),
            plan_id: self.plan_id,
            node_error: error,
        })
    }
}

pub struct NodeErrorReport {
    pub pid: usize,
    pub p_name: String,
    pub plan_id: Option<u32>,
    pub node_error: Option<NodeErrorType>,
}
