use std::sync::Arc;

use databend_common_exception::ErrorCode;
use parking_lot::Mutex;

pub struct ErrorInfo {
    // processor id
    pub pid: usize,
    // processor name
    pub p_name: String,
    // plan id
    pub plan_id: Option<u32>,

    pub error: Mutex<Option<ErrorCode>>,
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
    pub node_error: Option<ErrorCode>,
}
