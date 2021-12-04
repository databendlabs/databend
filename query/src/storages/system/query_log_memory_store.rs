use std::collections::VecDeque;

use common_infallible::RwLock;

#[derive(Clone)]
pub struct QueryLog {
    pub(crate) query_log_type: i8,
    pub(crate) tenant_id: String,
    pub(crate) cluster_id: String,
    pub(crate) sql_user: String,
    pub(crate) sql_user_privileges: String,
    pub(crate) sql_user_quota: String,
    pub(crate) client_address: String,
    pub(crate) query_id: String,
    pub(crate) query_text: String,
    /// A 32-bit datetime representing the elapsed time since UNIX epoch (1970-01-01)
    /// in seconds, it's physical type is UInt32
    pub(crate) query_start_time: u32,
    /// A 32-bit datetime representing the elapsed time since UNIX epoch (1970-01-01)
    /// in seconds, it's physical type is UInt32
    pub(crate) query_end_time: u32,
    pub(crate) written_rows: u64,
    pub(crate) written_bytes: u64,
    pub(crate) read_rows: u64,
    pub(crate) read_bytes: u64,
    pub(crate) result_rows: u64,
    pub(crate) result_result: u64,
    pub(crate) memory_usage: u64,
    pub(crate) cpu_usage: u32,
    pub(crate) exception_code: i32,
    pub(crate) exception: String,
    pub(crate) client_info: String,
    pub(crate) current_database: String,
    pub(crate) databases: String,
    pub(crate) columns: String,
    pub(crate) projections: String,
    pub(crate) server_version: String,
}

pub struct QueryLogMemoryStore {
    query_logs: RwLock<VecDeque<QueryLog>>,
    size: usize,
}

impl QueryLogMemoryStore {
    pub fn create(size: usize) -> Self {
        QueryLogMemoryStore {
            query_logs: RwLock::new(VecDeque::new()),
            size,
        }
    }

    pub fn append_query_log(&self, log: QueryLog) {
        let mut query_logs = self.query_logs.write();
        if query_logs.len() == self.size {
            query_logs.pop_front();
        }
        query_logs.push_back(log);
    }

    pub fn append_query_logs(&self, logs: Vec<QueryLog>) {
        for log in logs {
            self.append_query_log(log);
        }
    }

    pub fn list_query_logs(&self) -> Vec<QueryLog> {
        let query_logs = self.query_logs.read();
        let mut result: Vec<QueryLog> = Vec::new();
        for log in query_logs.iter() {
            result.push(log.clone());
        }
        result
    }

    pub fn size(&self) -> usize {
        let query_logs = self.query_logs.read();
        query_logs.len()
    }
}
