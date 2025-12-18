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

#![feature(try_blocks)]
#![feature(thread_id_value)]
#![feature(buf_read_has_data_left)]
#![allow(clippy::uninlined_format_args)]

mod config;
mod crash_hook;
mod filter;
mod init;
mod loggers;
pub mod module_tag;
mod panic_hook;
mod remote_log;
mod structlog;

mod predefined_tables;
mod query_log_collector;

pub use crash_hook::SignalListener;
pub use crash_hook::pipe_file;

pub use crate::config::CONFIG_DEFAULT_LOG_LEVEL;
pub use crate::config::Config;
pub use crate::config::FileConfig;
pub use crate::config::HistoryConfig;
pub use crate::config::HistoryTableConfig;
pub use crate::config::LogFormat;
pub use crate::config::OTLPConfig;
pub use crate::config::OTLPEndpointConfig;
pub use crate::config::OTLPProtocol;
pub use crate::config::ProfileLogConfig;
pub use crate::config::QueryLogConfig;
pub use crate::config::StderrConfig;
pub use crate::config::StructLogConfig;
pub use crate::config::TracingConfig;
pub use crate::crash_hook::set_crash_hook;
pub use crate::init::GlobalLogger;
pub use crate::init::init_logging;
pub use crate::init::inject_span_to_tonic_request;
pub use crate::init::start_trace_for_remote_request;
pub use crate::panic_hook::log_panic;
pub use crate::panic_hook::set_panic_hook;
pub use crate::predefined_tables::HistoryTable;
pub use crate::predefined_tables::get_all_history_table_names;
pub use crate::predefined_tables::init_history_tables;
pub use crate::remote_log::LogBuffer as RemoteLogBuffer;
pub use crate::remote_log::LogMessage;
pub use crate::remote_log::RemoteLog;
pub use crate::remote_log::RemoteLogElement;
pub use crate::remote_log::RemoteLogGuard;
pub use crate::remote_log::convert_to_batch;
pub use crate::structlog::DummyReporter;
pub use crate::structlog::StructLogReporter;
pub fn closure_name<F: std::any::Any>() -> &'static str {
    let func_path = std::any::type_name::<F>();
    func_path
        .rsplit("::")
        .find(|name| *name != "{{closure}}")
        .unwrap()
}
