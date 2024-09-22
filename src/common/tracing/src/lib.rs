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
#![allow(clippy::uninlined_format_args)]

mod config;
mod crash_hook;
mod init;
mod loggers;
mod panic_hook;
mod structlog;

pub use crate::config::Config;
pub use crate::config::FileConfig;
pub use crate::config::OTLPConfig;
pub use crate::config::OTLPEndpointConfig;
pub use crate::config::OTLPProtocol;
pub use crate::config::ProfileLogConfig;
pub use crate::config::QueryLogConfig;
pub use crate::config::StderrConfig;
pub use crate::config::StructLogConfig;
pub use crate::config::TracingConfig;
pub use crate::crash_hook::set_crash_hook;
pub use crate::init::init_logging;
pub use crate::init::inject_span_to_tonic_request;
pub use crate::init::start_trace_for_remote_request;
pub use crate::init::GlobalLogger;
pub use crate::panic_hook::log_panic;
pub use crate::panic_hook::set_panic_hook;
pub use crate::structlog::DummyReporter;
pub use crate::structlog::StructLogReporter;

pub fn closure_name<F: std::any::Any>() -> &'static str {
    let func_path = std::any::type_name::<F>();
    func_path
        .rsplit("::")
        .find(|name| *name != "{{closure}}")
        .unwrap()
}
