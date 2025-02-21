// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(clippy::uninlined_format_args)]
#![feature(try_blocks)]

mod entry;

use databend_common_base::mem_allocator::GlobalAllocator;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::ThreadTracker;
use databend_common_config::InnerConfig;
use databend_common_exception::Result;
use databend_common_exception::ResultExt;
use databend_common_tracing::pipe_file;
use databend_common_tracing::set_crash_hook;
use databend_common_tracing::SignalListener;
use databend_enterprise_query::enterprise_services::EnterpriseServices;
use entry::MainError;

use crate::entry::init_services;
use crate::entry::run_cmd;
use crate::entry::start_services;

#[global_allocator]
pub static GLOBAL_ALLOCATOR: GlobalAllocator = GlobalAllocator::create();

fn main() {
    let binary_version = (*databend_common_config::DATABEND_COMMIT_VERSION).clone();

    // Crash tracker
    let (input, output) = pipe_file().unwrap();
    set_crash_hook(output);
    SignalListener::spawn(input, binary_version);

    // Thread tracker
    ThreadTracker::init();

    match Runtime::with_default_worker_threads() {
        Err(cause) => {
            eprintln!("Databend Query start failure, cause: {:?}", cause);
            std::process::exit(cause.code() as i32);
        }
        Ok(rt) => {
            if let Err(cause) = rt.block_on(main_entrypoint()) {
                eprintln!("Databend Query start failure, cause: {:?}", cause);
                std::process::exit(cause.code() as i32);
            }
        }
    }
}

pub async fn main_entrypoint() -> Result<(), MainError> {
    let make_error = || "an fatal error occurred in query";

    let conf: InnerConfig = InnerConfig::load().await.with_context(make_error)?;
    if run_cmd(&conf).await.with_context(make_error)? {
        return Ok(());
    }

    init_services(&conf, true).await.with_context(make_error)?;
    EnterpriseServices::init(conf.clone())
        .await
        .with_context(make_error)?;
    start_services(&conf).await.with_context(make_error)
}
