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

#![allow(clippy::uninlined_format_args)]
#![feature(try_blocks)]

mod cmd;
mod entry;

use clap::Parser;
use databend_common_base::mem_allocator::TrackingGlobalAllocator;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::ThreadTracker;
use databend_common_exception::Result;
use databend_common_exception::ResultExt;
use databend_common_license::license_manager::LicenseManager;
use databend_common_license::license_manager::OssLicenseManager;
use databend_common_tracing::SignalListener;
use databend_common_tracing::pipe_file;
use databend_common_tracing::set_crash_hook;
use databend_common_version::DATABEND_COMMIT_VERSION;
use entry::MainError;

use self::cmd::Cmd;
use crate::entry::init_services;
use crate::entry::run_cmd;
use crate::entry::start_services;

#[global_allocator]
pub static GLOBAL_ALLOCATOR: TrackingGlobalAllocator = TrackingGlobalAllocator::create();

fn main() {
    // Crash tracker
    let (input, output) = pipe_file().unwrap();
    set_crash_hook(output);
    SignalListener::spawn(input, DATABEND_COMMIT_VERSION.to_string());

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

async fn main_entrypoint() -> Result<(), MainError> {
    let make_error = || "an fatal error occurred in query";

    // if the usage is print, std::process::exit() will be called.
    let mut cmd = Cmd::parse();
    cmd.normalize();

    if run_cmd(&cmd).await.with_context(make_error)? {
        return Ok(());
    }

    let conf = cmd.init_inner_config(true).await.with_context(make_error)?;
    init_services(&conf, false).await?;
    // init oss license manager
    OssLicenseManager::init(conf.query.tenant_id.tenant_name().to_string())
        .with_context(make_error)?;
    start_services(&conf).await
}
