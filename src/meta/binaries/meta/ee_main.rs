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

#![feature(stmt_expr_attributes)]
#![allow(clippy::collapsible_if, clippy::uninlined_format_args)]
mod entry;

use databend_common_base::mem_allocator::DefaultGlobalAllocator;
use databend_meta::configs::Config;
use databend_meta_runtime::DatabendRuntime;

#[global_allocator]
pub static GLOBAL_ALLOCATOR: DefaultGlobalAllocator = DefaultGlobalAllocator::create();

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let conf = Config::load()?;
    conf.validate()?;
    entry::entry::<DatabendRuntime>(conf).await
}
