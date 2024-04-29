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

use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::Thread;
use databend_common_exception::Result;
use log::info;

use crate::pipelines::executor::QueriesPipelineExecutor;

pub struct GlobalQueriesExecutor(pub QueriesPipelineExecutor);

impl GlobalQueriesExecutor {
    pub fn init() -> Result<()> {
        let num_cpus = num_cpus::get();
        GlobalInstance::set(QueriesPipelineExecutor::create(num_cpus)?);
        Thread::spawn(|| {
            if let Err(e) = Self::instance().execute() {
                info!("Executor finished with error: {:?}", e);
            }
        });
        Ok(())
    }

    pub fn instance() -> Arc<QueriesPipelineExecutor> {
        GlobalInstance::get()
    }
}
