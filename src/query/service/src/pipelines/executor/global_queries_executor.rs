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
use std::sync::OnceLock;

use databend_common_base::runtime::Thread;
use databend_common_exception::Result;
use log::info;

use crate::pipelines::executor::QueriesPipelineExecutor;

pub struct GlobalQueriesExecutor(pub QueriesPipelineExecutor);

static GLOBAL_QUERIES_EXECUTOR: OnceLock<Arc<QueriesPipelineExecutor>> = OnceLock::new();

impl GlobalQueriesExecutor {
    fn init_once() -> Result<Arc<QueriesPipelineExecutor>> {
        let num_cpus = num_cpus::get();
        let executor = QueriesPipelineExecutor::create(num_cpus)?;
        let executor_clone = executor.clone();
        Thread::named_spawn(Some("GlobalQueriesExecutor".to_string()), move || {
            if let Err(e) = executor.execute() {
                info!("Executor finished with error: {:?}", e);
            }
        });
        Ok(executor_clone)
    }

    pub fn instance() -> Arc<QueriesPipelineExecutor> {
        GLOBAL_QUERIES_EXECUTOR
            .get_or_init(|| match Self::init_once() {
                Ok(executor) => executor,
                Err(e) => {
                    panic!("Failed to initialize GlobalQueriesExecutor: {:?}", e);
                }
            })
            .clone()
    }
}
