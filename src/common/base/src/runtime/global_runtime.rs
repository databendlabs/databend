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

use databend_common_exception::Result;

use crate::base::GlobalInstance;
use crate::runtime::Runtime;

pub struct GlobalIORuntime;

pub struct GlobalQueryRuntime(pub Runtime);

impl GlobalQueryRuntime {
    #[inline(always)]
    pub fn runtime<'a>(self: &'a Arc<Self>) -> &'a Runtime {
        &self.0
    }
}

impl GlobalIORuntime {
    pub fn init(num_cpus: usize) -> Result<()> {
        let thread_num = std::cmp::max(num_cpus, num_cpus::get() / 2);
        let thread_num = std::cmp::max(2, thread_num);

        GlobalInstance::set(Arc::new(Runtime::with_worker_threads(
            thread_num,
            Some("IO-worker".to_owned()),
        )?));
        Ok(())
    }

    pub fn instance() -> Arc<Runtime> {
        GlobalInstance::get()
    }
}

impl GlobalQueryRuntime {
    pub fn init(num_cpus: usize) -> Result<()> {
        let thread_num = std::cmp::max(num_cpus, num_cpus::get() / 2);
        let thread_num = std::cmp::max(2, thread_num);
        // 20MB
        let thread_stack_size = 20 * 1024 * 1024;

        let rt = Runtime::with_worker_threads_stack_size(
            thread_num,
            Some("g-query-worker".to_owned()),
            Some(thread_stack_size),
        )?;

        GlobalInstance::set(Arc::new(GlobalQueryRuntime(rt)));
        Ok(())
    }

    pub fn instance() -> Arc<GlobalQueryRuntime> {
        GlobalInstance::get()
    }
}
