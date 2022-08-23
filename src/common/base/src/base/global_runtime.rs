// Copyright 2022 Datafuse Labs.
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

use common_exception::Result;
use once_cell::sync::OnceCell;

use crate::base::singleton_instance::Singleton;
use crate::base::Runtime;

pub struct GlobalIORuntime;

static GLOBAL_RUNTIME: OnceCell<Singleton<Arc<Runtime>>> = OnceCell::new();

impl GlobalIORuntime {
    pub fn init(num_cpus: usize, v: Singleton<Arc<Runtime>>) -> Result<()> {
        let thread_num = std::cmp::max(num_cpus, num_cpus::get() / 2);
        let thread_num = std::cmp::max(2, thread_num);

        v.init(Arc::new(Runtime::with_worker_threads(
            thread_num,
            Some("IO-worker".to_owned()),
        )?))?;

        GLOBAL_RUNTIME.set(v.clone()).ok();
        Ok(())
    }

    pub fn instance() -> Arc<Runtime> {
        match GLOBAL_RUNTIME.get() {
            None => panic!("GlobalRuntime is not init"),
            Some(global_runtime) => global_runtime.get(),
        }
    }
}
