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
use ctor::ctor;

use crate::base::GlobalInstance;
use crate::base::Pool;
use crate::base::Reusable;
use crate::runtime::Runtime;

pub struct GlobalIORuntime;

impl GlobalIORuntime {
    pub fn init(num_cpus: usize) -> Result<()> {
        let thread_num = std::cmp::max(num_cpus, num_cpus::get() / 2);
        let thread_num = std::cmp::max(2, thread_num);

        let runtime = Arc::new(Runtime::with_worker_threads(
            thread_num,
            Some("IO-worker".to_owned()),
        )?);

        GlobalInstance::set((thread_num as i32, runtime));
        Ok(())
    }

    pub fn instance() -> Arc<Runtime> {
        let (_, runtime): (i32, Arc<Runtime>) = GlobalInstance::get();
        runtime
    }
}

pub struct ReusableRuntimePool {
    thread_num: usize,
    pool: Pool<Arc<Runtime>>,
}

#[ctor]
pub static RESUE_RUNTIME: ReusableRuntimePool = reuse_runtime_pool();

fn reuse_runtime_pool() -> ReusableRuntimePool {
    let thread_num = num_cpus::get().clamp(2, 64);
    let pool = Pool::new(4);

    ReusableRuntimePool { thread_num, pool }
}

impl ReusableRuntimePool {
    pub fn pull(&self) -> Reusable<Arc<Runtime>> {
        self.pool.pull(|| {
            Arc::new(
                Runtime::with_worker_threads(
                    self.thread_num,
                    Some("ReusableRuntime-Worker".to_owned()),
                )
                .unwrap(),
            )
        })
    }
}
