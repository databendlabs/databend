// Copyright 2021 Datafuse Labs.
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

use std::time::Duration;

use common_base::base::GlobalInstance;
use common_config::Config;
use common_exception::Result;
use common_tracing::set_panic_hook;
use databend_query::sessions::SessionManager;
use databend_query::GlobalServices;
use time::Instant;

pub struct TestGlobalServices;

unsafe impl Send for TestGlobalServices {}

unsafe impl Sync for TestGlobalServices {}

impl TestGlobalServices {
    pub async fn setup(config: Config) -> Result<TestGuard> {
        set_panic_hook();
        std::env::set_var("UNIT_TEST", "TRUE");

        GlobalServices::init_with(config, false).await?;

        match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(thread_name) => Ok(TestGuard {
                thread_name: thread_name.to_string(),
            }),
        }
    }
}

pub struct TestGuard {
    thread_name: String,
}

impl Drop for TestGuard {
    fn drop(&mut self) {
        // Hack: The session may be referenced by other threads. Let's try to wait.
        let now = Instant::now();
        while !SessionManager::instance().processes_info().is_empty() {
            std::thread::sleep(Duration::from_millis(500));

            if now.elapsed() > Duration::from_secs(3) {
                break;
            }
        }

        GlobalInstance::drop_testing(&self.thread_name)
    }
}
