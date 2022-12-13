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

use common_base::base::GlobalInstance;
use common_config::Config;
use common_exception::Result;
use common_tracing::set_panic_hook;
use databend_query::clusters::ClusterDiscovery;
use databend_query::sessions::SessionManager;
use databend_query::GlobalServices;
use tracing::debug;
use tracing::info;

pub struct TestGlobalServices;

unsafe impl Send for TestGlobalServices {}

unsafe impl Sync for TestGlobalServices {}

impl TestGlobalServices {
    pub async fn setup(config: Config) -> Result<TestGuard> {
        set_panic_hook();
        std::env::set_var("UNIT_TEST", "TRUE");

        GlobalServices::init_with(config.clone(), false).await?;

        // Cluster register.
        {
            ClusterDiscovery::instance()
                .register_to_metastore(&config)
                .await?;
            info!(
                "Databend query has been registered:{:?} to metasrv:{:?}.",
                config.query.cluster_id, config.meta.endpoints
            );
        }

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
        debug!(
            "test {} is finished, starting dropping all resources",
            &self.thread_name
        );

        // Check if session manager sill have active sessions.
        {
            let session_mgr = SessionManager::instance();
            // Destory all sessions.
            for process in session_mgr.processes_info() {
                session_mgr.destroy_session(&process.id);
            }
            // Double check again.
            for process in session_mgr.processes_info() {
                debug!("process {process:?} still running after drop, something must be wrong");
            }
        }

        GlobalInstance::drop_testing(&self.thread_name);

        debug!("test {} resources have been dropped", &self.thread_name);
    }
}
