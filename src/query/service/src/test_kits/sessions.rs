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

use std::borrow::Borrow;

use common_config::InnerConfig;
use common_exception::Result;
use common_license::license_manager::LicenseManager;
use common_license::license_manager::OssLicenseManager;
use common_tracing::set_panic_hook;
use log::info;

use crate::clusters::ClusterDiscovery;
use crate::GlobalServices;

pub struct TestGlobalServices;

unsafe impl Send for TestGlobalServices {}

unsafe impl Sync for TestGlobalServices {}

impl TestGlobalServices {
    pub async fn setup(config: impl Borrow<InnerConfig>) -> Result<TestGuard> {
        set_panic_hook();
        std::env::set_var("UNIT_TEST", "TRUE");

        let config = config.borrow();

        let thread_name = match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(thread_name) => thread_name.to_string(),
        };

        #[cfg(debug_assertions)]
        common_base::base::GlobalInstance::init_testing(&thread_name);

        GlobalServices::init_with(config.clone()).await?;
        OssLicenseManager::init(config.query.tenant_id.clone())?;

        // Cluster register.
        {
            ClusterDiscovery::instance()
                .register_to_metastore(config)
                .await?;
            info!(
                "Databend query has been registered:{:?} to metasrv:{:?}.",
                config.query.cluster_id, config.meta.endpoints
            );
        }

        Ok(TestGuard {
            thread_name: thread_name.to_string(),
        })
    }
}

pub struct TestGuard {
    thread_name: String,
}

impl TestGuard {
    pub fn new(thread_name: String) -> Self {
        Self { thread_name }
    }
}

impl Drop for TestGuard {
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        common_base::base::GlobalInstance::drop_testing(&self.thread_name);
    }
}
