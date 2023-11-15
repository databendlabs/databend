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

use common_config::InnerConfig;
use common_exception::Result;
use common_tracing::set_panic_hook;
use databend_query::clusters::ClusterDiscovery;
use databend_query::test_kits::TestGuard;
use databend_query::GlobalServices;
use log::info;

use crate::test_kits::mock_services::MockServices;

pub struct TestGlobalServices;

unsafe impl Send for TestGlobalServices {}

unsafe impl Sync for TestGlobalServices {}

impl TestGlobalServices {
    pub async fn setup(config: &InnerConfig, public_key: String) -> Result<TestGuard> {
        set_panic_hook();
        std::env::set_var("UNIT_TEST", "TRUE");

        let thread_name = match std::thread::current().name() {
            None => panic!("thread name is none"),
            Some(thread_name) => thread_name.to_string(),
        };

        #[cfg(debug_assertions)]
        common_base::base::GlobalInstance::init_testing(&thread_name);

        GlobalServices::init_with(config.clone()).await?;
        MockServices::init(config.clone(), public_key).await?;

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

        Ok(TestGuard::new(thread_name.to_string()))
    }
}
