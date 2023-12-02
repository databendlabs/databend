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

use common_config::InnerConfig;
use common_exception::Result;
use common_license::license_manager::LicenseManager;
use common_license::license_manager::OssLicenseManager;
use common_tracing::set_panic_hook;
use log::info;

use crate::clusters::ClusterDiscovery;
use crate::test_kits::ConfigBuilder;
use crate::test_kits::TestFixture;
use crate::GlobalServices;

impl TestFixture {
    /// Setup the test.
    pub async fn setup() -> Result<()> {
        let config = ConfigBuilder::create().build();
        Self::setup_with_config(&config).await
    }

    /// Setup the test environment.
    /// Set the panic hook.
    /// Set the unit test env.
    /// Init the global instance.
    /// Init the global services.
    /// Init the license manager.
    /// Register the cluster to the metastore.
    pub async fn setup_with_config(config: &InnerConfig) -> Result<()> {
        set_panic_hook();
        std::env::set_var("UNIT_TEST", "TRUE");

        let thread_name = std::thread::current().name().unwrap().to_string();
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
                "Databend query unit test setup registered:{:?} to metasrv:{:?}.",
                config.query.cluster_id, config.meta.endpoints
            );
        }

        Ok(())
    }

    /// Teardown the test environment.
    pub async fn teardown() -> Result<()> {
        let thread_name = std::thread::current().name().unwrap().to_string();

        #[cfg(debug_assertions)]
        common_base::base::GlobalInstance::drop_testing(&thread_name);

        Ok(())
    }
}
