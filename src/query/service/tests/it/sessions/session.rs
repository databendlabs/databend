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

use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_common_meta_app::tenant::Tenant;
use databend_query::sessions::SessionType;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_session() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let session = fixture.new_session_with_type(SessionType::Dummy).await?;

    // Tenant.
    {
        let actual = session.get_current_tenant();
        assert_eq!(actual.tenant_name(), "test");

        // We are not in management mode, so always get the config tenant.
        session.set_current_tenant(Tenant::new_literal("tenant2"));
        let actual = session.get_current_tenant();
        assert_eq!(actual.tenant_name(), "test");
    }

    // Settings.
    {
        let settings = session.get_settings();
        settings.set_max_threads(3)?;
        let actual = settings.get_max_threads()?;
        assert_eq!(actual, 3);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_session_in_management_mode() -> Result<()> {
    let config = ConfigBuilder::create().with_management_mode().build();
    let fixture = TestFixture::setup_with_config(&config).await?;
    let session = fixture.new_session_with_type(SessionType::Dummy).await?;

    // Tenant.
    {
        let actual = session.get_current_tenant();
        assert_eq!(actual.tenant_name(), "test");

        session.set_current_tenant(Tenant::new_literal("tenant2"));
        let actual = session.get_current_tenant();
        assert_eq!(actual.tenant_name(), "tenant2");
    }

    Ok(())
}
