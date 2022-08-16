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

use common_base::base::tokio;
use common_exception::Result;
use common_settings::Settings;
use databend_query::sessions::{Session, SessionContext};
use databend_query::sessions::SessionManager;
use databend_query::sessions::SessionType;

use crate::tests::ConfigBuilder;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_session() -> Result<()> {
    let conf = ConfigBuilder::create().build();

    let typ = SessionType::Dummy;
    let id = String::from("test-001");
    let tenant = &conf.query.tenant_id.clone();
    let session_settings = Settings::default_settings(tenant);
    let session_ctx = SessionContext::try_create(conf.clone(), session_settings)?;
    let session = Session::try_create(id, typ, session_ctx, None)?;

    // Tenant.
    {
        let actual = session.get_current_tenant();
        assert_eq!(&actual, "test");

        // We are not in management mode, so always get the config tenant.
        session.set_current_tenant("tenant2".to_string());
        let actual = session.get_current_tenant();
        assert_eq!(&actual, "test");
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
    let conf = ConfigBuilder::create().with_management_mode().build();

    let typ = SessionType::Dummy;
    let id = String::from("test-001");
    let tenant = &conf.query.tenant_id.clone();
    let session_settings = Settings::default_settings(tenant);
    let session_ctx = SessionContext::try_create(conf.clone(), session_settings)?;
    let session = Session::try_create(id, typ, session_ctx, None)?;

    // Tenant.
    {
        let actual = session.get_current_tenant();
        assert_eq!(&actual, "test");

        session.set_current_tenant("tenant2".to_string());
        let actual = session.get_current_tenant();
        assert_eq!(&actual, "tenant2");
    }

    // let session_manager = SessionManager::instance();
    // // test session leak
    // let leak_id;
    // {
    //     let leak_session = session_manager.create_session(SessionType::Dummy).await?;
    //     leak_id = leak_session.get_id();
    //     assert!(
    //         session_manager
    //             .get_session_by_id(leak_id.as_str())
    //             .await
    //             .is_some()
    //     );
    // }
    // assert!(
    //     session_manager
    //         .get_session_by_id(leak_id.as_str())
    //         .await
    //         .is_none()
    // );

    Ok(())
}
