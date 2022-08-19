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
use crate::tests::TestGlobalServices;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_session_setting() -> Result<()> {
    TestGlobalServices::setup(crate::tests::ConfigBuilder::create().build()).await?;
    let session = SessionManager::instance().create_session(SessionType::Dummy).await?;

    // Settings.
    {
        let settings = session.get_settings();
        settings.set_settings("max_threads".to_string(), "3".to_string(), true)?;
        let actual = settings.get_max_threads()?;
        let expect = 3;
        assert_eq!(actual, expect);
    }

    Ok(())
}
