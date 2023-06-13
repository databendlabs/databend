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

use std::sync::Arc;

use common_config::InnerConfig;
use common_exception::Result;
use common_meta_app::principal::AuthInfo;
use common_meta_app::principal::GrantObject;
use common_meta_app::principal::PasswordHashMethod;
use common_meta_app::principal::UserInfo;
use common_meta_app::principal::UserPrivilegeSet;
use databend_query::sessions::QueryContext;
use databend_query::sessions::SessionManager;
use databend_query::sessions::SessionType;
use databend_query::sessions::TableContext;
use databend_query::test_kits::TestGuard;

use crate::test_kits::sessions::TestGlobalServices;

pub async fn create_query_context_with_config(
    config: InnerConfig,
    mut current_user: Option<UserInfo>,
) -> Result<(TestGuard, Arc<QueryContext>)> {
    let guard = TestGlobalServices::setup(config).await?;

    let dummy_session = SessionManager::instance()
        .create_session(SessionType::Dummy)
        .await?;

    if current_user.is_none() {
        let mut user_info = UserInfo::new("root", "127.0.0.1", AuthInfo::Password {
            hash_method: PasswordHashMethod::Sha256,
            hash_value: Vec::from("pass"),
        });

        user_info.grants.grant_privileges(
            &GrantObject::Global,
            UserPrivilegeSet::available_privileges_on_global(),
        );

        current_user = Some(user_info);
    }

    dummy_session
        .set_authed_user(current_user.unwrap(), None)
        .await?;
    let dummy_query_context = dummy_session.create_query_context().await?;

    dummy_query_context.get_settings().set_max_threads(8)?;
    Ok((guard, dummy_query_context))
}
