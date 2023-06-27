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

use common_exception::Result;
use common_license::license::LicenseInfo;
use common_meta_app::principal::AuthInfo;
use common_meta_app::principal::GrantObject;
use common_meta_app::principal::PasswordHashMethod;
use common_meta_app::principal::UserInfo;
use common_meta_app::principal::UserPrivilegeSet;
use common_meta_app::storage::StorageFsConfig;
use common_meta_app::storage::StorageParams;
use databend_query::sessions::QueryContext;
use databend_query::sessions::SessionManager;
use databend_query::sessions::SessionType;
use databend_query::sessions::TableContext;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestGuard;
use jwt_simple::algorithms::ECDSAP256KeyPairLike;
use jwt_simple::prelude::Claims;
use jwt_simple::prelude::Duration;
use jwt_simple::prelude::ES256KeyPair;
use tempfile::TempDir;

use crate::test_kits::sessions::TestGlobalServices;

fn build_custom_claims(license_type: String, org: String) -> LicenseInfo {
    LicenseInfo {
        r#type: Some(license_type),
        org: Some(org),
        tenants: Some(vec!["test".to_string()]),
    }
}

pub async fn create_ee_query_context(
    mut current_user: Option<UserInfo>,
) -> Result<(TestGuard, Arc<QueryContext>, String)> {
    let key_pair = ES256KeyPair::generate();
    let claims = Claims::with_custom_claims(
        build_custom_claims("trial".to_string(), "databend".to_string()),
        Duration::from_hours(2),
    );
    let token = key_pair.sign(claims)?;
    let public_key = key_pair.public_key().to_pem().unwrap();

    let tmp_dir = TempDir::new().unwrap();
    let mut conf = ConfigBuilder::create().config();
    conf.query.databend_enterprise_license = Some(token);
    // make sure we are using `fs` storage
    let root = tmp_dir.path().to_str().unwrap().to_string();
    conf.storage.allow_insecure = true;
    conf.storage.params = StorageParams::Fs(StorageFsConfig {
        // use `TempDir` as root path (auto clean)
        root: root.clone(),
    });

    let guard = TestGlobalServices::setup(conf, public_key).await?;

    let dummy_session = SessionManager::instance()
        .create_session(SessionType::Dummy)
        .await?;

    if current_user.is_none() {
        let mut user_info = UserInfo::new("root", "%", AuthInfo::Password {
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
    Ok((guard, dummy_query_context, root))
}
