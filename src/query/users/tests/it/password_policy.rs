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
use databend_common_grpc::RpcClientConf;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::PasswordPolicy;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserInfo;
use databend_common_users::UserApiProvider;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_password_policy() -> Result<()> {
    let conf = RpcClientConf::default();
    let user_mgr = UserApiProvider::try_create_simple(conf).await?;

    let tenant = "test";
    let username = "test-user1";
    let hostname = "%";
    let pwd = "test-pwd";

    let policy_name = "test_policy".to_string();

    // add password policy
    let password_policy = PasswordPolicy {
        name: policy_name.clone(),
        min_length: 12,
        max_length: 24,
        min_upper_case_chars: 2,
        min_lower_case_chars: 2,
        min_numeric_chars: 2,
        min_special_chars: 2,
        min_age_days: 1,
        max_age_days: 30,
        max_retries: 3,
        lockout_time_mins: 30,
        history: 5,
        comment: "".to_string(),
        create_on: Utc.with_ymd_and_hms(2023, 12, 19, 12, 0, 0).unwrap(),
        update_on: None,
    };

    let res = user_mgr.add_password_policy(tenant, password_policy.clone(), false).await;
    assert!(res.ok());

    // invalid min length
    let mut invalid_password_policy1 = password_policy.clone();
    invalid_password_policy1.min_length = 0;
    let res = user_mgr.add_password_policy(tenant, invalid_password_policy1, false).await;
    assert!(res.is_err());

    // invalid max length
    let mut invalid_password_policy2 = password_policy.clone();
    invalid_password_policy2.max_length = 260;
    let res = user_mgr.add_password_policy(tenant, invalid_password_policy2, false).await;
    assert!(res.is_err());

    // invalid min length greater than max length
    let mut invalid_password_policy3 = password_policy.clone();
    invalid_password_policy3.min_length = 30;
    invalid_password_policy3.max_length = 20;
    let res = user_mgr.add_password_policy(tenant, invalid_password_policy2, false).await;
    assert!(res.is_err());

    // invalid min chars
    let mut invalid_password_policy4 = password_policy.clone();
    invalid_password_policy4.min_upper_case_chars = 270;
    invalid_password_policy4.min_lower_case_chars = 271;
    invalid_password_policy4.min_numeric_chars = 272;
    invalid_password_policy4.min_special_chars = 273;
    let res = user_mgr.add_password_policy(tenant, invalid_password_policy4, false).await;
    assert!(res.is_err());

    // invalid sum of upper chars, lower chars, numeric chars and special chars greater than max length
    let mut invalid_password_policy5 = password_policy.clone();
    invalid_password_policy5.max_length = 30;
    invalid_password_policy5.min_upper_case_chars = 10;
    invalid_password_policy5.min_lower_case_chars = 11;
    invalid_password_policy5.min_numeric_chars = 12;
    invalid_password_policy5.min_special_chars = 13;
    let res = user_mgr.add_password_policy(tenant, invalid_password_policy5, false).await;
    assert!(res.is_err());

    // invalid min age days greater than max age days
    let mut invalid_password_policy6 = password_policy.clone();
    invalid_password_policy6.min_age_days = 20;
    invalid_password_policy6.max_age_days = 10;
    let res = user_mgr.add_password_policy(tenant, invalid_password_policy6, false).await;
    assert!(res.is_err());

    // invalid max retries
    let mut invalid_password_policy7 = password_policy.clone();
    invalid_password_policy7.max_retries = 20;
    let res = user_mgr.add_password_policy(tenant, invalid_password_policy7, false).await;
    assert!(res.is_err());

    // invalid lockout time mins
    let mut invalid_password_policy8 = password_policy.clone();
    invalid_password_policy8.lockout_time_mins = 2000;
    let res = user_mgr.add_password_policy(tenant, invalid_password_policy8, false).await;
    assert!(res.is_err());

    // invalid history
    let mut invalid_password_policy9 = password_policy.clone();
    invalid_password_policy9.history = 50;
    let res = user_mgr.add_password_policy(tenant, invalid_password_policy9, false).await;
    assert!(res.is_err());

    // update password policy
    let res = user_mgr.update_password_policy(tenant, &policy_name, 
        Some(10),
        Some(20),
        Some(3),
        Some(3),
        Some(3),
        Some(3),
        Some(2),
        Some(50),
        Some(10),
        Some(20),
        Some(10),
        None,
        false).await;
    assert!(res.ok());

    // add user
    let auth_info = AuthInfo::Password {
        hash_value: Vec::from(pwd),
        hash_method: PasswordHashMethod::Sha256,
    };

    let mut user_info = UserInfo::new(username, hostname, auth_info.clone());
    let mut option = UserOption::empty();
    option = option
        .with_password_policy(Some(policy_name.clone()));
    user_info.update_auth_option(None, Some(option))
    user_mgr.add_user(tenant, user_info, false).await?;

    // drop password policy
    let res = user_mgr.drop_password_policy(tenant, policy_name.as_ref(), false).await;
    assert!(res.is_err());

    user_mgr.drop_user(tenant, user.clone(), false).await?;

    let res = user_mgr.drop_password_policy(tenant, policy_name.as_ref(), false).await;
    assert!(res.is_ok());

    Ok(())
}
