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

use chrono::{DateTime, Utc};
use databend_common_exception::exception::Result;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::PasswordHashMethod;
use databend_common_meta_app::principal::UserInfo;

#[test]
fn test_user_info() -> Result<()> {
    // This test will introduce a older UserInfo struct and a new UserInfo struct.
    // And check the serialize(old_userinfo) can be deserialized by the new UserInfo.
    #[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Default)]
    #[serde(default)]
    pub struct OldUserInfo {
        pub name: String,
        pub hostname: String,
        pub auth_info: AuthInfo,
    }

    let old = OldUserInfo {
        name: "old-name".to_string(),
        hostname: "old-host".to_string(),
        auth_info: AuthInfo::Password {
            hash_value: Vec::from("pwd"),
            hash_method: PasswordHashMethod::Sha256,
        },
    };

    let ser_old = serde_json::to_string(&old)?;
    let new = UserInfo::try_from(ser_old.into_bytes())?;

    let mut expect = UserInfo::new("old-name", "old-host", AuthInfo::Password {
        hash_value: Vec::from("pwd"),
        hash_method: PasswordHashMethod::Sha256,
    });
    expect.created_on = DateTime::<Utc>::default();
    expect.update_on = DateTime::<Utc>::default();

    assert_eq!(new, expect);

    Ok(())
}
