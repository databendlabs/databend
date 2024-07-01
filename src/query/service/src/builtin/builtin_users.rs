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

use std::collections::HashMap;
use std::str::FromStr;

use databend_common_config::UserConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::AuthType;

pub struct BuiltinUsers {
    user_configs: Vec<UserConfig>,
}

impl BuiltinUsers {
    pub fn create(user_configs: Vec<UserConfig>) -> BuiltinUsers {
        BuiltinUsers { user_configs }
    }

    fn check_no_auth_string(auth_string: Option<String>, auth_info: AuthInfo) -> Result<AuthInfo> {
        match auth_string {
            Some(s) if !s.is_empty() => Err(ErrorCode::InvalidConfig(format!(
                "should not set auth_string for auth_type {}",
                auth_info.get_type().to_str()
            ))),
            _ => Ok(auth_info),
        }
    }

    /// Convert to auth infos.
    pub fn to_meta_auth_infos(&self) -> Result<HashMap<String, AuthInfo>> {
        let mut auth_infos = HashMap::new();
        for user_config in self.user_configs.iter() {
            let name = user_config.name.clone();
            let auth_config = user_config.auth.clone();
            let auth_type = AuthType::from_str(&auth_config.auth_type)?;
            let auth_info = match auth_type {
                AuthType::NoPassword => {
                    Self::check_no_auth_string(auth_config.auth_string.clone(), AuthInfo::None)
                }
                AuthType::JWT => {
                    Self::check_no_auth_string(auth_config.auth_string.clone(), AuthInfo::JWT)
                }
                AuthType::Sha256Password | AuthType::DoubleSha1Password => {
                    let password_type = auth_type.get_password_type().expect("must success");
                    match &auth_config.auth_string {
                        None => Err(ErrorCode::InvalidConfig("must set auth_string")),
                        Some(s) => {
                            let p = hex::decode(s).map_err(|e| {
                                ErrorCode::InvalidConfig(format!("password is not hex: {e:?}"))
                            })?;
                            Ok(AuthInfo::Password {
                                hash_value: p,
                                hash_method: password_type,
                            })
                        }
                    }
                }
            }?;

            auth_infos.insert(name, auth_info);
        }
        Ok(auth_infos)
    }
}
