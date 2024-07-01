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

#[cfg(test)]
mod tests {
    use databend_common_config::UserAuthConfig;
    use databend_common_meta_app::principal::PasswordHashMethod;

    use super::*;

    // Helper function to create a UserConfig for testing
    fn create_user_config(name: &str, auth_type: &str, auth_string: Option<String>) -> UserConfig {
        UserConfig {
            name: name.to_string(),
            auth: UserAuthConfig {
                auth_type: auth_type.to_string(),
                auth_string,
            },
        }
    }

    #[test]
    fn test_no_password_user() {
        let user_configs = vec![create_user_config("user1", "no_password", None)];
        let builtin_users = BuiltinUsers::create(user_configs);

        let auth_infos = builtin_users.to_meta_auth_infos().unwrap();
        let auth_info = auth_infos.get("user1").unwrap();

        assert_eq!(auth_info.get_type(), AuthType::NoPassword);
    }

    #[test]
    fn test_jwt_user() {
        let user_configs = vec![create_user_config("user2", "jwt", None)];
        let builtin_users = BuiltinUsers::create(user_configs);

        let auth_infos = builtin_users.to_meta_auth_infos().unwrap();
        let auth_info = auth_infos.get("user2").unwrap();

        assert_eq!(auth_info.get_type(), AuthType::JWT);
    }

    #[test]
    fn test_sha256_password_user() {
        let user_configs = vec![create_user_config(
            "user3",
            "sha256_password",
            Some("5e884898da28047151d0e56f8dc6292773603d0d6aabbddde4208fdfb800bde3".to_string()),
        )];
        let builtin_users = BuiltinUsers::create(user_configs);

        let auth_infos = builtin_users.to_meta_auth_infos().unwrap();
        let auth_info = auth_infos.get("user3").unwrap();

        match auth_info {
            AuthInfo::Password {
                hash_value,
                hash_method,
            } => {
                assert_eq!(hash_method, &PasswordHashMethod::Sha256);
                assert_eq!(
                    hex::encode(hash_value),
                    "5e884898da28047151d0e56f8dc6292773603d0d6aabbddde4208fdfb800bde3"
                );
            }
            _ => panic!("Unexpected auth info type"),
        }
    }

    #[test]
    fn test_double_sha1_password_user() {
        let user_configs = vec![create_user_config(
            "user4",
            "double_sha1_password",
            Some("8bff94b1c58e7733cb1bc36d385bb4986bffeb17".to_string()),
        )];
        let builtin_users = BuiltinUsers::create(user_configs);

        let auth_infos = builtin_users.to_meta_auth_infos().unwrap();
        let auth_info = auth_infos.get("user4").unwrap();

        match auth_info {
            AuthInfo::Password {
                hash_value,
                hash_method,
            } => {
                assert_eq!(hash_method, &PasswordHashMethod::DoubleSha1);
                assert_eq!(
                    hex::encode(hash_value),
                    "8bff94b1c58e7733cb1bc36d385bb4986bffeb17"
                );
            }
            _ => panic!("Unexpected auth info type"),
        }
    }

    #[test]
    fn test_invalid_auth_string() {
        let user_configs = vec![create_user_config(
            "user5",
            "sha256_password",
            Some("invalid_hex".to_string()),
        )];
        let builtin_users = BuiltinUsers::create(user_configs);

        let result = builtin_users.to_meta_auth_infos();
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_auth_string_for_password() {
        let user_configs = vec![create_user_config("user6", "sha256_password", None)];
        let builtin_users = BuiltinUsers::create(user_configs);

        let result = builtin_users.to_meta_auth_infos();
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_auth_type() {
        let user_configs = vec![create_user_config("user7", "InvalidAuthType", None)];
        let builtin_users = BuiltinUsers::create(user_configs);

        let result = builtin_users.to_meta_auth_infos();
        assert!(result.is_err());
    }
}
