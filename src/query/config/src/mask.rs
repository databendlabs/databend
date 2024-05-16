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

use crate::config::AzblobStorageConfig;
use crate::config::CosStorageConfig;
use crate::config::FsStorageConfig;
use crate::config::GcsStorageConfig;
use crate::config::HdfsConfig;
use crate::config::MetaConfig;
use crate::config::ObsStorageConfig;
use crate::config::OssStorageConfig;
use crate::config::QueryConfig;
use crate::config::S3StorageConfig;
use crate::config::WebhdfsStorageConfig;
use crate::Config;
use crate::StorageConfig;

fn mask_sensitive_field(field: &str) -> String {
    if field.is_empty() {
        String::new()
    } else {
        let field_length = field.len();
        let visible_length = (field_length as f64 * 0.2).ceil() as usize;
        let mask_length = field_length - visible_length;
        let mask = "*".repeat(mask_length);
        format!("{}{}", mask, &field[field_length - visible_length..])
    }
}

// Mask the config value to ******
impl Config {
    pub fn with_mask(self) -> Self {
        Config {
            subcommand: self.subcommand,
            cmd: self.cmd,
            config_file: self.config_file,
            query: self.query.mask_display(),
            log: self.log,
            meta: self.meta.mask_display(),
            storage: self.storage.mask_display(),
            catalog: self.catalog,
            cache: self.cache,
            background: self.background,
            catalogs: self.catalogs,
        }
    }
}

impl QueryConfig {
    fn mask_display(&self) -> Self {
        let mut masked_config = self.clone();

        // Mask Databend Enterprise license
        masked_config.databend_enterprise_license = self
            .databend_enterprise_license
            .as_ref()
            .map(|license| mask_sensitive_field(license));

        // Mask OpenAI API key
        masked_config.openai_api_key = mask_sensitive_field(&self.openai_api_key);

        masked_config
    }
}

impl MetaConfig {
    fn mask_display(&self) -> Self {
        let mut masked_config = self.clone();

        // Mask MetaStore backend user password
        masked_config.password = mask_sensitive_field(&self.password);

        // Mask deprecated password field
        masked_config.meta_password = self
            .meta_password
            .as_ref()
            .map(|password| mask_sensitive_field(password));

        masked_config
    }
}

impl FsStorageConfig {
    fn mask_display(&self) -> Self {
        self.clone()
    }
}

impl GcsStorageConfig {
    fn mask_display(&self) -> Self {
        let mut masked_config = self.clone();
        masked_config.credential = mask_sensitive_field(&self.credential);
        masked_config
    }
}

impl S3StorageConfig {
    fn mask_display(&self) -> Self {
        let mut masked_config = self.clone();
        masked_config.access_key_id = mask_sensitive_field(&self.access_key_id);
        masked_config.secret_access_key = mask_sensitive_field(&self.secret_access_key);
        masked_config.security_token = mask_sensitive_field(&self.security_token);
        masked_config.master_key = mask_sensitive_field(&self.master_key);
        masked_config
    }
}

impl AzblobStorageConfig {
    fn mask_display(&self) -> Self {
        let mut masked_config = self.clone();
        masked_config.account_key = mask_sensitive_field(&self.account_key);
        masked_config
    }
}

impl HdfsConfig {
    fn mask_display(&self) -> Self {
        self.clone()
    }
}

impl ObsStorageConfig {
    fn mask_display(&self) -> Self {
        let mut masked_config = self.clone();
        masked_config.obs_access_key_id = mask_sensitive_field(&self.obs_access_key_id);
        masked_config.obs_secret_access_key = mask_sensitive_field(&self.obs_secret_access_key);
        masked_config
    }
}

impl OssStorageConfig {
    fn mask_display(&self) -> Self {
        let mut masked_config = self.clone();
        masked_config.oss_access_key_id = mask_sensitive_field(&self.oss_access_key_id);
        masked_config.oss_access_key_secret = mask_sensitive_field(&self.oss_access_key_secret);
        masked_config.oss_server_side_encryption_key_id =
            mask_sensitive_field(&self.oss_server_side_encryption_key_id);
        masked_config
    }
}

impl WebhdfsStorageConfig {
    fn mask_display(&self) -> Self {
        let mut masked_config = self.clone();
        masked_config.webhdfs_delegation = mask_sensitive_field(&self.webhdfs_delegation);
        masked_config
    }
}

impl CosStorageConfig {
    fn mask_display(&self) -> Self {
        let mut masked_config = self.clone();
        masked_config.cos_secret_id = mask_sensitive_field(&self.cos_secret_id);
        masked_config.cos_secret_key = mask_sensitive_field(&self.cos_secret_key);
        masked_config
    }
}

impl StorageConfig {
    fn mask_display(&self) -> Self {
        match &self.typ.to_lowercase()[..] {
            "fs" => Self {
                fs: self.fs.mask_display(),
                ..self.clone()
            },
            "gcs" => Self {
                gcs: self.gcs.mask_display(),
                ..self.clone()
            },
            "s3" => Self {
                s3: self.s3.mask_display(),
                ..self.clone()
            },
            "azblob" => Self {
                azblob: self.azblob.mask_display(),
                ..self.clone()
            },
            "hdfs" => Self {
                hdfs: self.hdfs.mask_display(),
                ..self.clone()
            },
            "obs" => Self {
                obs: self.obs.mask_display(),
                ..self.clone()
            },
            "oss" => Self {
                oss: self.oss.mask_display(),
                ..self.clone()
            },
            "webhdfs" => Self {
                webhdfs: self.webhdfs.mask_display(),
                ..self.clone()
            },
            "cos" => Self {
                cos: self.cos.mask_display(),
                ..self.clone()
            },
            _ => self.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mask_sensitive_field() {
        // Empty field
        assert_eq!(mask_sensitive_field(""), "");

        // Field with length less than 5
        assert_eq!(mask_sensitive_field("a"), "a");
        assert_eq!(mask_sensitive_field("ab"), "*b");
        assert_eq!(mask_sensitive_field("abc"), "**c");
        assert_eq!(mask_sensitive_field("abcd"), "***d");

        // Field with length exactly 5
        assert_eq!(mask_sensitive_field("abcde"), "****e");

        // Field with length greater than 5
        assert_eq!(mask_sensitive_field("abcdef"), "****ef");
        assert_eq!(mask_sensitive_field("abcdefghij"), "********ij");
        assert_eq!(mask_sensitive_field("abcdefghijklmnop"), "************mnop");
        assert_eq!(
            mask_sensitive_field("abcdefghijklmnopqrstuvwxyz"),
            "********************uvwxyz"
        );

        // Field with length not divisible by 5
        assert_eq!(mask_sensitive_field("abcdefg"), "*****fg");
        assert_eq!(mask_sensitive_field("abcdefghijklm"), "**********klm");
        assert_eq!(
            mask_sensitive_field("abcdefghijklmnopqrst"),
            "****************qrst"
        );
    }

    #[test]
    fn test_meta_config_mask_display() {
        let config = MetaConfig {
            password: "password".to_string(),
            meta_password: Some("meta_password".to_string()),
            ..MetaConfig::default()
        };

        let masked_config = config.mask_display();
        assert_eq!(masked_config.password, "******rd");
        assert_eq!(masked_config.meta_password.unwrap(), "**********ord");
    }

    #[test]
    fn test_gcs_storage_config_mask_display() {
        let config = GcsStorageConfig {
            credential: "credential".to_string(),
            ..GcsStorageConfig::default()
        };

        let masked_config = config.mask_display();
        assert_eq!(masked_config.credential, "********al");
    }

    #[test]
    fn test_s3_storage_config_mask_display() {
        let config = S3StorageConfig {
            access_key_id: "access_key_id".to_string(),
            secret_access_key: "secret_access_key".to_string(),
            security_token: "security_token".to_string(),
            master_key: "master_key".to_string(),
            ..S3StorageConfig::default()
        };

        let masked_config = config.mask_display();
        assert_eq!(masked_config.access_key_id, "**********_id");
        assert_eq!(masked_config.secret_access_key, "*************_key");
        assert_eq!(masked_config.security_token, "***********ken");
        assert_eq!(masked_config.master_key, "********ey");
    }

    #[test]
    fn test_azblob_storage_config_mask_display() {
        let config = AzblobStorageConfig {
            account_key: "account_key".to_string(),
            ..AzblobStorageConfig::default()
        };

        let masked_config = config.mask_display();
        assert_eq!(masked_config.account_key, "********key");
    }

    #[test]
    fn test_obs_storage_config_mask_display() {
        let config = ObsStorageConfig {
            obs_access_key_id: "obs_access_key_id".to_string(),
            obs_secret_access_key: "obs_secret_access_key".to_string(),
            ..ObsStorageConfig::default()
        };

        let masked_config = config.mask_display();
        assert_eq!(masked_config.obs_access_key_id, "*************y_id");
        assert_eq!(masked_config.obs_secret_access_key, "****************s_key");
    }

    #[test]
    fn test_oss_storage_config_mask_display() {
        let config = OssStorageConfig {
            oss_access_key_id: "oss_access_key_id".to_string(),
            oss_access_key_secret: "oss_access_key_secret".to_string(),
            oss_server_side_encryption_key_id: "oss_server_side_encryption_key_id".to_string(),
            ..OssStorageConfig::default()
        };

        let masked_config = config.mask_display();
        assert_eq!(masked_config.oss_access_key_id, "*************y_id");
        assert_eq!(masked_config.oss_access_key_secret, "****************ecret");
        assert_eq!(
            masked_config.oss_server_side_encryption_key_id,
            "**************************_key_id"
        );
    }

    #[test]
    fn test_webhdfs_storage_config_mask_display() {
        let config = WebhdfsStorageConfig {
            webhdfs_delegation: "webhdfs_delegation".to_string(),
            ..WebhdfsStorageConfig::default()
        };

        let masked_config = config.mask_display();
        assert_eq!(masked_config.webhdfs_delegation, "**************tion");
    }

    #[test]
    fn test_cos_storage_config_mask_display() {
        let config = CosStorageConfig {
            cos_secret_id: "cos_secret_id".to_string(),
            cos_secret_key: "cos_secret_key".to_string(),
            ..CosStorageConfig::default()
        };

        let masked_config = config.mask_display();
        assert_eq!(masked_config.cos_secret_id, "**********_id");
        assert_eq!(masked_config.cos_secret_key, "***********key");
    }
}
