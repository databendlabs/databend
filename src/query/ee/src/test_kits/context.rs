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

use databend_common_config::InnerConfig;
use databend_common_exception::Result;
use databend_common_license::license::LicenseInfo;
use databend_common_meta_app::storage::StorageFsConfig;
use databend_common_meta_app::storage::StorageParams;
use databend_query::test_kits::*;
use jwt_simple::algorithms::ECDSAP256KeyPairLike;
use jwt_simple::prelude::Claims;
use jwt_simple::prelude::Duration;
use jwt_simple::prelude::ES256KeyPair;
use tempfile::TempDir;

use crate::test_kits::setup::TestFixture;

fn build_custom_claims(license_type: String, org: String) -> LicenseInfo {
    LicenseInfo {
        r#type: Some(license_type),
        org: Some(org),
        tenants: Some(vec!["test".to_string()]),
        features: None,
    }
}

pub struct EESetup {
    config: InnerConfig,
    pk: String,
}

impl EESetup {
    pub fn new() -> Self {
        let key_pair = ES256KeyPair::generate();
        let claims = Claims::with_custom_claims(
            build_custom_claims("trial".to_string(), "databend".to_string()),
            Duration::from_hours(2),
        );
        let token = key_pair.sign(claims).unwrap();
        let public_key = key_pair.public_key().to_pem().unwrap();

        let mut conf = ConfigBuilder::create().config();
        conf.query.common.databend_enterprise_license = Some(token);
        conf.storage.allow_insecure = true;

        let tmp_dir = TempDir::new().expect("create tmp dir failed");
        let root = tmp_dir.path().to_str().unwrap().to_string();
        conf.storage.params = StorageParams::Fs(StorageFsConfig { root });
        Self {
            config: conf,
            pk: public_key,
        }
    }

    pub fn config_mut(&mut self) -> &mut InnerConfig {
        &mut self.config
    }
}

impl Default for EESetup {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Setup for EESetup {
    async fn setup(&self) -> Result<InnerConfig> {
        TestFixture::setup(&self.config, self.pk.clone()).await?;
        Ok(self.config.clone())
    }
}
