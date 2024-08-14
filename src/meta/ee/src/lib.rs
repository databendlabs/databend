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
use std::sync::Mutex;
use std::time::Duration;

use databend_common_base::display::display_unix_epoch::DisplayUnixTimeStampExt;
use databend_common_license::license::LicenseInfo;
use jwt_simple::algorithms::ECDSAP256KeyPairLike;
use jwt_simple::algorithms::ECDSAP256PublicKeyLike;
use jwt_simple::algorithms::ES256KeyPair;
use jwt_simple::algorithms::ES256PublicKey;
use jwt_simple::claims::Claims;
use jwt_simple::claims::JWTClaims;
use jwt_simple::prelude::Clock;

#[derive(Clone, Default)]
pub struct MetaServiceEnterpriseGate {
    /// License ES256 signed jwt claim token.
    license_token: Arc<Mutex<Option<String>>>,

    /// The public key to verify the license token.
    public_key: String,
}

impl MetaServiceEnterpriseGate {
    const LICENSE_PUBLIC_KEY: &'static str = r#"-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEGsKCbhXU7j56VKZ7piDlLXGhud0a
pWjW3wxSdeARerxs/BeoWK7FspDtfLaAT8iJe4YEmR0JpkRQ8foWs0ve3w==
-----END PUBLIC KEY-----"#;

    pub fn new(token: Option<String>) -> Self {
        Self {
            license_token: Arc::new(Mutex::new(token)),
            public_key: Self::LICENSE_PUBLIC_KEY.to_string(),
        }
    }

    /// Create with a temp license for testing.
    pub fn new_testing() -> Self {
        let lic = LicenseInfo {
            r#type: Some("trial".to_string()),
            org: Some("databend".to_string()),
            tenants: Some(vec!["test".to_string()]),
            features: None,
        };

        let key_pair = ES256KeyPair::generate();
        let claims = Claims::with_custom_claims(lic, jwt_simple::prelude::Duration::from_hours(2));
        let token = key_pair.sign(claims).unwrap();

        let public_key = key_pair.public_key().to_pem().unwrap();

        Self {
            license_token: Arc::new(Mutex::new(Some(token))),
            public_key,
        }
    }

    /// Parse the JWT token and restore the claims.
    pub fn parse_jwt_token(&self, raw: &str) -> Result<JWTClaims<LicenseInfo>, anyhow::Error> {
        let public_key = ES256PublicKey::from_pem(&self.public_key)?;

        let claim = public_key.verify_token::<LicenseInfo>(raw, None)?;

        Ok(claim)
    }

    fn check_license(&self, raw: &str) -> Result<(), anyhow::Error> {
        let claim = self.parse_jwt_token(raw)?;

        let now = Clock::now_since_epoch();

        if Some(now) > claim.expires_at {
            let expires_at = claim.expires_at.unwrap_or_default();
            let unix_timestamp = Duration::from_millis(expires_at.as_millis());

            return Err(anyhow::anyhow!(format!(
                "License is expired at: {}",
                unix_timestamp.display_unix_timestamp()
            )));
        }

        Ok(())
    }

    pub fn assert_cluster_enabled(&self) -> Result<(), anyhow::Error> {
        let token = {
            let x = self.license_token.lock().unwrap();
            x.clone()
        };

        let Some(token) = token.as_ref() else {
            log::error!(
                "No license set in config `databend_enterprise_license`, clustering is disabled",
            );
            return Err(anyhow::anyhow!(
                "No license set in config `databend_enterprise_license`, clustering is disabled"
            ));
        };

        if let Err(e) = self.check_license(token) {
            log::error!("Check license failed: {}", e);
            return Err(e);
        }

        Ok(())
    }

    pub fn update_license(&self, license: String) -> Result<(), anyhow::Error> {
        self.check_license(&license)?;

        let mut x = self.license_token.lock().unwrap();
        *x = Some(license);

        Ok(())
    }
}
