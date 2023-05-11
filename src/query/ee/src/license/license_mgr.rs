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

use common_base::base::GlobalInstance;
use common_exception::exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_license::license::LicenseInfo;
use common_license::license_manager::LicenseManager;
use common_license::license_manager::LicenseManagerWrapper;
use common_settings::Settings;
use dashmap::DashMap;
use jwt_simple::algorithms::ES256PublicKey;
use jwt_simple::claims::JWTClaims;
use jwt_simple::prelude::Clock;
use jwt_simple::prelude::ECDSAP256PublicKeyLike;

const LICENSE_PUBLIC_KEY: &str = r#"-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEGsKCbhXU7j56VKZ7piDlLXGhud0a
pWjW3wxSdeARerxs/BeoWK7FspDtfLaAT8iJe4YEmR0JpkRQ8foWs0ve3w==
-----END PUBLIC KEY-----"#;

pub struct RealLicenseManager {
    // cache available settings to get avoid of unneeded license parsing time.
    pub(crate) cache: DashMap<String, JWTClaims<LicenseInfo>>,
    public_key: String,
}

impl LicenseManager for RealLicenseManager {
    fn init() -> Result<()> {
        let rm = RealLicenseManager {
            cache: DashMap::new(),
            public_key: LICENSE_PUBLIC_KEY.to_string(),
        };
        let wrapper = LicenseManagerWrapper {
            manager: Box::new(rm),
        };
        GlobalInstance::set(Arc::new(wrapper));
        Ok(())
    }

    fn instance() -> Arc<Box<dyn LicenseManager>> {
        GlobalInstance::get()
    }

    fn check_enterprise_enabled(
        &self,
        settings: &Arc<Settings>,
        tenant: String,
        feature: String,
    ) -> Result<()> {
        let license_key = settings.get_enterprise_license().map_err_to_code(
            ErrorCode::LicenseKeyInvalid,
            || format!("use of {feature} requires an enterprise license. failed to load license key for {tenant}"),
        )?;

        if license_key.is_empty() {
            return Err(ErrorCode::LicenseKeyInvalid("license key is empty"));
        }

        if let Some(v) = self.cache.get(license_key.as_str()) {
            return Self::verify_license(v.value());
        }

        let license = self.parse_license(license_key.as_str()).map_err_to_code(
            ErrorCode::LicenseKeyInvalid,
            || format!("use of {feature} requires an enterprise license. current license is invalid for {tenant}"),
        )?;
        self.cache.insert(license_key, license);
        Ok(())
    }

    fn parse_license(&self, raw: &str) -> Result<JWTClaims<LicenseInfo>> {
        let public_key = ES256PublicKey::from_pem(self.public_key.as_str())
            .map_err_to_code(ErrorCode::LicenseKeyParseError, || "public key load failed")?;
        public_key
            .verify_token::<LicenseInfo>(raw, None)
            .map_err_to_code(
                ErrorCode::LicenseKeyParseError,
                || "jwt claim decode failed",
            )
    }
}

impl RealLicenseManager {
    // this method mainly used for unit tests
    pub fn new(public_key: String) -> Self {
        RealLicenseManager {
            cache: DashMap::new(),
            public_key,
        }
    }

    // Only need to verify expire since other fields have already verified before enter into cache.
    fn verify_license(l: &JWTClaims<LicenseInfo>) -> Result<()> {
        let now = Clock::now_since_epoch();
        match l.expires_at {
            Some(expire_at) => {
                if now > expire_at {
                    return Err(ErrorCode::LicenseKeyInvalid(format!(
                        "license key expired in {:?}",
                        expire_at
                    )));
                }
            }
            None => {
                return Err(ErrorCode::LicenseKeyInvalid(
                    "cannot find valid expire time",
                ));
            }
        }

        Ok(())
    }
}
