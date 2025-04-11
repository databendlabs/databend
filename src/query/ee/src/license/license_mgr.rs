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

use dashmap::DashMap;
use databend_common_base::base::GlobalInstance;
use databend_common_exception::exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ToErrorCode;
use databend_common_license::license::Feature;
use databend_common_license::license::LicenseInfo;
use databend_common_license::license::StorageQuota;
use databend_common_license::license_manager::LicenseManager;
use databend_common_license::license_manager::LicenseManagerSwitch;
use jwt_simple::algorithms::ES256PublicKey;
use jwt_simple::claims::JWTClaims;
use jwt_simple::prelude::Clock;
use jwt_simple::prelude::ECDSAP256PublicKeyLike;
use jwt_simple::JWTError;
use log::warn;

const LICENSE_PUBLIC_KEY: &str = r#"-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEGsKCbhXU7j56VKZ7piDlLXGhud0a
pWjW3wxSdeARerxs/BeoWK7FspDtfLaAT8iJe4YEmR0JpkRQ8foWs0ve3w==
-----END PUBLIC KEY-----"#;

pub const LICENSE_URL: &str = "https://docs.databend.com/guides/products/dee/";

pub struct RealLicenseManager {
    tenant: String,
    public_keys: Vec<String>,

    // cache available settings to get avoid of unneeded license parsing time.
    pub(crate) cache: DashMap<String, JWTClaims<LicenseInfo>>,
}

impl RealLicenseManager {
    fn parse_license_impl(&self, raw: &str) -> Result<JWTClaims<LicenseInfo>> {
        for public_key in &self.public_keys {
            let public_key = ES256PublicKey::from_pem(public_key)
                .map_err_to_code(ErrorCode::LicenseKeyParseError, || "public key load failed")?;

            return match public_key.verify_token::<LicenseInfo>(raw, None) {
                Ok(v) => Ok(v),
                Err(cause) => match cause.downcast_ref::<JWTError>() {
                    Some(JWTError::TokenHasExpired) => {
                        warn!("License expired");
                        Err(ErrorCode::LicenseKeyExpired("license key is expired."))
                    }
                    Some(JWTError::InvalidSignature) => {
                        continue;
                    }
                    _ => Err(ErrorCode::LicenseKeyParseError("jwt claim decode failed")),
                },
            };
        }

        Err(ErrorCode::LicenseKeyParseError("wt claim decode failed"))
    }
}

impl LicenseManager for RealLicenseManager {
    fn init(tenant: String) -> Result<()> {
        let public_key_str = embedded_public_keys()?;

        let mut public_keys = Vec::new();
        let mut remain_str = public_key_str.as_str();

        let len = "-----END PUBLIC KEY-----".len();
        while let Some(r_pos) = remain_str.find("-----END PUBLIC KEY-----") {
            let key_str = &remain_str[..r_pos + len];
            public_keys.push(key_str.to_string());
            remain_str = remain_str[r_pos + len..].trim();
        }

        public_keys.push(LICENSE_PUBLIC_KEY.to_string());

        let rm = RealLicenseManager {
            tenant,
            public_keys,
            cache: DashMap::new(),
        };

        let license_manager_switch = Arc::new(LicenseManagerSwitch::create(Box::new(rm)));

        GlobalInstance::set(license_manager_switch);
        Ok(())
    }

    fn instance() -> Arc<Box<dyn LicenseManager>> {
        GlobalInstance::get()
    }

    fn check_enterprise_enabled(&self, license_key: String, feature: Feature) -> Result<()> {
        if license_key.is_empty() {
            return feature.verify_default(format!(
                "The use of this feature requires a Databend Enterprise Edition license. No license key found for tenant: {}. To unlock enterprise features, please contact Databend to obtain a license. Learn more at {}",
                self.tenant, LICENSE_URL
            ));
        }

        if let Some(v) = self.cache.get(&license_key) {
            return self.verify_feature(v.value(), feature);
        }

        match self.parse_license_impl(&license_key) {
            Ok(license) => {
                self.verify_feature(&license, feature)?;
                self.cache.insert(license_key, license);
                Ok(())
            }
            Err(e) => match e.code() == ErrorCode::LICENSE_KEY_EXPIRED {
                true => self.verify_if_expired(feature),
                false => Err(e),
            },
        }
    }

    fn parse_license(&self, raw: &str) -> Result<JWTClaims<LicenseInfo>> {
        if let Some(v) = self.cache.get(raw) {
            // Previously cached valid license might be expired
            let claim = v.value();
            if Self::verify_license_expired(claim)? {
                Err(ErrorCode::LicenseKeyExpired("license key is expired."))
            } else {
                Ok((*claim).clone())
            }
        } else {
            self.parse_license_impl(raw)
        }
    }

    fn get_storage_quota(&self, license_key: String) -> Result<StorageQuota> {
        if license_key.is_empty() {
            return Ok(StorageQuota::default());
        }

        if let Some(v) = self.cache.get(&license_key) {
            if Self::verify_license_expired(v.value())? {
                return Err(ErrorCode::LicenseKeyExpired(format!(
                    "license key expired in {:?}",
                    v.value().expires_at,
                )));
            }
            return Ok(v.custom.get_storage_quota());
        }

        let license = self.parse_license(&license_key).map_err_to_code(
            ErrorCode::LicenseKeyInvalid,
            || format!("use of storage requires an enterprise license. current license is invalid for {}", self.tenant),
        )?;

        if Self::verify_license_expired(&license)? {
            return Err(ErrorCode::LicenseKeyExpired(format!(
                "license key expired in {:?}",
                license.expires_at,
            )));
        }

        let quota = license.custom.get_storage_quota();
        self.cache.insert(license_key, license);
        Ok(quota)
    }
}

impl RealLicenseManager {
    // this method mainly used for unit tests
    pub fn new(tenant: String, public_key: String) -> Self {
        RealLicenseManager {
            tenant,
            cache: DashMap::new(),
            public_keys: vec![public_key],
        }
    }

    fn verify_license_expired(l: &JWTClaims<LicenseInfo>) -> Result<bool> {
        let now = Clock::now_since_epoch();
        match l.expires_at {
            Some(expire_at) => Ok(now > expire_at),
            None => Err(ErrorCode::LicenseKeyInvalid(
                "cannot find valid expire time",
            )),
        }
    }

    fn verify_feature(&self, l: &JWTClaims<LicenseInfo>, feature: Feature) -> Result<()> {
        if Self::verify_license_expired(l)? {
            return self.verify_if_expired(feature);
        }

        if l.custom.features.is_none() {
            return Ok(());
        }

        let verify_features = l.custom.features.as_ref().unwrap();
        for verify_feature in verify_features {
            if verify_feature.verify(&feature)? {
                return Ok(());
            }
        }

        Err(ErrorCode::LicenseKeyInvalid(format!(
            "license key does not support feature {}, supported features: {}",
            feature,
            l.custom.display_features()
        )))
    }

    fn verify_if_expired(&self, feature: Feature) -> Result<()> {
        feature.verify_default("").map_err(|_|
            ErrorCode::LicenseKeyExpired(format!(
                "The use of this feature requires a Databend Enterprise Edition license. License key has expired for tenant: {}. To unlock enterprise features, please contact Databend to obtain a license. Learn more at https://docs.databend.com/guides/products/dee/",
                self.tenant
            ))
        )
    }
}

fn embedded_public_keys() -> Result<String> {
    let pub_key = env!("DATABEND_ENTERPRISE_LICENSE_PUBLIC_KEY").to_string();

    if pub_key.is_empty() {
        return Ok(pub_key);
    }

    let decode_res = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        pub_key.as_bytes(),
    );

    match decode_res {
        Err(e) => Err(ErrorCode::Internal(format!(
            "Cannot parse embedded public key {:?}",
            e
        ))),
        Ok(bytes) => match String::from_utf8(bytes) {
            Err(e) => Err(ErrorCode::Internal(format!(
                "Cannot parse embedded public key {:?}",
                e
            ))),
            Ok(keys) => Ok(keys),
        },
    }
}

#[cfg(test)]
mod tests {
    use jwt_simple::prelude::Duration;

    use super::*;

    impl RealLicenseManager {
        // Test helper method to insert a license into the cache
        #[cfg(test)]
        pub fn insert_into_cache_for_test(&self, key: &str, claims: JWTClaims<LicenseInfo>) {
            self.cache.insert(key.to_string(), claims);
        }
    }

    // Helper function to create expired JWT claims
    fn create_expired_claims() -> JWTClaims<LicenseInfo> {
        JWTClaims {
            issued_at: None,
            expires_at: Some(Clock::now_since_epoch() - Duration::from_days(1)),
            invalid_before: None,
            issuer: None,
            subject: None,
            audiences: None,
            jwt_id: None,
            nonce: None,
            custom: LicenseInfo {
                r#type: None,
                org: None,
                tenants: None,
                features: None,
            },
        }
    }

    // Helper function to create valid (non-expired) JWT claims
    fn create_valid_claims() -> JWTClaims<LicenseInfo> {
        JWTClaims {
            issued_at: None,
            expires_at: Some(Clock::now_since_epoch() + Duration::from_days(30)),
            invalid_before: None,
            issuer: None,
            subject: None,
            audiences: None,
            jwt_id: None,
            nonce: None,
            custom: LicenseInfo {
                r#type: None,
                org: None,
                tenants: None,
                features: None,
            },
        }
    }

    #[test]
    fn test_parse_license_expired_in_cache() {
        // Create a RealLicenseManager instance
        let manager =
            RealLicenseManager::new("test-tenant".to_string(), LICENSE_PUBLIC_KEY.to_string());

        // Create an expired license and insert it into the cache
        let expired_claims = create_expired_claims();
        let license_key = "expired-license";
        manager.insert_into_cache_for_test(license_key, expired_claims);

        // Call parse_license method, should return LicenseKeyExpired error
        let result = manager.parse_license(license_key);
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.code(), ErrorCode::LICENSE_KEY_EXPIRED);
        } else {
            panic!("Expected LicenseKeyExpired error but got Ok result");
        }
    }

    #[test]
    fn test_parse_license_valid_in_cache() {
        // Create a RealLicenseManager instance
        let manager =
            RealLicenseManager::new("test-tenant".to_string(), LICENSE_PUBLIC_KEY.to_string());

        // Create a valid license and insert it into the cache
        let valid_claims = create_valid_claims();
        let license_key = "valid-license";
        manager.insert_into_cache_for_test(license_key, valid_claims.clone());

        // Call parse_license method, should return the valid license
        let result = manager.parse_license(license_key);
        assert!(result.is_ok());
        if let Ok(claims) = result {
            assert_eq!(claims.expires_at, valid_claims.expires_at);
        } else {
            panic!("Expected valid license but got Err result");
        }
    }
}
