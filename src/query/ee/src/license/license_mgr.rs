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
use databend_common_license::license::VerifyResult;
use databend_common_license::license_manager::LicenseManager;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_version::DATABEND_ENTERPRISE_LICENSE_PUBLIC_KEY;
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
                .map_err_to_code(ErrorCode::LicenseKeyParseError, || {
                    "[LicenseManager] Public key load failed"
                })?;

            return match public_key.verify_token::<LicenseInfo>(raw, None) {
                Ok(v) => Ok(v),
                Err(cause) => match cause.downcast_ref::<JWTError>() {
                    Some(JWTError::TokenHasExpired) => Err(ErrorCode::LicenseKeyExpired(
                        "[LicenseManager] License key is expired",
                    )),
                    Some(JWTError::InvalidSignature) => {
                        continue;
                    }
                    _ => Err(ErrorCode::LicenseKeyParseError(
                        "[LicenseManager] JWT claim decode failed",
                    )),
                },
            };
        }

        Err(ErrorCode::LicenseKeyParseError(
            "[LicenseManager] JWT claim decode failed",
        ))
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
                "[LicenseManager] Feature '{}' requires Databend Enterprise Edition license. No license key found for tenant: {}. Learn more at {}",
                feature, self.tenant, LICENSE_URL
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
                warn!("[LicenseManager] Cached license expired");
                Err(ErrorCode::LicenseKeyExpired(
                    "[LicenseManager] License key is expired.",
                ))
            } else {
                Ok((*claim).clone())
            }
        } else {
            let license = self.parse_license_impl(raw)?;
            self.cache.insert(raw.to_string(), license.clone());
            Ok(license)
        }
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
                "[LicenseManager] Cannot find valid expiration time",
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
        let mut has_verify_failed = false;
        for verify_feature in verify_features {
            match verify_feature.verify(&feature)? {
                VerifyResult::MissMatch => {}
                VerifyResult::Success => {
                    return Ok(());
                }
                VerifyResult::Failure => {
                    has_verify_failed = true;
                }
            }
        }

        match has_verify_failed {
            true => Err(ErrorCode::LicenseKeyInvalid(format!(
                "[LicenseManager] License does not support feature: {}. Supported features: {}",
                feature,
                l.custom.display_features()
            ))),
            // If the feature is not included in the license, default verification is used.
            false => feature.verify_default(format!(
                "[LicenseManager] License does not support feature: {}. Supported features: {}",
                feature,
                l.custom.display_features()
            )),
        }
    }

    fn verify_if_expired(&self, feature: Feature) -> Result<()> {
        feature.verify_default("").map_err(|_|
            ErrorCode::LicenseKeyExpired(format!(
                "[LicenseManager] Feature '{}' requires Databend Enterprise Edition license. License key expired for tenant: {}. Learn more at {}",
                feature, self.tenant, LICENSE_URL
            ))
        )
    }
}

fn embedded_public_keys() -> Result<String> {
    let pub_key = DATABEND_ENTERPRISE_LICENSE_PUBLIC_KEY.to_string();
    if pub_key.is_empty() {
        return Ok(pub_key);
    }

    let decode_res = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        pub_key.as_bytes(),
    );

    match decode_res {
        Err(e) => Err(ErrorCode::Internal(format!(
            "[LicenseManager] Cannot parse embedded public key: {:?}",
            e
        ))),
        Ok(bytes) => match String::from_utf8(bytes) {
            Err(e) => Err(ErrorCode::Internal(format!(
                "[LicenseManager] Cannot parse embedded public key: {:?}",
                e
            ))),
            Ok(keys) => Ok(keys),
        },
    }
}

#[cfg(test)]
mod tests {
    use jwt_simple::algorithms::ECDSAP256KeyPairLike;
    use jwt_simple::prelude::Duration;
    use jwt_simple::prelude::ES256KeyPair;

    use super::*;

    impl RealLicenseManager {
        // Helper to insert license into cache for testing
        #[cfg(test)]
        pub fn insert_into_cache_for_test(&self, key: &str, claims: JWTClaims<LicenseInfo>) {
            self.cache.insert(key.to_string(), claims);
        }

        // Helper to check if license exists in cache
        #[cfg(test)]
        pub fn is_in_cache(&self, key: &str) -> bool {
            self.cache.contains_key(key)
        }
    }

    // Create expired JWT claims for testing
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

    // Create valid JWT claims with test data
    fn create_valid_claims() -> JWTClaims<LicenseInfo> {
        JWTClaims {
            issued_at: None,
            expires_at: Some(Clock::now_since_epoch() + Duration::from_days(30)),
            invalid_before: None,
            issuer: Some("Databend".into()),
            subject: Some("test-license".into()),
            audiences: Some("test-tenant".into()),
            jwt_id: Some("test-jwt-id-123".into()),
            nonce: None,
            custom: LicenseInfo {
                r#type: Some("enterprise".into()),
                org: Some("Test Organization".into()),
                tenants: Some(vec!["tenant1".into(), "tenant2".into()]),
                features: None,
            },
        }
    }

    #[test]
    fn test_parse_license_expired_in_cache() {
        // Test retrieving expired license from cache
        let manager =
            RealLicenseManager::new("test-tenant".to_string(), LICENSE_PUBLIC_KEY.to_string());

        let expired_claims = create_expired_claims();
        let license_key = "expired-license";
        manager.insert_into_cache_for_test(license_key, expired_claims);

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
        // Test retrieving valid license from cache
        let manager =
            RealLicenseManager::new("test-tenant".to_string(), LICENSE_PUBLIC_KEY.to_string());

        let valid_claims = create_valid_claims();
        let license_key = "valid-license";
        manager.insert_into_cache_for_test(license_key, valid_claims.clone());

        let result = manager.parse_license(license_key);
        assert!(result.is_ok());
        if let Ok(claims) = result {
            assert_eq!(claims.expires_at, valid_claims.expires_at);
        } else {
            panic!("Expected valid license but got Err result");
        }
    }

    #[test]
    fn test_parse_license_not_in_cache_but_valid() {
        // Test validating and caching a new license
        let key_pair = ES256KeyPair::generate();
        let public_key = key_pair.public_key().to_pem().unwrap();
        let valid_claims = create_valid_claims();
        let token = key_pair.sign(valid_claims.clone()).unwrap();

        let manager = RealLicenseManager::new("test-tenant".to_string(), public_key);
        assert!(!manager.is_in_cache(&token));

        // Verify successful validation adds to cache
        let result = manager.parse_license(&token);
        assert!(result.is_ok());
        assert!(manager.is_in_cache(&token));

        // Verify cached version returns correctly
        let second_result = manager.parse_license(&token);
        assert!(second_result.is_ok());

        if let Ok(claims) = second_result {
            // Verify non-timestamp fields match exactly
            assert_eq!(claims.issuer, valid_claims.issuer);
            assert_eq!(claims.subject, valid_claims.subject);
            assert_eq!(claims.audiences, valid_claims.audiences);
            assert_eq!(claims.jwt_id, valid_claims.jwt_id);

            // Verify LicenseInfo fields
            assert_eq!(claims.custom.r#type, valid_claims.custom.r#type);
            assert_eq!(claims.custom.org, valid_claims.custom.org);
            assert_eq!(claims.custom.tenants, valid_claims.custom.tenants);

            // Verify valid expiration
            let now = Clock::now_since_epoch();
            assert!(claims.expires_at.unwrap() > now);
        } else {
            panic!("Expected valid license but got Err result");
        }
    }

    #[test]
    fn test_parse_license_invalid_not_added_to_cache() {
        // Test invalid licenses aren't cached
        let key_pair = ES256KeyPair::generate();
        let valid_claims = create_valid_claims();
        let token = key_pair.sign(valid_claims).unwrap();

        // Use a different public key to force validation failure
        let different_key_pair = ES256KeyPair::generate();
        let wrong_public_key = different_key_pair.public_key().to_pem().unwrap();
        let manager = RealLicenseManager::new("test-tenant".to_string(), wrong_public_key);

        assert!(!manager.is_in_cache(&token));
        let result = manager.parse_license(&token);
        assert!(result.is_err());

        // Verify failed validation doesn't add to cache
        assert!(!manager.is_in_cache(&token));
    }
}
