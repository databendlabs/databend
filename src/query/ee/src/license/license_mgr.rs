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
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use dashmap::DashMap;
use databend_common_base::base::GlobalInstance;
use databend_common_exception::Result;
use databend_common_exception::ToErrorCode;
use databend_common_exception::exception::ErrorCode;
use databend_common_license::license::Feature;
use databend_common_license::license::LicenseClaims;
use databend_common_license::license::VerifyResult;
use databend_common_license::license_manager::LicenseManager;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_version::DATABEND_ENTERPRISE_LICENSE_PUBLIC_KEY;
use jsonwebtoken::Algorithm;
use jsonwebtoken::DecodingKey;
use jsonwebtoken::Validation;
use jsonwebtoken::decode;
use jsonwebtoken::errors::ErrorKind;
use jwt_simple::claims::DEFAULT_TIME_TOLERANCE_SECS;
use jwt_simple::common::DEFAULT_MAX_TOKEN_LENGTH;
use jwt_simple::token::Token;
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
    pub(crate) cache: DashMap<String, LicenseClaims>,
}

impl RealLicenseManager {
    fn current_unix_time() -> Result<Duration> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err_to_code(
                ErrorCode::LicenseKeyParseError,
                || "[LicenseManager] System clock is before the Unix epoch",
            )
    }

    fn validate_token_metadata(raw: &str) -> Result<()> {
        // These limits were applied implicitly by `jwt-simple::verify_token`.
        if raw.len() > DEFAULT_MAX_TOKEN_LENGTH {
            return Err(ErrorCode::LicenseKeyParseError(
                "[LicenseManager] License key is too long",
            ));
        }

        let metadata = Token::decode_metadata(raw).map_err_to_code(
            ErrorCode::LicenseKeyParseError,
            || "[LicenseManager] JWT header decode failed",
        )?;
        if metadata.signature_type().is_some_and(|typ| {
            let typ = typ.to_uppercase();
            typ != "JWT" && !typ.ends_with("+JWT")
        }) {
            return Err(ErrorCode::LicenseKeyParseError(
                "[LicenseManager] Invalid JWT type",
            ));
        }
        Ok(())
    }

    fn license_validation() -> Validation {
        // Match the policy previously applied by `jwt-simple::verify_token`.
        // `jsonwebtoken` otherwise uses different defaults for clock tolerance,
        // required expiry, not-before, and audience validation.
        let mut validation = Validation::new(Algorithm::ES256);
        validation.leeway = DEFAULT_TIME_TOLERANCE_SECS;
        validation.validate_nbf = true;
        // Check exp once in `validate_claim_times` to retain the previous
        // verifier's subsecond expiry boundary.
        validation.validate_exp = false;
        validation.required_spec_claims.clear();
        validation.validate_aud = false;
        validation
    }

    fn validate_claim_times(claims: &LicenseClaims, now: Duration) -> Result<()> {
        // `jsonwebtoken` does not validate iat; retain the previous future-iat check.
        if claims
            .issued_at
            .is_some_and(|iat| iat > now.as_secs().saturating_add(DEFAULT_TIME_TOLERANCE_SECS))
        {
            return Err(ErrorCode::LicenseKeyParseError(
                "[LicenseManager] License issued in the future",
            ));
        }

        let tolerance = Duration::from_secs(DEFAULT_TIME_TOLERANCE_SECS);
        if claims
            .expires_at
            .is_some_and(|exp| now.saturating_sub(tolerance) > Duration::from_secs(exp))
        {
            return Err(ErrorCode::LicenseKeyExpired(
                "[LicenseManager] License key is expired",
            ));
        }
        Ok(())
    }

    fn parse_license_impl(&self, raw: &str) -> Result<LicenseClaims> {
        Self::validate_token_metadata(raw)?;
        let validation = Self::license_validation();

        for public_key in &self.public_keys {
            let public_key = DecodingKey::from_ec_pem(public_key.as_bytes()).map_err_to_code(
                ErrorCode::LicenseKeyParseError,
                || "[LicenseManager] Public key load failed",
            )?;

            match decode::<LicenseClaims>(raw, &public_key, &validation) {
                Ok(token) => {
                    Self::validate_claim_times(&token.claims, Self::current_unix_time()?)?;
                    return Ok(token.claims);
                }
                Err(cause) => match cause.kind() {
                    // Public-key rotation requires trying the next trusted key.
                    ErrorKind::InvalidSignature => continue,
                    _ => {
                        return Err(ErrorCode::LicenseKeyParseError(
                            "[LicenseManager] JWT claim decode failed",
                        ));
                    }
                },
            }
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

    fn parse_license(&self, raw: &str) -> Result<LicenseClaims> {
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

    fn verify_license_expired(l: &LicenseClaims) -> Result<bool> {
        Self::verify_license_expired_at(l, Self::current_unix_time()?)
    }

    fn verify_license_expired_at(l: &LicenseClaims, now: Duration) -> Result<bool> {
        match l.expires_at {
            Some(expire_at) => Ok(now > Duration::from_secs(expire_at)),
            None => Err(ErrorCode::LicenseKeyInvalid(
                "[LicenseManager] Cannot find valid expiration time",
            )),
        }
    }

    fn verify_feature(&self, l: &LicenseClaims, feature: Feature) -> Result<()> {
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
    use databend_common_license::license::LicenseInfo;
    use jsonwebtoken::EncodingKey;
    use jsonwebtoken::Header;
    use jsonwebtoken::encode;
    use jwt_simple::algorithms::ECDSAP256KeyPairLike;
    use jwt_simple::claims::JWTClaims;
    use jwt_simple::prelude::Clock;
    use jwt_simple::prelude::Duration as JwtDuration;
    use jwt_simple::prelude::ES256KeyPair;
    use serde_json::Value;

    use super::*;

    impl RealLicenseManager {
        // Helper to insert license into cache for testing
        #[cfg(test)]
        pub fn insert_into_cache_for_test(&self, key: &str, claims: JWTClaims<LicenseInfo>) {
            let claims = serde_json::from_value(serde_json::to_value(claims).unwrap()).unwrap();
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
            expires_at: Some(Clock::now_since_epoch() - JwtDuration::from_days(1)),
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
            expires_at: Some(Clock::now_since_epoch() + JwtDuration::from_days(30)),
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

    fn sign_claims(key_pair: &ES256KeyPair, claims: &Value) -> String {
        let key = EncodingKey::from_ec_pem(key_pair.to_pem().unwrap().as_bytes()).unwrap();
        encode(&Header::new(Algorithm::ES256), claims, &key).unwrap()
    }

    #[test]
    fn test_full_width_expiry_and_cache() {
        let key_pair = ES256KeyPair::generate();
        let mut manager = RealLicenseManager::new(
            "test-tenant".to_string(),
            ES256KeyPair::generate().public_key().to_pem().unwrap(),
        );
        manager
            .public_keys
            .push(key_pair.public_key().to_pem().unwrap());
        for exp in [u32::MAX as u64, u32::MAX as u64 + 1, 4_891_363_200] {
            let mut payload = serde_json::to_value(create_valid_claims()).unwrap();
            payload["exp"] = exp.into();
            let token = sign_claims(&key_pair, &payload);
            for _ in 0..2 {
                let claims = manager.parse_license(&token).unwrap();
                assert_eq!(claims.expires_at, Some(exp));
                assert!(manager.is_in_cache(&token));
                manager
                    .check_enterprise_enabled(token.clone(), Feature::LicenseInfo)
                    .unwrap();
            }
            let claims = manager.parse_license(&token).unwrap();
            let at_expiry = Duration::from_secs(exp);
            assert!(!RealLicenseManager::verify_license_expired_at(&claims, at_expiry).unwrap());
            assert!(
                RealLicenseManager::verify_license_expired_at(
                    &claims,
                    at_expiry + Duration::from_nanos(1),
                )
                .unwrap()
            );
            if exp > u32::MAX as u64 {
                assert!(
                    !RealLicenseManager::verify_license_expired_at(
                        &claims,
                        Duration::from_secs(u32::MAX as u64 + 1),
                    )
                    .unwrap()
                );
            }
        }
    }

    #[test]
    fn test_timestamp_validation_compatibility() {
        use jwt_simple::prelude::ECDSAP256PublicKeyLike;

        let key_pair = ES256KeyPair::generate();
        let public_key = key_pair.public_key();
        let manager =
            RealLicenseManager::new("test-tenant".to_string(), public_key.to_pem().unwrap());
        let now = Clock::now_since_epoch().as_secs();
        for (field, value, valid) in [
            ("exp", now + 3600, true),
            ("exp", now - 60, true),
            ("exp", now - 890, true),
            ("exp", now - 910, false),
            ("exp", now - 3600, false),
            ("nbf", now + 60, true),
            ("nbf", now + 890, true),
            ("nbf", now + 910, false),
            ("nbf", now + 3600, false),
            ("iat", now + 60, true),
            ("iat", now + 890, true),
            ("iat", now + 910, false),
            ("iat", now + 3600, false),
        ] {
            let mut payload = serde_json::to_value(create_valid_claims()).unwrap();
            payload[field] = value.into();
            let token = sign_claims(&key_pair, &payload);
            assert_eq!(
                public_key.verify_token::<LicenseInfo>(&token, None).is_ok(),
                valid
            );
            let expected = match (field, valid) {
                (_, true) => Ok(()),
                ("exp", false) => Err(ErrorCode::LICENSE_KEY_EXPIRED),
                (_, false) => Err(ErrorCode::LICENSE_KEY_PARSE_ERROR),
            };
            assert_eq!(
                manager
                    .parse_license_impl(&token)
                    .map(|_| ())
                    .map_err(|error| error.code()),
                expected,
                "field: {field}, value: {value}"
            );
        }

        let mut payload = serde_json::to_value(create_valid_claims()).unwrap();
        payload.as_object_mut().unwrap().remove("exp");
        let token = sign_claims(&key_pair, &payload);
        assert!(public_key.verify_token::<LicenseInfo>(&token, None).is_ok());
        assert!(manager.parse_license_impl(&token).is_ok());
        assert!(
            manager
                .check_enterprise_enabled(token, Feature::LicenseInfo)
                .is_err()
        );

        // The previous verifier accepted and truncated fractional Unix seconds.
        let mut payload = serde_json::to_value(create_valid_claims()).unwrap();
        payload["iat"] = serde_json::json!(now as f64 - 0.5);
        payload["nbf"] = serde_json::json!(now as f64 - 0.5);
        payload["exp"] = serde_json::json!(now as f64 + 3600.8);
        let token = sign_claims(&key_pair, &payload);
        assert!(public_key.verify_token::<LicenseInfo>(&token, None).is_ok());
        let claims = manager.parse_license_impl(&token).unwrap();
        assert_eq!(claims.issued_at, Some(now - 1));
        assert_eq!(claims.invalid_before, Some(now - 1));
        assert_eq!(claims.expires_at, Some(now + 3600));
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
            assert_eq!(
                claims.expires_at,
                valid_claims.expires_at.map(|v| v.as_secs())
            );
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
            assert_eq!(
                serde_json::to_value(&claims).unwrap()["aud"],
                serde_json::to_value(&valid_claims).unwrap()["aud"]
            );
            assert_eq!(claims.jwt_id, valid_claims.jwt_id);

            // Verify LicenseInfo fields
            assert_eq!(claims.custom.r#type, valid_claims.custom.r#type);
            assert_eq!(claims.custom.org, valid_claims.custom.org);
            assert_eq!(claims.custom.tenants, valid_claims.custom.tenants);

            // Verify valid expiration
            let now = Clock::now_since_epoch();
            assert!(claims.expires_at.unwrap() > now.as_secs());
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
