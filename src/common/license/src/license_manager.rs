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

use std::ops::Deref;
use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use jwt_simple::claims::JWTClaims;

use crate::license::Feature;
use crate::license::LicenseInfo;
use crate::license::StorageQuota;

pub trait LicenseManager: Sync + Send {
    fn init(tenant: String) -> Result<()>
    where Self: Sized;

    fn instance() -> Arc<Box<dyn LicenseManager>>
    where Self: Sized;

    /// Check whether enterprise feature is available given context
    /// This function returns `LicenseKeyInvalid` error if enterprise license key is not valid or expired.
    fn check_enterprise_enabled(&self, license_key: String, feature: Feature) -> Result<()>;

    /// Encodes a raw license string as a JWT using the constant public key.
    ///
    /// This function takes a raw license string and a secret key,
    /// The function returns a `jwt_simple::Claim` object that represents the
    /// decoded contents of the JWT  with custom fields `LicenseInfo`
    ///
    /// # Arguments
    ///
    /// * `raw` - The raw license string to be encoded.
    ///
    /// # Returns
    ///
    /// A `jwt_simple::Claim` object representing the decoded contents of the JWT.
    ///
    /// # Errors
    ///
    /// This function may return `LicenseKeyParseError` error if the encoding or decoding of the JWT fails.
    fn parse_license(&self, raw: &str) -> Result<JWTClaims<LicenseInfo>>;

    fn is_license_valid(&self, license_key: String) -> bool {
        self.parse_license(&license_key).is_ok()
    }

    fn check_license(&self, license_key: String) -> Result<()> {
        self.parse_license(&license_key)?;
        Ok(())
    }

    /// Get the storage quota from license key.
    fn get_storage_quota(&self, license_key: String) -> Result<StorageQuota>;
}

pub struct LicenseManagerSwitch {
    manager: Box<dyn LicenseManager>,
}

impl LicenseManagerSwitch {
    pub fn create(manager: Box<dyn LicenseManager>) -> LicenseManagerSwitch {
        LicenseManagerSwitch { manager }
    }

    pub fn instance() -> Arc<LicenseManagerSwitch> {
        GlobalInstance::get()
    }
}

impl Deref for LicenseManagerSwitch {
    type Target = dyn LicenseManager;

    fn deref(&self) -> &Self::Target {
        self.manager.as_ref()
    }
}

pub struct OssLicenseManager {}

impl LicenseManager for OssLicenseManager {
    fn init(_tenant: String) -> Result<()> {
        let rm = OssLicenseManager {};
        GlobalInstance::set(Arc::new(LicenseManagerSwitch::create(Box::new(rm))));
        Ok(())
    }

    fn instance() -> Arc<Box<dyn LicenseManager>> {
        GlobalInstance::get()
    }

    fn check_enterprise_enabled(&self, _license_key: String, feature: Feature) -> Result<()> {
        // oss ignore license key.
        feature.verify_default("Need Commercial License".to_string())
    }

    fn parse_license(&self, _raw: &str) -> Result<JWTClaims<LicenseInfo>> {
        Err(ErrorCode::LicenceDenied(
            "Need Commercial License".to_string(),
        ))
    }

    /// Always return default storage quota.
    fn get_storage_quota(&self, _: String) -> Result<StorageQuota> {
        Ok(StorageQuota::default())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use databend_common_exception::ErrorCode;

    use super::*;

    // Constants for testing
    const VALID_LICENSE_KEY: &str = "valid-license-key";
    const INVALID_LICENSE_KEY: &str = "invalid-license-key";

    // A mock LicenseManager implementation for testing
    struct MockLicenseManager {
        valid_license: bool,
    }

    impl LicenseManager for MockLicenseManager {
        fn init(_tenant: String) -> Result<()> {
            unimplemented!()
        }

        fn instance() -> Arc<Box<dyn LicenseManager>> {
            unimplemented!()
        }

        fn check_enterprise_enabled(&self, _license_key: String, _feature: Feature) -> Result<()> {
            unimplemented!()
        }

        fn parse_license(&self, raw: &str) -> Result<JWTClaims<LicenseInfo>> {
            if self.valid_license && raw == VALID_LICENSE_KEY {
                // We do not care about the claims
                Ok(JWTClaims {
                    issued_at: None,
                    expires_at: None,
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
                })
            } else {
                // Simulate an invalid license
                Err(ErrorCode::LicenceDenied("Invalid cert".to_string()))
            }
        }

        fn get_storage_quota(&self, _license_key: String) -> Result<StorageQuota> {
            unimplemented!()
        }
    }

    #[test]
    fn test_is_license_valid() {
        // Create a MockLicenseManager that accepts the VALID_LICENSE_KEY
        let valid_manager = MockLicenseManager {
            valid_license: true,
        };

        // Test with a valid license key
        assert!(valid_manager.is_license_valid(VALID_LICENSE_KEY.to_string()));

        // Test with an invalid license key
        assert!(!valid_manager.is_license_valid(INVALID_LICENSE_KEY.to_string()));

        // Create a MockLicenseManager that denies all the keys
        let invalid_manager = MockLicenseManager {
            valid_license: false,
        };

        // All license keys should be considered invalid
        assert!(!invalid_manager.is_license_valid(VALID_LICENSE_KEY.to_string()));
        assert!(!invalid_manager.is_license_valid(INVALID_LICENSE_KEY.to_string()));
    }

    #[test]
    fn test_check_license() {
        // Create a MockLicenseManager that accepts the VALID_LICENSE_KEY
        let valid_manager = MockLicenseManager {
            valid_license: true,
        };

        // Test with a valid license key
        let result = valid_manager.check_license(VALID_LICENSE_KEY.to_string());
        assert!(result.is_ok());

        // Test with an invalid license key
        let result = valid_manager.check_license(INVALID_LICENSE_KEY.to_string());
        assert!(result.is_err());

        let e = result.unwrap_err();
        assert_eq!(ErrorCode::LICENCE_DENIED, e.code());

        // Create a MockLicenseManager that denies all the keys
        let invalid_manager = MockLicenseManager {
            valid_license: false,
        };

        // All license keys should be considered invalid
        let result = invalid_manager.check_license(VALID_LICENSE_KEY.to_string());
        assert!(result.is_err());
        let e = result.unwrap_err();
        assert_eq!(ErrorCode::LICENCE_DENIED, e.code());
    }
}
