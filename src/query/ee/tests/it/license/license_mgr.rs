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

use databend_common_base::base::tokio;
use databend_common_license::license::Feature;
use databend_common_license::license::LicenseInfo;
use databend_common_license::license_manager::LicenseManager;
use databend_enterprise_query::license::RealLicenseManager;
use databend_query::test_kits::*;
use jwt_simple::algorithms::ES256KeyPair;
use jwt_simple::claims::Claims;
use jwt_simple::prelude::Duration;
use jwt_simple::prelude::ECDSAP256KeyPairLike;
use jwt_simple::prelude::UnixTimeStamp;

fn build_custom_claims(
    license_type: String,
    org: String,
    features: Option<Vec<Feature>>,
) -> LicenseInfo {
    LicenseInfo {
        r#type: Some(license_type),
        org: Some(org),
        tenants: None,
        features,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_parse_license() -> databend_common_exception::Result<()> {
    let fixture = TestFixture::setup().await?;

    let key_pair = ES256KeyPair::generate();
    let license_mgr = RealLicenseManager::new(
        fixture.default_tenant().tenant_name().to_string(),
        key_pair.public_key().to_pem().unwrap(),
    );
    let claims = Claims::with_custom_claims(
        build_custom_claims("trial".to_string(), "databend".to_string(), None),
        Duration::from_hours(2),
    );
    let token = key_pair.sign(claims)?;

    let parsed = license_mgr.parse_license(token.as_str());
    assert!(parsed.is_ok());

    assert!(
        license_mgr
            .check_enterprise_enabled(token.clone(), Feature::Test)
            .is_ok()
    );
    // test cache hit
    assert!(
        license_mgr
            .check_enterprise_enabled(token, Feature::Test)
            .is_ok()
    );

    // test expired token
    let mut claims = Claims::with_custom_claims(
        build_custom_claims("trial".to_string(), "expired".to_string(), None),
        Duration::from_hours(0),
    );
    claims.expires_at = Some(UnixTimeStamp::new(1, 1));
    let token = key_pair.sign(claims)?;
    let parsed = license_mgr.parse_license(token.as_str());
    assert!(parsed.is_err());
    assert!(
        license_mgr
            .check_enterprise_enabled(token, Feature::Test)
            .is_err()
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_license_features() -> databend_common_exception::Result<()> {
    let fixture = TestFixture::setup().await?;

    let key_pair = ES256KeyPair::generate();
    let license_mgr = RealLicenseManager::new(
        fixture.default_tenant().tenant_name().to_string(),
        key_pair.public_key().to_pem().unwrap(),
    );
    let claims = Claims::with_custom_claims(
        build_custom_claims(
            "trial".to_string(),
            "expired".to_string(),
            Some(vec![
                Feature::Test,
                Feature::LicenseInfo,
                Feature::Vacuum,
                Feature::Stream,
            ]),
        ),
        Duration::from_hours(2),
    );
    let token = key_pair.sign(claims)?;

    let parsed = license_mgr.parse_license(token.as_str());
    assert!(parsed.is_ok());

    assert!(
        license_mgr
            .check_enterprise_enabled(token.clone(), Feature::ComputedColumn)
            .is_err()
    );

    assert!(
        license_mgr
            .check_enterprise_enabled(token.clone(), Feature::LicenseInfo)
            .is_ok()
    );

    assert!(
        license_mgr
            .check_enterprise_enabled(token.clone(), Feature::VirtualColumn)
            .is_err()
    );

    assert!(
        license_mgr
            .check_enterprise_enabled(token.clone(), Feature::Test)
            .is_ok()
    );

    assert!(
        license_mgr
            .check_enterprise_enabled(token.clone(), Feature::Vacuum)
            .is_ok()
    );

    assert!(
        license_mgr
            .check_enterprise_enabled(token, Feature::Stream)
            .is_ok()
    );

    // test expired token
    let mut claims = Claims::with_custom_claims(
        build_custom_claims(
            "trial".to_string(),
            "expired".to_string(),
            Some(vec![Feature::Test, Feature::LicenseInfo, Feature::Vacuum]),
        ),
        Duration::from_hours(0),
    );
    claims.expires_at = Some(UnixTimeStamp::new(1, 1));
    let token = key_pair.sign(claims)?;
    let parsed = license_mgr.parse_license(token.as_str());
    assert!(parsed.is_err());
    assert!(
        license_mgr
            .check_enterprise_enabled(token, Feature::Test)
            .is_err()
    );

    Ok(())
}
