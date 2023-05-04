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

use common_license::license_manager::LicenseManager;
use enterprise_query::license::license_mgr::RealLicenseManager;

#[test]
fn test_make_license() -> common_exception::Result<()> {
    assert_eq!(
        RealLicenseManager::make_license("not valid")
            .err()
            .unwrap()
            .code(),
        common_exception::ErrorCode::LicenseKeyParseError("").code()
    );
    let valid_key = "eyJhbGciOiAiRVMyNTYiLCAidHlwIjogIkpXVCJ9.eyJ0eXBlIjogInRyaWFsIiwgIm9yZyI6ICJ0ZXN0IiwgInRlbmFudHMiOiBudWxsLCAiaXNzIjogImRhdGFiZW5kIiwgImlhdCI6IDE2ODE4OTk1NjIsICJuYmYiOiAxNjgxODk5NTYyLCAiZXhwIjogMTY4MzE5NTU2MX0.EfnbkZgNGuCUM0yZ7wg1ARgkiY3g32OHkoWZYoEADNEBPd4Dp8Dhq-W5qzTTSEthiMiiRym0DswGpzYPFSwQww";
    let claim = RealLicenseManager::make_license(valid_key).unwrap();
    assert_eq!(claim.issuer.unwrap(), "databend");
    assert_eq!(claim.custom.org.unwrap(), "test");
    assert_eq!(claim.custom.r#type.unwrap(), "trial");
    assert!(claim.custom.tenants.is_none());
    Ok(())
}
