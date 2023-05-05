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
use common_license::license::LICENSE_PUBLIC_KEY;
use common_license::license_manager::LicenseManager;
use common_license::license_manager::LicenseManagerWrapper;
use jwt_simple::algorithms::ES256PublicKey;
use jwt_simple::claims::JWTClaims;
use jwt_simple::prelude::ECDSAP256PublicKeyLike;

pub struct RealLicenseManager {}

impl LicenseManager for RealLicenseManager {
    fn init() -> Result<()> {
        let rm = RealLicenseManager {};
        let wrapper = LicenseManagerWrapper {
            manager: Box::new(rm),
        };
        GlobalInstance::set(Arc::new(wrapper));
        Ok(())
    }

    fn instance() -> Arc<Box<dyn LicenseManager>> {
        GlobalInstance::get()
    }

    fn is_active(&self) -> bool {
        true
    }

    fn make_license(raw: &str) -> Result<JWTClaims<LicenseInfo>> {
        let public_key = ES256PublicKey::from_pem(LICENSE_PUBLIC_KEY)
            .map_err_to_code(ErrorCode::LicenseKeyParseError, || "public key load failed")?;
        public_key
            .verify_token::<LicenseInfo>(raw, None)
            .map_err_to_code(
                ErrorCode::LicenseKeyParseError,
                || "jwt claim decode failed",
            )
    }
}
