// Copyright 2021 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use jwt_simple::prelude::*;

#[derive(Serialize, Deserialize, Debug)]
pub struct GrpcClaim {
    pub username: String,
}

#[derive(Clone)]
pub struct GrpcToken {
    key: HS256Key,
}

impl GrpcToken {
    pub fn create() -> Self {
        let key = HS256Key::generate();
        Self { key }
    }

    pub fn try_create_token(&self, claim: GrpcClaim) -> Result<String> {
        let claims = Claims::with_custom_claims(claim, Duration::from_days(3650));
        self.key
            .authenticate(claims)
            .map_err_to_code(ErrorCode::AuthenticateFailure, || {
                "Cannot create flight token, because authenticate failure"
            })
    }

    pub fn try_verify_token(&self, token: String) -> Result<GrpcClaim> {
        let claims = self.key.verify_token::<GrpcClaim>(&token, None)?;
        Ok(claims.custom)
    }
}
