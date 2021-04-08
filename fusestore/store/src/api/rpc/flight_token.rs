// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use anyhow::Result;
use jwt_simple::prelude::*;

#[derive(Serialize, Deserialize, Debug)]
pub struct FlightClaim {
    pub(crate) user_is_admin: bool,
    pub(crate) username: String,
}

#[derive(Clone)]
pub struct FlightToken {
    key: HS256Key,
}

impl FlightToken {
    pub fn create() -> Self {
        let key = HS256Key::generate();
        Self { key }
    }

    pub fn try_create_token(&self, claim: FlightClaim) -> Result<String> {
        let claims = Claims::with_custom_claims(claim, Duration::from_days(3650));
        self.key.authenticate(claims)
    }

    pub fn try_verify_token(&self, token: String) -> Result<FlightClaim> {
        let claims = self.key.verify_token::<FlightClaim>(&token, None)?;
        Ok(claims.custom)
    }
}
