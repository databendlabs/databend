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

use std::fmt;
use std::time::Duration;

use databend_common_base::display::display_option::DisplayOptionExt;
use databend_common_base::display::display_unix_epoch::DisplayUnixTimeStamp;
use databend_common_base::display::display_unix_epoch::DisplayUnixTimeStampExt;
use jwt_simple::claims::JWTClaims;
use jwt_simple::prelude::coarsetime;

/// A helper struct to Display JWT claims.
pub struct DisplayJWTClaims<'a, T> {
    claims: &'a JWTClaims<T>,
}

impl<'a, T> fmt::Display for DisplayJWTClaims<'a, T>
where T: fmt::Display
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn to_ts(v: Option<coarsetime::UnixTimeStamp>) -> Option<DisplayUnixTimeStamp> {
            v.map(|x| {
                let duration = Duration::from_micros(x.as_micros());
                duration.display_unix_timestamp()
            })
        }

        write!(f, "JWTClaims{{")?;
        write!(f, "issuer: {}, ", self.claims.issuer.display())?;
        write!(f, "issued_at: {}, ", to_ts(self.claims.issued_at).display())?;
        write!(
            f,
            "expires_at: {}, ",
            to_ts(self.claims.expires_at).display()
        )?;
        write!(f, "custom: {}", self.claims.custom)?;
        write!(f, "}}")
    }
}

/// Add `display_jwt_claims` method to `JWTClaims<T>` if `T` is Display.
pub trait DisplayJWTClaimsExt<T> {
    fn display_jwt_claims(&self) -> DisplayJWTClaims<T>;
}

impl<T> DisplayJWTClaimsExt<T> for JWTClaims<T>
where T: fmt::Display
{
    fn display_jwt_claims(&self) -> DisplayJWTClaims<T> {
        DisplayJWTClaims { claims: self }
    }
}

#[cfg(test)]
mod tests {
    use jwt_simple::prelude::coarsetime;

    use crate::display_jwt_claims::DisplayJWTClaimsExt;

    #[test]
    fn test_display_jwt_claims() {
        use jwt_simple::claims::JWTClaims;

        let claims = JWTClaims {
            issuer: Some("issuer".to_string()),
            subject: None,
            audiences: None,
            jwt_id: None,
            issued_at: Some(coarsetime::UnixTimeStamp::from_millis(1723102819023)),
            expires_at: Some(coarsetime::UnixTimeStamp::from_millis(1723102819023)),
            custom: "custom".to_string(),
            invalid_before: None,
            nonce: None,
        };

        let display = claims.display_jwt_claims();
        assert_eq!(
            format!("{}", display),
            "JWTClaims{issuer: issuer, issued_at: 2024-08-08T07:40:19.022460Z+0000, expires_at: 2024-08-08T07:40:19.022460Z+0000, custom: custom}"
        );
    }
}
