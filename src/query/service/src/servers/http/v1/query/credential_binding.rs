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
use std::net::Ipv4Addr;
use std::sync::LazyLock;

use databend_common_base::headers::HEADER_AUTH_METHOD;
use databend_common_meta_app::tenant::Tenant;
use hmac::Hmac;
use hmac::Mac;
use http::HeaderMap;
use http::header::AUTHORIZATION;
use rand::RngCore;
use rand::rngs::OsRng;
use sha2::Sha256;
use subtle::ConstantTimeEq;

const DOMAIN: &[u8] = b"databend-http-query-credential-binding-v1";

// Query state lives only in this process, so a per-process key is sufficient
// and keeps bindings useless outside it.
static KEY: LazyLock<[u8; 32]> = LazyLock::new(|| {
    let mut key = [0; 32];
    OsRng.fill_bytes(&mut key);
    key
});

/// A keyed digest of the credential that submitted an HTTP query.
///
/// A follow-up request presenting exactly the same credential for the same
/// requested tenant is the submitter itself, so it may keep polling the query
/// without re-authenticating, e.g. after the user's password has changed.
/// The credential cannot be recovered from the digest.
#[derive(Clone)]
pub struct CredentialBinding {
    digest: [u8; 32],
    bound_client_ip: Option<Ipv4Addr>,
}

impl CredentialBinding {
    /// Returns `None` unless the request carries exactly one `Authorization`
    /// header and at most one auth-method header.
    pub(crate) fn from_request(tenant: &Tenant, headers: &HeaderMap) -> Option<Self> {
        let mut authorization = headers.get_all(AUTHORIZATION).iter();
        let value = authorization.next()?;
        if authorization.next().is_some() {
            return None;
        }
        let mut auth_methods = headers.get_all(HEADER_AUTH_METHOD).iter();
        let auth_method = auth_methods
            .next()
            .map(|v| v.as_bytes().to_ascii_lowercase())
            .unwrap_or_default();
        if auth_methods.next().is_some() {
            return None;
        }

        let mut mac = Hmac::<Sha256>::new_from_slice(KEY.as_slice()).expect("any key size");
        for part in [
            DOMAIN,
            tenant.tenant_name().as_bytes(),
            &auth_method,
            value.as_bytes(),
        ] {
            mac.update(&(part.len() as u64).to_be_bytes());
            mac.update(part);
        }
        Some(Self {
            digest: mac.finalize().into_bytes().into(),
            bound_client_ip: None,
        })
    }

    pub(crate) fn restrict_to_client_ip(mut self, client_ip: Option<&str>) -> Option<Self> {
        self.bound_client_ip = Some(client_ip?.parse().ok()?);
        Some(self)
    }

    pub(crate) fn matches(&self, other: &Self, client_ip: Option<&str>) -> bool {
        bool::from(self.digest.ct_eq(&other.digest))
            && self.bound_client_ip.is_none_or(|bound_ip| {
                client_ip.and_then(|ip| ip.parse::<Ipv4Addr>().ok()) == Some(bound_ip)
            })
    }
}

impl fmt::Debug for CredentialBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CredentialBinding([REDACTED])")
    }
}

#[cfg(test)]
mod tests {
    use databend_common_base::headers::HEADER_AUTH_METHOD;
    use databend_common_meta_app::tenant::Tenant;
    use http::HeaderMap;
    use http::HeaderValue;
    use http::header::AUTHORIZATION;

    use super::CredentialBinding;

    fn headers(auth: &str, method: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_str(auth).unwrap());
        if let Some(method) = method {
            headers.insert(HEADER_AUTH_METHOD, HeaderValue::from_str(method).unwrap());
        }
        headers
    }

    fn bind(tenant: &str, auth: &str, method: Option<&str>) -> CredentialBinding {
        CredentialBinding::from_request(&Tenant::new_literal(tenant), &headers(auth, method))
            .unwrap()
    }

    #[test]
    fn test_binding_matches_only_identical_requests() {
        let base = bind("t1", "Basic dTpw", None);
        assert!(base.matches(&bind("t1", "Basic dTpw", None), None));
        assert!(!base.matches(&bind("t2", "Basic dTpw", None), None));
        assert!(!base.matches(&bind("t1", "Basic dTpx", None), None));
        assert!(!base.matches(&bind("t1", "Basic dTpw", Some("keypair")), None));
        // Length prefixes keep field boundaries unambiguous.
        assert!(!bind("t1a", "b", None).matches(&bind("t1", "ab", None), None));

        let keypair = bind("t1", "Bearer x", Some("keypair"));
        assert!(keypair.matches(&bind("t1", "Bearer x", Some("KeyPair")), None));
    }

    #[test]
    fn test_binding_client_ip_restriction() {
        let candidate = bind("t1", "Basic dTpw", None);
        let source = Some("192.0.2.1");
        let other = Some("192.0.2.2");
        let invalid = Some("");
        let bound = candidate.clone().restrict_to_client_ip(source).unwrap();
        assert!(bound.matches(&candidate, source));
        assert!(!bound.matches(&candidate, other));
        assert!(!bound.matches(&candidate, None));
        assert!(!bound.matches(&candidate, invalid));
        assert!(candidate.matches(&candidate, other));
        assert!(candidate.clone().restrict_to_client_ip(None).is_none());
        assert!(candidate.restrict_to_client_ip(invalid).is_none());
    }

    #[test]
    fn test_binding_requires_single_authorization() {
        let tenant = Tenant::new_literal("t1");
        assert!(CredentialBinding::from_request(&tenant, &HeaderMap::new()).is_none());
        let mut duplicate = headers("Basic dTpw", None);
        duplicate.append(AUTHORIZATION, HeaderValue::from_static("Basic dTpw"));
        assert!(CredentialBinding::from_request(&tenant, &duplicate).is_none());
        let mut duplicate_method = headers("Bearer x", Some("keypair"));
        duplicate_method.append(HEADER_AUTH_METHOD, HeaderValue::from_static("keypair"));
        assert!(CredentialBinding::from_request(&tenant, &duplicate_method).is_none());
    }

    #[test]
    fn test_binding_debug_is_redacted() {
        let binding = bind("t1", "Basic dTpw", None);
        assert_eq!(format!("{binding:?}"), "CredentialBinding([REDACTED])");
    }
}
