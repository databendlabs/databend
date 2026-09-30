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

//! Presign for Azure Blob Storage with Workload Identity.
//!
//! OpenDAL azblob only reports presign capability when a static SAS token is
//! configured, and reqsign refuses to put a Bearer token into a query string.
//! Deployments that authenticate with Azure Workload Identity (no account key,
//! no SAS) therefore cannot presign at all. This module mints a short-lived,
//! single-blob User Delegation SAS instead: the federated token is exchanged
//! for an Entra ID access token, which requests a user delegation key, which
//! signs the SAS. No long-lived credential is ever materialized.
//!
//! The string-to-sign follows reqsign-azure-storage's user delegation SAS
//! implementation (service version 2020-12-06).

use std::sync::LazyLock;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::DateTime;
use chrono::Utc;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::storage::StorageAzblobConfig;
use hmac::Hmac;
use hmac::Mac;
use http::HeaderMap;
use http::HeaderValue;
use http::Method;
use opendal::raw::PresignedRequest;
use opendal::raw::build_abs_path;
use opendal::raw::normalize_path;
use opendal::raw::normalize_root;
use opendal::raw::percent_encode_path;
use serde::Deserialize;
use sha2::Sha256;

const STORAGE_SCOPE: &str = "https://storage.azure.com/.default";
const SAS_VERSION: &str = "2020-12-06";
const DEFAULT_AUTHORITY_HOST: &str = "https://login.microsoftonline.com/";
/// Azure caps a user delegation key (and therefore the SAS) at 7 days.
const MAX_EXPIRE: Duration = Duration::from_secs(7 * 24 * 3600);
/// Start the key slightly in the past to tolerate clock skew with Azure.
const KEY_START_SKEW: chrono::Duration = chrono::Duration::minutes(5);

static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build azblob presign http client")
});

/// The presign operation to authorize.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AzblobPresignOp<'a> {
    Read,
    Write { content_type: Option<&'a str> },
}

impl AzblobPresignOp<'_> {
    fn method(&self) -> Method {
        match self {
            AzblobPresignOp::Read => Method::GET,
            AzblobPresignOp::Write { .. } => Method::PUT,
        }
    }

    /// Permissions in Azure canonical order: create a new blob or overwrite.
    fn permissions(&self) -> &'static str {
        match self {
            AzblobPresignOp::Read => "r",
            AzblobPresignOp::Write { .. } => "cw",
        }
    }
}

struct WorkloadIdentity {
    client_id: String,
    tenant_id: String,
    token_file: String,
    authority_host: String,
}

impl WorkloadIdentity {
    fn from_env() -> Option<Self> {
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
        Some(Self {
            client_id: var("AZURE_CLIENT_ID")?,
            tenant_id: var("AZURE_TENANT_ID")?,
            token_file: var("AZURE_FEDERATED_TOKEN_FILE")?,
            authority_host: var("AZURE_AUTHORITY_HOST")
                .unwrap_or_else(|| DEFAULT_AUTHORITY_HOST.to_string()),
        })
    }
}

/// Whether presign should fall back to a user delegation SAS: no static
/// credential is configured and Workload Identity is available.
pub fn azblob_user_delegation_presign_supported(cfg: &StorageAzblobConfig) -> bool {
    cfg.account_key.is_empty() && WorkloadIdentity::from_env().is_some()
}

/// Presign `path` (relative to `cfg.root`) with a user delegation SAS.
pub async fn azblob_user_delegation_presign(
    cfg: &StorageAzblobConfig,
    path: &str,
    op: AzblobPresignOp<'_>,
    expire: Duration,
) -> Result<PresignedRequest> {
    if expire.is_zero() || expire > MAX_EXPIRE {
        return Err(ErrorCode::BadArguments(format!(
            "azblob presign expire must be between 1 second and {} seconds",
            MAX_EXPIRE.as_secs()
        )));
    }
    let target = BlobTarget::new(cfg, path)?;
    let identity = WorkloadIdentity::from_env().ok_or_else(|| {
        ErrorCode::StorageUnsupported("azblob presign requires Azure Workload Identity")
    })?;

    let now = Utc::now();
    let expiry = now
        + chrono::Duration::from_std(expire)
            .map_err(|e| ErrorCode::BadArguments(format!("invalid presign expire: {e}")))?;
    let token = fetch_access_token(&identity).await?;
    let key = fetch_user_delegation_key(&target, &token, now, expiry).await?;
    let query = user_delegation_sas_query(
        &target.account,
        &target.canonical_path(),
        op.permissions(),
        &format_iso(expiry),
        &key,
    )?;

    let uri = format!("{}?{}", target.url(), query)
        .parse::<http::Uri>()
        .map_err(|e| ErrorCode::StorageOther(format!("invalid azblob presign uri: {e}")))?;
    let mut headers = HeaderMap::new();
    if let AzblobPresignOp::Write { content_type } = op {
        // Put Blob rejects requests without an explicit blob type.
        headers.insert("x-ms-blob-type", HeaderValue::from_static("BlockBlob"));
        if let Some(content_type) = content_type {
            let value = HeaderValue::from_str(content_type)
                .map_err(|e| ErrorCode::BadArguments(format!("invalid content type: {e}")))?;
            headers.insert(http::header::CONTENT_TYPE, value);
        }
    }
    Ok(PresignedRequest::new(op.method(), uri, headers))
}

#[derive(Debug)]
struct BlobTarget {
    endpoint: String,
    account: String,
    container: String,
    blob: String,
}

impl BlobTarget {
    fn new(cfg: &StorageAzblobConfig, path: &str) -> Result<Self> {
        let endpoint = cfg.endpoint_url.trim().trim_end_matches('/').to_string();
        if endpoint.is_empty() || cfg.container.is_empty() {
            return Err(ErrorCode::StorageOther(
                "azblob presign requires endpoint_url and container",
            ));
        }
        let account = if cfg.account_name.is_empty() {
            account_from_endpoint(&endpoint)?
        } else {
            cfg.account_name.clone()
        };
        let path = normalize_path(path);
        if path.ends_with('/') {
            return Err(ErrorCode::BadArguments(format!(
                "azblob presign requires a file path, got directory {path}"
            )));
        }
        let blob = build_abs_path(&normalize_root(&cfg.root), &path);
        Ok(Self {
            endpoint,
            account,
            container: cfg.container.clone(),
            blob,
        })
    }

    fn url(&self) -> String {
        format!(
            "{}/{}/{}",
            self.endpoint,
            self.container,
            percent_encode_path(&self.blob)
        )
    }

    fn canonical_path(&self) -> String {
        format!("/blob/{}/{}/{}", self.account, self.container, self.blob)
    }
}

fn account_from_endpoint(endpoint: &str) -> Result<String> {
    let host = url::Url::parse(endpoint)
        .ok()
        .and_then(|u| u.host_str().map(str::to_string))
        .unwrap_or_default();
    match host.split_once(".blob.") {
        Some((account, _)) if !account.is_empty() => Ok(account.to_string()),
        _ => Err(ErrorCode::StorageOther(format!(
            "cannot infer azblob account name from endpoint {endpoint}"
        ))),
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

async fn fetch_access_token(identity: &WorkloadIdentity) -> Result<String> {
    let assertion = tokio::fs::read_to_string(&identity.token_file)
        .await
        .map_err(|e| ErrorCode::StorageOther(format!("read federated token file: {e}")))?;
    let url = format!(
        "{}/{}/oauth2/v2.0/token",
        identity.authority_host.trim_end_matches('/'),
        identity.tenant_id
    );
    let resp = HTTP_CLIENT
        .post(url)
        .form(&[
            ("client_id", identity.client_id.as_str()),
            ("scope", STORAGE_SCOPE),
            (
                "client_assertion_type",
                "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            ),
            ("client_assertion", assertion.trim()),
            ("grant_type", "client_credentials"),
        ])
        .send()
        .await
        .map_err(|e| ErrorCode::StorageOther(format!("request Entra ID token: {e}")))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(ErrorCode::StoragePermissionDenied(format!(
            "Entra ID token request failed with status {status}"
        )));
    }
    let body: TokenResponse = resp
        .json()
        .await
        .map_err(|e| ErrorCode::StorageOther(format!("parse Entra ID token: {e}")))?;
    Ok(body.access_token)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UserDelegationKey {
    signed_oid: String,
    signed_tid: String,
    signed_start: String,
    signed_expiry: String,
    signed_service: String,
    signed_version: String,
    value: String,
}

async fn fetch_user_delegation_key(
    target: &BlobTarget,
    token: &str,
    now: DateTime<Utc>,
    expiry: DateTime<Utc>,
) -> Result<UserDelegationKey> {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?><KeyInfo><Start>{}</Start><Expiry>{}</Expiry></KeyInfo>",
        format_iso(now - KEY_START_SKEW),
        format_iso(expiry)
    );
    let resp = HTTP_CLIENT
        .post(format!(
            "{}/?restype=service&comp=userdelegationkey",
            target.endpoint
        ))
        .bearer_auth(token)
        .header("x-ms-version", SAS_VERSION)
        .header(
            "x-ms-date",
            now.format("%a, %d %b %Y %H:%M:%S GMT").to_string(),
        )
        .header(http::header::CONTENT_TYPE, "application/xml")
        .body(body)
        .send()
        .await
        .map_err(|e| ErrorCode::StorageOther(format!("request user delegation key: {e}")))?;
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| ErrorCode::StorageOther(format!("read user delegation key: {e}")))?;
    if !status.is_success() {
        // Azure error bodies carry only a code/message, never the key.
        return Err(ErrorCode::StoragePermissionDenied(format!(
            "user delegation key request failed with status {status}: {}",
            extract_tag(&text, "Code").unwrap_or_default()
        )));
    }
    parse_user_delegation_key(&text)
}

fn parse_user_delegation_key(xml: &str) -> Result<UserDelegationKey> {
    let tag = |name: &str| {
        extract_tag(xml, name).ok_or_else(|| {
            ErrorCode::StorageOther(format!("user delegation key response missing {name}"))
        })
    };
    Ok(UserDelegationKey {
        signed_oid: tag("SignedOid")?,
        signed_tid: tag("SignedTid")?,
        signed_start: tag("SignedStart")?,
        signed_expiry: tag("SignedExpiry")?,
        signed_service: tag("SignedService")?,
        signed_version: tag("SignedVersion")?,
        value: tag("Value")?,
    })
}

fn extract_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&format!("</{tag}>"))? + start;
    Some(xml[start..end].trim().to_string())
}

/// Build the SAS query string for a single blob, HTTPS only.
fn user_delegation_sas_query(
    account: &str,
    canonical_path: &str,
    permissions: &str,
    expiry: &str,
    key: &UserDelegationKey,
) -> Result<String> {
    debug_assert!(canonical_path.starts_with(&format!("/blob/{account}/")));
    let fields = [
        permissions,
        "", // signed start
        expiry,
        canonical_path,
        &key.signed_oid,
        &key.signed_tid,
        &key.signed_start,
        &key.signed_expiry,
        &key.signed_service,
        &key.signed_version,
        "", // authorized user object id
        "", // unauthorized user object id
        "", // correlation id
        "", // signed ip
        "https",
        SAS_VERSION,
        "b",
        "", // snapshot time
        "", // encryption scope
        "", // rscc
        "", // rscd
        "", // rsce
        "", // rscl
        "", // rsct
    ];
    let decoded = BASE64
        .decode(key.value.as_bytes())
        .map_err(|e| ErrorCode::StorageOther(format!("invalid user delegation key: {e}")))?;
    let mut mac = Hmac::<Sha256>::new_from_slice(&decoded)
        .map_err(|e| ErrorCode::StorageOther(format!("invalid user delegation key: {e}")))?;
    mac.update(fields.join("\n").as_bytes());
    let signature = BASE64.encode(mac.finalize().into_bytes());

    Ok(url::form_urlencoded::Serializer::new(String::new())
        .append_pair("sv", SAS_VERSION)
        .append_pair("se", expiry)
        .append_pair("sp", permissions)
        .append_pair("sr", "b")
        .append_pair("skoid", &key.signed_oid)
        .append_pair("sktid", &key.signed_tid)
        .append_pair("skt", &key.signed_start)
        .append_pair("ske", &key.signed_expiry)
        .append_pair("sks", &key.signed_service)
        .append_pair("skv", &key.signed_version)
        .append_pair("spr", "https")
        .append_pair("sig", &signature)
        .finish())
}

fn format_iso(time: DateTime<Utc>) -> String {
    time.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(root: &str) -> StorageAzblobConfig {
        StorageAzblobConfig {
            endpoint_url: "https://account.blob.core.windows.net/".to_string(),
            container: "container".to_string(),
            account_name: String::new(),
            account_key: String::new(),
            root: root.to_string(),
            network_config: None,
        }
    }

    fn key() -> UserDelegationKey {
        UserDelegationKey {
            signed_oid: "oid".to_string(),
            signed_tid: "tid".to_string(),
            signed_start: "2022-03-01T08:12:34Z".to_string(),
            signed_expiry: "2022-03-08T08:12:34Z".to_string(),
            signed_service: "b".to_string(),
            signed_version: "2020-12-06".to_string(),
            value: "a2V5".to_string(),
        }
    }

    fn query_value(query: &str, name: &str) -> Option<String> {
        url::form_urlencoded::parse(query.as_bytes())
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.into_owned())
    }

    #[test]
    fn matches_reqsign_user_delegation_signature() {
        // Vector from reqsign-azure-storage `grants_real_user_delegation_protocol_shape`.
        let query = user_delegation_sas_query(
            "account",
            "/blob/account/container/path/to/blob name.txt",
            "rw",
            "2022-03-01T08:17:34Z",
            &key(),
        )
        .unwrap();
        assert_eq!(
            query_value(&query, "sig").as_deref(),
            Some("aoHQpVbSMBC3EY94Aw7g2XFUZxtqh48MWBZLxq32Q6g=")
        );
        assert_eq!(query_value(&query, "sr").as_deref(), Some("b"));
        assert_eq!(query_value(&query, "spr").as_deref(), Some("https"));
        assert_eq!(query_value(&query, "skoid").as_deref(), Some("oid"));
        assert!(query_value(&query, "st").is_none());
    }

    #[test]
    fn builds_encoded_url_and_raw_canonical_path() {
        let target = BlobTarget::new(&config("/stage/user/u1/"), "dir/a b.csv").unwrap();
        assert_eq!(target.account, "account");
        assert_eq!(
            target.url(),
            "https://account.blob.core.windows.net/container/stage/user/u1/dir/a%20b.csv"
        );
        assert_eq!(
            target.canonical_path(),
            "/blob/account/container/stage/user/u1/dir/a b.csv"
        );
    }

    #[test]
    fn prefers_configured_account_name() {
        let mut cfg = config("");
        cfg.account_name = "configured".to_string();
        assert_eq!(BlobTarget::new(&cfg, "f").unwrap().account, "configured");
    }

    #[test]
    fn rejects_directory_and_unknown_account() {
        assert!(BlobTarget::new(&config(""), "dir/").is_err());
        let mut cfg = config("");
        cfg.endpoint_url = "https://example.com".to_string();
        assert!(BlobTarget::new(&cfg, "f").is_err());
    }

    #[test]
    fn parses_user_delegation_key() {
        let xml = "<?xml version=\"1.0\" encoding=\"utf-8\"?><UserDelegationKey><SignedOid>oid</SignedOid><SignedTid>tid</SignedTid><SignedStart>2022-03-01T08:12:34Z</SignedStart><SignedExpiry>2022-03-08T08:12:34Z</SignedExpiry><SignedService>b</SignedService><SignedVersion>2020-12-06</SignedVersion><Value>a2V5</Value></UserDelegationKey>";
        assert_eq!(parse_user_delegation_key(xml).unwrap(), key());
        assert!(parse_user_delegation_key("<UserDelegationKey/>").is_err());
    }

    #[test]
    fn write_operation_permissions_and_method() {
        let op = AzblobPresignOp::Write { content_type: None };
        assert_eq!(op.permissions(), "cw");
        assert_eq!(op.method(), Method::PUT);
        assert_eq!(AzblobPresignOp::Read.permissions(), "r");
    }

    #[tokio::test]
    async fn rejects_expire_beyond_azure_limit() {
        let err = azblob_user_delegation_presign(
            &config(""),
            "f",
            AzblobPresignOp::Read,
            MAX_EXPIRE + Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert!(err.message().contains("expire"));
    }
}
