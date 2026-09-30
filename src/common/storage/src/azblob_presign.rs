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

use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::DateTime;
use chrono::Timelike;
use chrono::Utc;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::storage::StorageAzblobConfig;
use databend_common_meta_app::storage::StorageParams;
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
use reqwest::StatusCode;
use serde::Deserialize;
use sha2::Sha256;

const STORAGE_SCOPE: &str = "https://storage.azure.com/.default";
const SAS_VERSION: &str = "2020-12-06";
const DEFAULT_AUTHORITY_HOST: &str = "https://login.microsoftonline.com/";
/// Largest `EXPIRE` accepted, matching Azure's 7-day user delegation key limit.
const MAX_EXPIRE: Duration = Duration::from_secs(7 * 24 * 3600);
/// Longest key (and SAS) validity requested from Azure. Azure limits the key
/// expiry to 7 days after *its* current time, so stay 5 minutes inside that
/// limit to tolerate a node clock running ahead of Azure.
const MAX_KEY_VALIDITY: chrono::Duration = chrono::Duration::minutes(7 * 24 * 60 - 5);
/// Start the key slightly in the past to tolerate a node clock running behind.
const KEY_START_SKEW: chrono::Duration = chrono::Duration::minutes(5);
/// Extra key validity beyond the requested SAS, so later presigns reuse the key.
const KEY_REUSE_WINDOW: chrono::Duration = chrono::Duration::hours(1);
/// Refresh a cached access token this long before it expires.
const TOKEN_REFRESH_BUFFER: chrono::Duration = chrono::Duration::minutes(2);

static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build azblob presign http client")
});

/// Every PRESIGN would otherwise pay an Entra token exchange plus a Get User
/// Delegation Key round trip, the way reqsign caches credentials on its loader.
static TOKEN_CACHE: LazyLock<ExpiringCache<String>> = LazyLock::new(ExpiringCache::default);
static KEY_CACHE: LazyLock<ExpiringCache<UserDelegationKey>> =
    LazyLock::new(ExpiringCache::default);

/// Values with an absolute expiry. Keys are derived from the process identity
/// and storage account, so there are only a handful of entries and no eviction
/// is needed. Concurrent refreshes at expiry are harmless.
struct ExpiringCache<V> {
    entries: Mutex<HashMap<String, (V, DateTime<Utc>)>>,
}

impl<V> Default for ExpiringCache<V> {
    fn default() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }
}

impl<V: Clone> ExpiringCache<V> {
    /// A cached value that stays valid at least until `valid_until`.
    fn get(&self, key: &str, valid_until: DateTime<Utc>) -> Option<(V, DateTime<Utc>)> {
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries
            .get(key)
            .filter(|(_, expires_at)| *expires_at >= valid_until)
            .cloned()
    }

    fn insert(&self, key: String, value: V, expires_at: DateTime<Utc>) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries.insert(key, (value, expires_at));
    }

    fn remove(&self, key: &str) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries.remove(key);
    }
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkloadIdentity {
    client_id: String,
    tenant_id: String,
    token_file: String,
    authority_host: String,
}

impl WorkloadIdentity {
    fn from_env() -> Option<Self> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// Variables injected by the AKS Workload Identity webhook; blank values
    /// count as missing.
    fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Option<Self> {
        let var = |name: &str| lookup(name).filter(|v| !v.trim().is_empty());
        Some(Self {
            client_id: var("AZURE_CLIENT_ID")?,
            tenant_id: var("AZURE_TENANT_ID")?,
            token_file: var("AZURE_FEDERATED_TOKEN_FILE")?,
            authority_host: var("AZURE_AUTHORITY_HOST")
                .unwrap_or_else(|| DEFAULT_AUTHORITY_HOST.to_string()),
        })
    }

    fn cache_key(&self) -> String {
        format!(
            "{}|{}|{}",
            self.authority_host.trim_end_matches('/'),
            self.tenant_id,
            self.client_id
        )
    }
}

/// The azblob config to presign with a user delegation SAS when the stage
/// operator cannot presign itself. `internal_params` is `None` for external
/// stages, which keep failing as unsupported.
pub fn azblob_user_delegation_presign_config(
    internal_params: Option<StorageParams>,
) -> Option<StorageAzblobConfig> {
    user_delegation_config(internal_params, WorkloadIdentity::from_env().as_ref())
}

fn user_delegation_config(
    internal_params: Option<StorageParams>,
    identity: Option<&WorkloadIdentity>,
) -> Option<StorageAzblobConfig> {
    match internal_params? {
        // A configured account key means presign should come from it, not WI.
        StorageParams::Azblob(cfg) if cfg.account_key.is_empty() && identity.is_some() => Some(cfg),
        _ => None,
    }
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
    let headers = presign_headers(op)?;

    let now = now_secs();
    let wanted = sas_expiry(now, expire)?;
    let (key, key_expiry) = user_delegation_key(&identity, &target, now, wanted).await?;
    // The SAS must not outlive the key that signs it.
    let expiry = wanted.min(key_expiry);
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
    Ok(PresignedRequest::new(op.method(), uri, headers))
}

fn presign_headers(op: AzblobPresignOp<'_>) -> Result<HeaderMap> {
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
    Ok(headers)
}

/// Current time truncated to whole seconds, the precision of SAS timestamps,
/// so comparisons against parsed Azure timestamps are exact.
fn now_secs() -> DateTime<Utc> {
    let now = Utc::now();
    now.with_nanosecond(0).unwrap_or(now)
}

/// Requested SAS expiry. `EXPIRE = 604800` is accepted but clamped to
/// `MAX_KEY_VALIDITY`, which Azure is guaranteed to accept despite clock skew.
fn sas_expiry(now: DateTime<Utc>, expire: Duration) -> Result<DateTime<Utc>> {
    let expire = chrono::Duration::from_std(expire)
        .map_err(|e| ErrorCode::BadArguments(format!("invalid presign expire: {e}")))?;
    Ok(now + expire.min(MAX_KEY_VALIDITY))
}

/// Key expiry to request for a new key: long enough to be reused for a while,
/// never beyond `MAX_KEY_VALIDITY`.
fn key_request_expiry(now: DateTime<Utc>, sas_expiry: DateTime<Utc>) -> DateTime<Utc> {
    (sas_expiry + KEY_REUSE_WINDOW).min(now + MAX_KEY_VALIDITY)
}

/// A user delegation key valid at least until `sas_expiry`, cached per
/// identity and storage account.
async fn user_delegation_key(
    identity: &WorkloadIdentity,
    target: &BlobTarget,
    now: DateTime<Utc>,
    sas_expiry: DateTime<Utc>,
) -> Result<(UserDelegationKey, DateTime<Utc>)> {
    let cache_key = format!("{}|{}", identity.cache_key(), target.endpoint);
    if let Some(cached) = KEY_CACHE.get(&cache_key, sas_expiry) {
        return Ok(cached);
    }
    let token = access_token(identity, now).await?;
    let result =
        fetch_user_delegation_key(target, &token, now, key_request_expiry(now, sas_expiry)).await;
    if result.is_err() {
        // The token may have been revoked; do not keep reusing it.
        TOKEN_CACHE.remove(&identity.cache_key());
    }
    let (key, expiry) = result?;
    KEY_CACHE.insert(cache_key, key.clone(), expiry);
    Ok((key, expiry))
}

async fn access_token(identity: &WorkloadIdentity, now: DateTime<Utc>) -> Result<String> {
    let cache_key = identity.cache_key();
    if let Some((token, _)) = TOKEN_CACHE.get(&cache_key, now + TOKEN_REFRESH_BUFFER) {
        return Ok(token);
    }
    let (token, lifetime) = fetch_access_token(identity).await?;
    if let Some(lifetime) = lifetime {
        TOKEN_CACHE.insert(cache_key, token.clone(), now + lifetime);
    }
    Ok(token)
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
    /// Seconds; some Entra endpoints return it as a string.
    #[serde(default)]
    expires_in: Option<ExpiresIn>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ExpiresIn {
    Number(i64),
    Text(String),
}

impl ExpiresIn {
    fn lifetime(&self) -> Option<chrono::Duration> {
        let secs = match self {
            ExpiresIn::Number(v) => *v,
            ExpiresIn::Text(v) => v.trim().parse().ok()?,
        };
        (secs > 0).then(|| chrono::Duration::seconds(secs))
    }
}

/// Entra error bodies carry error/error_codes/trace_id, never the assertion or
/// a token, so they are safe to surface.
#[derive(Debug, Default, Deserialize)]
struct EntraError {
    #[serde(default)]
    error: String,
    #[serde(default)]
    error_description: String,
    #[serde(default)]
    error_codes: Vec<i64>,
    #[serde(default)]
    trace_id: String,
}

fn entra_error_message(status: StatusCode, err: &EntraError) -> String {
    // The description repeats the trace/correlation ids on later lines.
    let description = err.error_description.lines().next().unwrap_or_default();
    let codes = err
        .error_codes
        .iter()
        .map(|c| c.to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "Entra ID token request failed with status {status}: error={} codes=[{codes}] trace_id={} {description}",
        err.error, err.trace_id
    )
    .trim_end()
    .to_string()
}

/// Throttling and server errors are transient; other failures usually mean a
/// Workload Identity or RBAC misconfiguration.
fn error_for_status(status: StatusCode, message: String) -> ErrorCode {
    if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
        ErrorCode::StorageOther(message)
    } else {
        ErrorCode::StoragePermissionDenied(message)
    }
}

/// Returns the token and, when Entra reports it, its lifetime.
async fn fetch_access_token(
    identity: &WorkloadIdentity,
) -> Result<(String, Option<chrono::Duration>)> {
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
        let err = resp.json::<EntraError>().await.unwrap_or_default();
        return Err(error_for_status(status, entra_error_message(status, &err)));
    }
    let body: TokenResponse = resp
        .json()
        .await
        .map_err(|e| ErrorCode::StorageOther(format!("parse Entra ID token: {e}")))?;
    let lifetime = body.expires_in.as_ref().and_then(ExpiresIn::lifetime);
    Ok((body.access_token, lifetime))
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
) -> Result<(UserDelegationKey, DateTime<Utc>)> {
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
        let message = extract_tag(&text, "Message").unwrap_or_default();
        return Err(error_for_status(
            status,
            format!(
                "user delegation key request failed with status {status}: {} {}",
                extract_tag(&text, "Code").unwrap_or_default(),
                message.lines().next().unwrap_or_default()
            )
            .trim_end()
            .to_string(),
        ));
    }
    let key = parse_user_delegation_key(&text)?;
    let key_expiry = DateTime::parse_from_rfc3339(&key.signed_expiry)
        .map_err(|e| ErrorCode::StorageOther(format!("invalid user delegation key expiry: {e}")))?
        .with_timezone(&Utc);
    Ok((key, key_expiry))
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

    fn identity() -> WorkloadIdentity {
        WorkloadIdentity {
            client_id: "client".to_string(),
            tenant_id: "tenant".to_string(),
            token_file: "/var/run/token".to_string(),
            authority_host: DEFAULT_AUTHORITY_HOST.to_string(),
        }
    }

    fn lookup<'a>(vars: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |name| {
            vars.iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| v.to_string())
        }
    }

    #[test]
    fn workload_identity_requires_all_webhook_variables() {
        let full = [
            ("AZURE_CLIENT_ID", "client"),
            ("AZURE_TENANT_ID", "tenant"),
            ("AZURE_FEDERATED_TOKEN_FILE", "/var/run/token"),
        ];
        assert_eq!(
            WorkloadIdentity::from_lookup(lookup(&full)),
            Some(identity())
        );

        let custom = [
            ("AZURE_CLIENT_ID", "client"),
            ("AZURE_TENANT_ID", "tenant"),
            ("AZURE_FEDERATED_TOKEN_FILE", "/var/run/token"),
            ("AZURE_AUTHORITY_HOST", "https://login.chinacloudapi.cn/"),
        ];
        assert_eq!(
            WorkloadIdentity::from_lookup(lookup(&custom))
                .unwrap()
                .authority_host,
            "https://login.chinacloudapi.cn/"
        );

        for missing in 0..full.len() {
            let mut vars = full.to_vec();
            vars.remove(missing);
            assert!(WorkloadIdentity::from_lookup(lookup(&vars)).is_none());
            let mut blank = full.to_vec();
            blank[missing].1 = "  ";
            assert!(WorkloadIdentity::from_lookup(lookup(&blank)).is_none());
        }
    }

    #[test]
    fn falls_back_only_for_keyless_internal_azblob_with_identity() {
        let id = identity();
        let azblob = || StorageParams::Azblob(config("/stage/internal/s1/"));

        // Keyless internal azblob stage with Workload Identity.
        assert_eq!(
            user_delegation_config(Some(azblob()), Some(&id)),
            Some(config("/stage/internal/s1/"))
        );
        // External stage.
        assert_eq!(user_delegation_config(None, Some(&id)), None);
        // No Workload Identity.
        assert_eq!(user_delegation_config(Some(azblob()), None), None);
        // A configured account key presigns through OpenDAL instead.
        let mut keyed = config("");
        keyed.account_key = "key".to_string();
        assert_eq!(
            user_delegation_config(Some(StorageParams::Azblob(keyed)), Some(&id)),
            None
        );
        // Other backends keep the original unsupported error.
        let fs = StorageParams::Fs(databend_common_meta_app::storage::StorageFsConfig::default());
        assert_eq!(user_delegation_config(Some(fs), Some(&id)), None);
    }

    #[test]
    fn write_presign_sets_blob_type_and_content_type() {
        let headers = presign_headers(AzblobPresignOp::Write {
            content_type: Some("text/csv"),
        })
        .unwrap();
        assert_eq!(headers.len(), 2);
        assert_eq!(headers["x-ms-blob-type"], "BlockBlob");
        assert_eq!(headers[http::header::CONTENT_TYPE], "text/csv");

        let headers = presign_headers(AzblobPresignOp::Write { content_type: None }).unwrap();
        assert_eq!(headers.len(), 1);
        assert_eq!(headers["x-ms-blob-type"], "BlockBlob");

        assert!(presign_headers(AzblobPresignOp::Read).unwrap().is_empty());
        assert!(
            presign_headers(AzblobPresignOp::Write {
                content_type: Some("bad\nvalue"),
            })
            .is_err()
        );
    }

    #[test]
    fn expiry_stays_within_azure_key_limit() {
        let now = DateTime::parse_from_rfc3339("2026-09-30T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let seven_days = chrono::Duration::days(7);

        let short = sas_expiry(now, Duration::from_secs(3600)).unwrap();
        assert_eq!(short, now + chrono::Duration::hours(1));
        assert_eq!(key_request_expiry(now, short), short + KEY_REUSE_WINDOW);

        // EXPIRE = 604800 is clamped so the key and SAS stay inside 7 days of
        // both the key start and Azure's current time, despite clock skew.
        let max = sas_expiry(now, MAX_EXPIRE).unwrap();
        assert_eq!(max, now + MAX_KEY_VALIDITY);
        let key_expiry = key_request_expiry(now, max);
        assert_eq!(key_expiry, now + MAX_KEY_VALIDITY);
        assert!(key_expiry >= max);
        assert!(key_expiry - (now - KEY_START_SKEW) <= seven_days);
        assert!(key_expiry - now < seven_days);
    }

    #[test]
    fn expiring_cache_returns_only_values_valid_long_enough() {
        let now = Utc::now();
        let cache = ExpiringCache::<String>::default();
        cache.insert(
            "k".to_string(),
            "v".to_string(),
            now + chrono::Duration::hours(1),
        );

        assert_eq!(cache.get("k", now).map(|(v, _)| v).as_deref(), Some("v"));
        assert!(cache.get("k", now + chrono::Duration::hours(2)).is_none());
        assert!(cache.get("other", now).is_none());
        cache.remove("k");
        assert!(cache.get("k", now).is_none());
    }

    #[test]
    fn token_lifetime_accepts_number_or_string() {
        assert_eq!(
            ExpiresIn::Number(3599).lifetime(),
            Some(chrono::Duration::seconds(3599))
        );
        assert_eq!(
            ExpiresIn::Text("3599".to_string()).lifetime(),
            Some(chrono::Duration::seconds(3599))
        );
        assert_eq!(ExpiresIn::Number(0).lifetime(), None);
        assert_eq!(ExpiresIn::Text("soon".to_string()).lifetime(), None);
    }

    #[test]
    fn transient_failures_are_not_permission_errors() {
        let code = |status| error_for_status(status, String::new()).code();
        let denied = ErrorCode::StoragePermissionDenied("").code();
        let other = ErrorCode::StorageOther("").code();
        assert_eq!(code(StatusCode::TOO_MANY_REQUESTS), other);
        assert_eq!(code(StatusCode::SERVICE_UNAVAILABLE), other);
        assert_eq!(code(StatusCode::BAD_REQUEST), denied);
        assert_eq!(code(StatusCode::FORBIDDEN), denied);
    }

    #[test]
    fn entra_error_message_surfaces_aadsts_details() {
        let err = EntraError {
            error: "invalid_client".to_string(),
            error_description: "AADSTS700213: No matching federated identity record found.\r\nTrace ID: t\r\nCorrelation ID: c".to_string(),
            error_codes: vec![700213],
            trace_id: "trace-1".to_string(),
        };
        let message = entra_error_message(StatusCode::BAD_REQUEST, &err);
        assert!(message.contains("400"));
        assert!(message.contains("error=invalid_client"));
        assert!(message.contains("codes=[700213]"));
        assert!(message.contains("trace_id=trace-1"));
        assert!(message.contains("AADSTS700213: No matching federated identity record found."));
        assert!(!message.contains("Correlation ID"));
    }
}
