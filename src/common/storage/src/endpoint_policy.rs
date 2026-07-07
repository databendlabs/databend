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
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_base::http_client::resolve_global_dns;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::storage::StorageParams;
use log::warn;
use url::Url;

use crate::EndpointUrlPolicy;
use crate::EndpointUrlPolicyConfig;

#[derive(Clone, Debug)]
struct IpCidr {
    network: IpAddr,
    prefix: u8,
}

impl IpCidr {
    fn contains(&self, ip: IpAddr) -> bool {
        match (self.network, ip) {
            (IpAddr::V4(network), IpAddr::V4(ip)) => {
                let mask = if self.prefix == 0 {
                    0
                } else {
                    u32::MAX << (32 - self.prefix)
                };
                u32::from(network) & mask == u32::from(ip) & mask
            }
            (IpAddr::V6(network), IpAddr::V6(ip)) => {
                let mask = if self.prefix == 0 {
                    0
                } else {
                    u128::MAX << (128 - self.prefix)
                };
                u128::from(network) & mask == u128::from(ip) & mask
            }
            _ => false,
        }
    }
}

impl FromStr for IpCidr {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let (ip, prefix) = match value.split_once('/') {
            Some((ip, prefix)) => {
                let ip: IpAddr = ip
                    .parse()
                    .map_err(|err| format!("invalid cidr ip {value}: {err}"))?;
                let prefix: u8 = prefix
                    .parse()
                    .map_err(|err| format!("invalid cidr prefix {value}: {err}"))?;
                (ip, prefix)
            }
            None => {
                let ip: IpAddr = value
                    .parse()
                    .map_err(|err| format!("invalid ip address {value}: {err}"))?;
                let prefix = match ip {
                    IpAddr::V4(_) => 32,
                    IpAddr::V6(_) => 128,
                };
                (ip, prefix)
            }
        };

        let max_prefix = match ip {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        if prefix > max_prefix {
            return Err(format!("invalid cidr prefix {value}"));
        }

        Ok(Self {
            network: ip,
            prefix,
        })
    }
}

#[derive(Clone, Debug)]
struct ProtectedSocket {
    host: String,
    ip: Option<IpAddr>,
    port: u16,
    wildcard_bind: bool,
}

impl FromStr for ProtectedSocket {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let value = value.trim();
        let authority = if value.contains("://") {
            let url = Url::parse(value).map_err(|err| format!("invalid socket {value}: {err}"))?;
            let host = url
                .host_str()
                .ok_or_else(|| format!("missing host in socket {value}"))?;
            let port = url
                .port_or_known_default()
                .ok_or_else(|| format!("missing port in socket {value}"))?;
            return Ok(Self::new(host, port));
        } else {
            value
        };

        let socket = authority
            .parse::<SocketAddr>()
            .map(|addr| Self::new(addr.ip().to_string(), addr.port()));
        if let Ok(socket) = socket {
            return Ok(socket);
        }

        let (host, port) = authority
            .rsplit_once(':')
            .ok_or_else(|| format!("missing port in socket {value}"))?;
        if host.contains(':') && !(host.starts_with('[') && host.ends_with(']')) {
            return Err(format!(
                "invalid socket {value}: IPv6 socket addresses must be bracketed, e.g. [::1]:9191"
            ));
        }
        let port: u16 = port
            .parse()
            .map_err(|err| format!("invalid socket port {value}: {err}"))?;
        let host = host
            .strip_prefix('[')
            .and_then(|s| s.strip_suffix(']'))
            .unwrap_or(host);
        Ok(Self::new(host, port))
    }
}

impl ProtectedSocket {
    fn new(host: impl ToString, port: u16) -> Self {
        let host = normalize_host(&host.to_string());
        let ip = host.parse::<IpAddr>().ok();
        let wildcard_bind = ip.is_some_and(|ip| ip.is_unspecified());
        Self {
            host,
            ip,
            port,
            wildcard_bind,
        }
    }

    fn matches_host(&self, host: &str, port: u16) -> bool {
        if self.port != port {
            return false;
        }

        let host = normalize_host(host);
        if self.host == host {
            return true;
        }

        self.wildcard_bind
            && host
                .parse::<IpAddr>()
                .is_ok_and(is_protected_wildcard_target)
    }

    fn matches_ip(&self, ip: IpAddr, port: u16) -> bool {
        self.port == port
            && (self.ip == Some(ip) || (self.wildcard_bind && is_protected_wildcard_target(ip)))
    }

    fn is_hostname_only(&self) -> bool {
        self.ip.is_none()
    }
}

#[derive(Clone, Debug)]
struct CompiledPolicy {
    policy: EndpointUrlPolicy,
    allowed_hosts: Vec<String>,
    blocked_hosts: Vec<String>,
    allowed_cidrs: Vec<IpCidr>,
    blocked_cidrs: Vec<IpCidr>,
    protected_sockets: Vec<ProtectedSocket>,
}

impl TryFrom<&EndpointUrlPolicyConfig> for CompiledPolicy {
    type Error = ErrorCode;

    fn try_from(config: &EndpointUrlPolicyConfig) -> Result<Self> {
        Ok(Self {
            policy: config.policy.clone(),
            allowed_hosts: config
                .allowed_hosts
                .iter()
                .map(|host| normalize_host(host))
                .collect(),
            blocked_hosts: config
                .blocked_hosts
                .iter()
                .map(|host| normalize_host(host))
                .collect(),
            allowed_cidrs: parse_cidrs(&config.allowed_cidrs)?,
            blocked_cidrs: parse_cidrs(&config.blocked_cidrs)?,
            protected_sockets: parse_protected_sockets(&config.protected_sockets)?,
        })
    }
}

pub struct EndpointUrlPolicyRegistry;

impl EndpointUrlPolicyRegistry {
    pub fn init(config: EndpointUrlPolicyConfig) -> Result<()> {
        GlobalInstance::set(Arc::new(CompiledPolicy::try_from(&config)?));
        Ok(())
    }

    fn policy() -> Arc<CompiledPolicy> {
        GlobalInstance::try_get().unwrap_or_else(|| {
            Arc::new(
                CompiledPolicy::try_from(&EndpointUrlPolicyConfig::default())
                    .expect("default endpoint url policy must be valid"),
            )
        })
    }
}

/// Result of a request-time endpoint policy check.
///
/// `resolved_addrs` carries the validated `(ip, port)` targets so that the
/// HTTP layer can build a pinned, redirect-disabled `reqwest::Client` and
/// avoid a second uncontrolled DNS lookup before the actual request. The
/// field is populated for both hostname endpoints (via DNS resolution) and
/// IP-literal endpoints (using the literal IP directly). It is only `None`
/// when the URL had no host at all (in practice unreachable for the
/// normalized URLs the policy accepts).
#[derive(Clone, Debug, Default)]
pub struct EndpointUrlCheck {
    pub resolved_addrs: Option<Vec<SocketAddr>>,
}

pub async fn check_storage_endpoint_url(url: &Url) -> Result<EndpointUrlCheck> {
    let policy = EndpointUrlPolicyRegistry::policy();
    policy.check_url_with_dns(url).await
}

/// Validate every external URL field in a `StorageParams`.
///
/// Single chokepoint for SQL definition paths that build `StorageParams` from
/// user input. Per-variant URL fields (including secondary endpoints such as
/// OSS `presign_endpoint_url`) all flow through this async check, so callers
/// do not have to remember which fields are URLs. Strict-mode DNS validation
/// runs here for earlier user feedback; the request-time check in
/// `check_storage_endpoint_url` remains the security boundary.
pub async fn check_storage_params_endpoints(sp: &StorageParams) -> Result<()> {
    let policy = EndpointUrlPolicyRegistry::policy();
    for endpoint in storage_params_endpoints(sp) {
        let url = normalize_endpoint_url(endpoint)?;
        policy.check_url_at_definition(&url).await?;
    }
    Ok(())
}

/// Enumerate every endpoint URL field of a `StorageParams` variant that the
/// egress policy validates. Variants without user-controlled URLs (Fs,
/// Memory, FTP, HDFS name node, Huggingface) return empty.
fn storage_params_endpoints(sp: &StorageParams) -> Vec<&str> {
    match sp {
        StorageParams::Azblob(c) => vec![c.endpoint_url.as_str()],
        StorageParams::S3(c) => vec![c.endpoint_url.as_str()],
        StorageParams::Gcs(c) => vec![c.endpoint_url.as_str()],
        StorageParams::Ipfs(c) => vec![c.endpoint_url.as_str()],
        StorageParams::Oss(c) => {
            let mut endpoints = vec![c.endpoint_url.as_str()];
            if !c.presign_endpoint_url.is_empty() {
                endpoints.push(c.presign_endpoint_url.as_str());
            }
            endpoints
        }
        StorageParams::Obs(c) => vec![c.endpoint_url.as_str()],
        StorageParams::Cos(c) => vec![c.endpoint_url.as_str()],
        StorageParams::Webhdfs(c) => vec![c.endpoint_url.as_str()],
        StorageParams::Http(c) => vec![c.endpoint_url.as_str()],
        StorageParams::Fs(_)
        | StorageParams::Ftp(_)
        | StorageParams::Hdfs(_)
        | StorageParams::Memory
        | StorageParams::Huggingface(_) => Vec::new(),
    }
}

pub fn normalize_endpoint_url(endpoint: &str) -> Result<Url> {
    let endpoint = endpoint.trim();
    let endpoint = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("https://{endpoint}")
    };
    let url = Url::parse(&endpoint)
        .map_err(|err| ErrorCode::InvalidConfig(format!("invalid storage endpoint url: {err}")))?;
    match url.scheme() {
        "http" | "https" => Ok(url),
        scheme => Err(ErrorCode::InvalidConfig(format!(
            "invalid storage endpoint url scheme: {scheme}"
        ))),
    }
}

impl CompiledPolicy {
    async fn check_url_with_dns(&self, url: &Url) -> Result<EndpointUrlCheck> {
        self.check_url_without_dns(url)?;

        let Some(host) = url.host_str() else {
            return Ok(EndpointUrlCheck::default());
        };
        let port = endpoint_port(url)?;
        // Url::host_str wraps IPv6 literals in `[ ]`; strip a single outer
        // pair before attempting to parse as an IP address so IPv6 literal
        // endpoints take the literal-IP branch instead of falling through
        // to DNS.
        let host_for_ip = strip_url_brackets(host);
        if let Ok(ip) = host_for_ip.parse::<IpAddr>() {
            self.check_request_ip(host, port, ip).await?;
            // Return the (host, ip:port) pair so the request-time path can
            // build a pinned, redirect-disabled reqwest::Client for IP-literal
            // endpoints too. Without this, the fetch path would fall back to
            // the global client whose default redirect policy lets an attacker
            // bounce the request via 30x to a different host and bypass the
            // egress policy.
            return Ok(EndpointUrlCheck {
                resolved_addrs: Some(vec![SocketAddr::new(ip, port)]),
            });
        }

        let resolved_addrs = resolve_global_dns(host, port).await.map_err(|err| {
            ErrorCode::InvalidConfig(format!(
                "Storage endpoint is denied by egress policy: host={host}, reason=dns lookup failed: {err}"
            ))
        })?;

        let mut resolved_ips = Vec::new();
        for addr in &resolved_addrs {
            let ip = addr.ip();
            resolved_ips.push(ip);
            self.check_request_ip(host, port, ip).await?;
        }

        if resolved_ips.is_empty() {
            return Err(ErrorCode::InvalidConfig(format!(
                "Storage endpoint is denied by egress policy: host={host}, reason=dns lookup returned no addresses"
            )));
        }

        Ok(EndpointUrlCheck {
            resolved_addrs: Some(resolved_addrs),
        })
    }

    async fn check_url_at_definition(&self, url: &Url) -> Result<()> {
        self.check_url_without_dns(url)?;

        let Some(host) = url.host_str() else {
            return Ok(());
        };
        if strip_url_brackets(host).parse::<IpAddr>().is_ok() {
            return Ok(());
        }

        // Strict mode always validates the resolved IPs so the user gets a
        // policy error before CREATE succeeds. Permissive mode only does the
        // DNS roundtrip when a blocklist that needs the resolved IP is
        // configured (`blocked_cidrs` or hostname-only `protected_sockets`);
        // otherwise nothing the resolved IP carries can change the verdict
        // and we save the lookup. Request-time validation via
        // `check_storage_endpoint_url` remains the security boundary in
        // either mode.
        let needs_dns = match self.policy {
            EndpointUrlPolicy::Strict | EndpointUrlPolicy::Allowlist => true,
            EndpointUrlPolicy::Permissive => {
                !self.blocked_cidrs.is_empty()
                    || self
                        .protected_sockets
                        .iter()
                        .any(ProtectedSocket::is_hostname_only)
            }
        };
        if !needs_dns {
            return Ok(());
        }

        self.check_url_with_dns(url).await.map(|_| ())
    }

    fn check_url_without_dns(&self, url: &Url) -> Result<()> {
        let Some(host) = url.host_str() else {
            return Ok(());
        };
        let port = endpoint_port(url)?;
        let host = normalize_host(host);

        if self
            .protected_sockets
            .iter()
            .any(|socket| socket.matches_host(&host, port))
        {
            return deny(&host, None, "protected databend-meta endpoint");
        }

        if host_matches_any(&host, &self.blocked_hosts) {
            return deny(&host, None, "blocked host");
        }

        if let Ok(ip) = host.parse::<IpAddr>() {
            return self.check_ip(&host, port, ip);
        }

        // Allowlist mode rejects hostnames not covered by any allowlist
        // entry. If allowed_cidrs is configured we must defer to the DNS
        // phase so check_ip can evaluate the resolved addresses against
        // those CIDRs.
        if matches!(self.policy, EndpointUrlPolicy::Allowlist)
            && !host_matches_any(&host, &self.allowed_hosts)
            && self.allowed_cidrs.is_empty()
        {
            return deny(&host, None, "not in allowlist");
        }

        Ok(())
    }

    fn check_ip(&self, host: &str, port: u16, ip: IpAddr) -> Result<()> {
        if self
            .protected_sockets
            .iter()
            .any(|socket| socket.matches_ip(ip, port))
        {
            return deny(host, Some(ip), "protected databend-meta endpoint");
        }

        if self.blocked_cidrs.iter().any(|cidr| cidr.contains(ip)) {
            return deny(host, Some(ip), "blocked cidr");
        }

        let allowed = host_matches_any(host, &self.allowed_hosts)
            || self.allowed_cidrs.iter().any(|cidr| cidr.contains(ip));

        if matches!(self.policy, EndpointUrlPolicy::Allowlist) && !allowed {
            return deny(host, Some(ip), "not in allowlist");
        }

        if let Some(reason) = builtin_blocked_ip_reason(ip) {
            if matches!(self.policy, EndpointUrlPolicy::Strict) && !allowed {
                return deny(host, Some(ip), reason);
            }
            warn_permissive(&self.policy, host, Some(ip), reason);
        }

        Ok(())
    }

    async fn check_request_ip(&self, host: &str, port: u16, ip: IpAddr) -> Result<()> {
        self.check_ip(host, port, ip)?;
        self.check_protected_hostname_alias(host, port, ip).await
    }

    async fn check_protected_hostname_alias(
        &self,
        endpoint_host: &str,
        port: u16,
        endpoint_ip: IpAddr,
    ) -> Result<()> {
        for socket in self
            .protected_sockets
            .iter()
            .filter(|socket| socket.port == port && socket.is_hostname_only())
        {
            let protected_addrs =
                resolve_global_dns(&socket.host, socket.port)
                    .await
                    .map_err(|err| {
                        ErrorCode::InvalidConfig(format!(
                            "Storage endpoint is denied by egress policy: host={endpoint_host}, ip={endpoint_ip}, reason=protected databend-meta endpoint dns lookup failed for {}: {err}",
                            socket.host
                        ))
                    })?;

            if protected_addrs.is_empty() {
                return Err(ErrorCode::InvalidConfig(format!(
                    "Storage endpoint is denied by egress policy: host={endpoint_host}, ip={endpoint_ip}, reason=protected databend-meta endpoint dns lookup returned no addresses for {}",
                    socket.host
                )));
            }

            if protected_addrs
                .iter()
                .any(|protected_addr| protected_addr.ip() == endpoint_ip)
            {
                return deny(
                    endpoint_host,
                    Some(endpoint_ip),
                    "protected databend-meta endpoint",
                );
            }
        }

        Ok(())
    }
}

fn endpoint_port(url: &Url) -> Result<u16> {
    // port_or_known_default returns None only for schemes other than http/https.
    // normalize_endpoint_url already rejects those, so this is effectively
    // unreachable for valid inputs.
    url.port_or_known_default().ok_or_else(|| {
        ErrorCode::InvalidConfig("storage endpoint url must include a port".to_string())
    })
}

fn parse_cidrs(values: &[String]) -> Result<Vec<IpCidr>> {
    values
        .iter()
        .map(|value| {
            value
                .parse::<IpCidr>()
                .map_err(|err| ErrorCode::InvalidConfig(err.to_string()))
        })
        .collect()
}

fn parse_protected_sockets(values: &[String]) -> Result<Vec<ProtectedSocket>> {
    values
        .iter()
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            value
                .parse::<ProtectedSocket>()
                .map_err(|err| ErrorCode::InvalidConfig(err.to_string()))
        })
        .collect()
}

fn host_matches_any(host: &str, patterns: &[String]) -> bool {
    let host = normalize_host(host);
    patterns.iter().any(|pattern| {
        if let Some(suffix) = pattern.strip_prefix("*.") {
            host.ends_with(&format!(".{suffix}"))
        } else {
            host == *pattern
        }
    })
}

fn builtin_blocked_ip_reason(ip: IpAddr) -> Option<&'static str> {
    match ip {
        IpAddr::V4(ip) => blocked_ipv4_reason(ip),
        IpAddr::V6(ip) => blocked_ipv6_reason(ip),
    }
}

fn is_protected_wildcard_target(ip: IpAddr) -> bool {
    builtin_blocked_ip_reason(ip).is_some()
}

fn blocked_ipv4_reason(ip: Ipv4Addr) -> Option<&'static str> {
    let octets = ip.octets();
    if ip.is_loopback() {
        return Some("loopback");
    }
    if ip.is_private() {
        return Some("private");
    }
    // Check the specific metadata IP before the broader link-local range so the
    // error message is accurate. 169.254.169.254 would otherwise match link-local.
    if ip == Ipv4Addr::new(169, 254, 169, 254) {
        return Some("metadata");
    }
    if octets[0] == 169 && octets[1] == 254 {
        return Some("link-local");
    }
    if ip.is_multicast() {
        return Some("multicast");
    }
    if ip.is_unspecified() || octets[0] == 0 {
        return Some("unspecified");
    }
    None
}

fn blocked_ipv6_reason(ip: Ipv6Addr) -> Option<&'static str> {
    let segments = ip.segments();
    if segments[0] == 0
        && segments[1] == 0
        && segments[2] == 0
        && segments[3] == 0
        && segments[4] == 0
        && segments[5] == 0xffff
    {
        let octets = ip.octets();
        return blocked_ipv4_reason(Ipv4Addr::new(
            octets[12], octets[13], octets[14], octets[15],
        ));
    }
    if segments[0] == 0
        && segments[1] == 0
        && segments[2] == 0
        && segments[3] == 0
        && segments[4] == 0
        && segments[5] == 0
        && !(segments[6] == 0 && (segments[7] == 0 || segments[7] == 1))
    {
        let octets = ip.octets();
        return blocked_ipv4_reason(Ipv4Addr::new(
            octets[12], octets[13], octets[14], octets[15],
        ));
    }
    if ip.is_loopback() {
        return Some("loopback");
    }
    if (segments[0] & 0xfe00) == 0xfc00 {
        return Some("private");
    }
    if (segments[0] & 0xffc0) == 0xfe80 {
        return Some("link-local");
    }
    if ip.is_multicast() {
        return Some("multicast");
    }
    if ip.is_unspecified() {
        return Some("unspecified");
    }
    None
}

/// Strip a single outer pair of `[ ]` from a host literal.
///
/// `Url::host_str` wraps IPv6 literals in brackets; downstream code that
/// wants to parse the host as an `IpAddr` or compare it as a bare hostname
/// needs the brackets removed. The pair is only stripped when both ends
/// match so other bracket characters in the input are left intact.
fn strip_url_brackets(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(host)
}

fn normalize_host(host: &str) -> String {
    let host = host.trim();
    let host = strip_url_brackets(host);
    host.trim_end_matches('.').to_ascii_lowercase()
}

fn deny(host: &str, ip: Option<IpAddr>, reason: &str) -> Result<()> {
    Err(ErrorCode::InvalidConfig(format!(
        "Storage endpoint is denied by egress policy: host={host}, ip={}, reason={reason}",
        DisplayIp(ip)
    )))
}

fn warn_permissive(policy: &EndpointUrlPolicy, host: &str, ip: Option<IpAddr>, reason: &str) {
    if matches!(policy, EndpointUrlPolicy::Permissive) {
        warn!(
            "Storage endpoint matches high-risk target but is allowed by permissive egress policy: host={}, ip={}, reason={}",
            host,
            DisplayIp(ip),
            reason
        );
    }
}

struct DisplayIp(Option<IpAddr>);

impl fmt::Display for DisplayIp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(ip) => write!(f, "{ip}"),
            None => write!(f, "-"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strict() -> CompiledPolicy {
        CompiledPolicy::try_from(&EndpointUrlPolicyConfig {
            policy: EndpointUrlPolicy::Strict,
            ..Default::default()
        })
        .unwrap()
    }

    fn check(policy: &CompiledPolicy, endpoint: &str) -> Result<()> {
        let url = normalize_endpoint_url(endpoint)?;
        policy.check_url_without_dns(&url)
    }

    #[test]
    fn test_strict_rejects_builtin_ip_ranges() {
        let policy = strict();
        for endpoint in [
            // IPv4 built-in blocked ranges
            "http://127.0.0.1:1",
            "http://10.0.0.1:1",
            "http://172.16.0.1:1",
            "http://192.168.0.1:1",
            "http://169.254.1.1:1",
            "http://169.254.169.254:80",
            "http://224.0.0.1:1",
            "http://0.0.0.0:1",
            // IPv4-mapped IPv6 (::ffff:x.x.x.x)
            "http://[::ffff:127.0.0.1]:1",
            "http://[::ffff:10.0.0.1]:1",
            "http://[::ffff:169.254.169.254]:80",
            // IPv4-compatible IPv6 (::x.x.x.x)
            "http://[::7f00:1]:1",
            // Native IPv6 blocked ranges
            "http://[::1]:1",          // loopback
            "http://[fc00::1]:1",      // ULA private (fc00::/7)
            "http://[fd12:3456::1]:1", // ULA private (fd00::/8, within fc00::/7)
            "http://[fe80::1]:1",      // link-local (fe80::/10)
            "http://[ff02::1]:1",      // multicast (ff00::/8)
            "http://[::]:1",           // unspecified
        ] {
            assert!(check(&policy, endpoint).is_err(), "{endpoint}");
        }
    }

    #[test]
    fn test_strict_allows_public_ip_without_allowlist() {
        let policy = strict();

        assert!(check(&policy, "https://93.184.216.34:443").is_ok());
    }

    #[tokio::test]
    async fn test_check_url_with_dns_returns_resolved_addrs_for_ip_literal() {
        // IP-literal endpoints must report their resolved address back so the
        // request-time path builds a pinned, redirect-disabled client (the
        // same protection hostname endpoints already get).
        let policy = strict();
        let url = normalize_endpoint_url("https://93.184.216.34:443").unwrap();

        let check = policy.check_url_with_dns(&url).await.unwrap();
        assert_eq!(
            check.resolved_addrs.as_deref(),
            Some(["93.184.216.34:443".parse::<SocketAddr>().unwrap()].as_slice())
        );
    }

    #[tokio::test]
    async fn test_check_url_with_dns_returns_resolved_addrs_for_ipv6_literal() {
        // Same guarantee for IPv6 literals: scheme/host/port carried through
        // verbatim, with the address pulled from the literal rather than DNS.
        let policy = CompiledPolicy::try_from(&EndpointUrlPolicyConfig {
            policy: EndpointUrlPolicy::Strict,
            allowed_cidrs: vec!["2606:2800:220:1::/64".to_string()],
            ..Default::default()
        })
        .unwrap();
        let url = normalize_endpoint_url("https://[2606:2800:220:1::1]:443").unwrap();

        let check = policy.check_url_with_dns(&url).await.unwrap();
        assert_eq!(
            check.resolved_addrs.as_deref(),
            Some(["[2606:2800:220:1::1]:443".parse::<SocketAddr>().unwrap()].as_slice())
        );
    }

    #[test]
    fn test_strict_allows_configured_private_cidr() {
        let policy = CompiledPolicy::try_from(&EndpointUrlPolicyConfig {
            policy: EndpointUrlPolicy::Strict,
            allowed_cidrs: vec!["127.0.0.1/32".to_string()],
            ..Default::default()
        })
        .unwrap();

        assert!(check(&policy, "http://127.0.0.1:9900").is_ok());
    }

    #[test]
    fn test_permissive_rejects_configured_blocklist() {
        let policy = CompiledPolicy::try_from(&EndpointUrlPolicyConfig {
            blocked_cidrs: vec!["127.0.0.1/32".to_string()],
            ..Default::default()
        })
        .unwrap();

        assert!(check(&policy, "http://127.0.0.1:9900").is_err());
    }

    #[test]
    fn test_protected_socket_is_always_rejected() {
        let policy = CompiledPolicy::try_from(&EndpointUrlPolicyConfig {
            protected_sockets: vec!["127.0.0.1:9191".to_string()],
            allowed_cidrs: vec!["127.0.0.1/32".to_string()],
            ..Default::default()
        })
        .unwrap();

        assert!(check(&policy, "http://127.0.0.1:9191").is_err());
        assert!(check(&policy, "http://127.0.0.1:9900").is_ok());
    }

    #[tokio::test]
    async fn test_definition_check_protected_socket_ip_literal_rejected() {
        // Error comes from check_url_without_dns matching the protected socket,
        // not from the Strict branch. Permissive mode is sufficient to trigger it.
        let policy = CompiledPolicy::try_from(&EndpointUrlPolicyConfig {
            protected_sockets: vec!["127.0.0.1:9191".to_string()],
            allowed_cidrs: vec!["127.0.0.1/32".to_string()],
            ..Default::default()
        })
        .unwrap();
        let url = normalize_endpoint_url("http://127.0.0.1:9191").unwrap();

        assert!(policy.check_url_at_definition(&url).await.is_err());
    }

    #[tokio::test]
    async fn test_definition_check_ip_literal_skips_dns_in_strict_mode() {
        // In Strict mode, check_url_at_definition must not attempt DNS for IP
        // literals. A public IP that passes the built-in checks should be
        // accepted without any network call.
        let policy = CompiledPolicy::try_from(&EndpointUrlPolicyConfig {
            policy: EndpointUrlPolicy::Strict,
            ..Default::default()
        })
        .unwrap();
        let url = normalize_endpoint_url("https://93.184.216.34:443").unwrap();

        assert!(policy.check_url_at_definition(&url).await.is_ok());
    }

    #[tokio::test]
    async fn test_definition_check_permissive_hostname_does_not_require_dns() {
        let policy = CompiledPolicy::try_from(&EndpointUrlPolicyConfig {
            policy: EndpointUrlPolicy::Permissive,
            ..Default::default()
        })
        .unwrap();
        let url = normalize_endpoint_url("http://does-not-resolve.invalid:9191").unwrap();

        assert!(policy.check_url_at_definition(&url).await.is_ok());
    }

    #[tokio::test]
    async fn test_definition_check_permissive_with_blocked_cidrs_resolves_hostname() {
        // With an explicit blocked_cidrs configured, permissive mode must run
        // the resolved IPs through the policy. The hostname is unresolvable,
        // so we expect a dns-lookup-failed policy error rather than a silent
        // pass.
        let policy = CompiledPolicy::try_from(&EndpointUrlPolicyConfig {
            policy: EndpointUrlPolicy::Permissive,
            blocked_cidrs: vec!["10.0.0.0/8".to_string()],
            ..Default::default()
        })
        .unwrap();
        let url = normalize_endpoint_url("http://does-not-resolve.invalid:9191").unwrap();

        let err = policy
            .check_url_at_definition(&url)
            .await
            .expect_err("permissive + blocked_cidrs must trigger DNS");
        assert!(
            err.to_string().contains("dns lookup failed"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_definition_check_permissive_with_hostname_protected_socket_resolves_hostname() {
        // Hostname-only protected sockets need DNS to detect IP aliases, so
        // permissive mode must also run DNS when one is configured.
        let policy = CompiledPolicy::try_from(&EndpointUrlPolicyConfig {
            policy: EndpointUrlPolicy::Permissive,
            protected_sockets: vec!["meta.internal:9191".to_string()],
            ..Default::default()
        })
        .unwrap();
        let url = normalize_endpoint_url("http://does-not-resolve.invalid:9191").unwrap();

        let err = policy
            .check_url_at_definition(&url)
            .await
            .expect_err("permissive + hostname protected socket must trigger DNS");
        assert!(
            err.to_string().contains("dns lookup failed"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_hostname_protected_socket_rejects_endpoint_ip_alias() {
        let policy = CompiledPolicy::try_from(&EndpointUrlPolicyConfig {
            protected_sockets: vec!["localhost:9191".to_string()],
            ..Default::default()
        })
        .unwrap();

        assert!(
            policy
                .check_request_ip("127.0.0.1", 9191, IpAddr::V4(Ipv4Addr::LOCALHOST))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_hostname_protected_socket_does_not_reject_different_port() {
        let policy = CompiledPolicy::try_from(&EndpointUrlPolicyConfig {
            protected_sockets: vec!["localhost:9191".to_string()],
            ..Default::default()
        })
        .unwrap();

        assert!(
            policy
                .check_request_ip("127.0.0.1", 9192, IpAddr::V4(Ipv4Addr::LOCALHOST))
                .await
                .is_ok()
        );
    }

    #[test]
    fn test_hostname_protected_socket_direct_host_rejected_without_dns() {
        let policy = CompiledPolicy::try_from(&EndpointUrlPolicyConfig {
            protected_sockets: vec!["localhost:9191".to_string()],
            ..Default::default()
        })
        .unwrap();

        assert!(check(&policy, "http://localhost:9191").is_err());
    }

    #[test]
    fn test_wildcard_protected_socket_rejects_local_targets() {
        let policy = CompiledPolicy::try_from(&EndpointUrlPolicyConfig {
            protected_sockets: vec!["0.0.0.0:9191".to_string(), "[::]:9191".to_string()],
            ..Default::default()
        })
        .unwrap();

        assert!(check(&policy, "http://127.0.0.1:9191").is_err());
        assert!(check(&policy, "http://10.0.0.1:9191").is_err());
        assert!(check(&policy, "http://[::1]:9191").is_err());
        assert!(check(&policy, "http://127.0.0.1:9900").is_ok());
    }

    #[test]
    fn test_protected_socket_ipv6_must_be_bracketed() {
        assert!(parse_protected_sockets(&["[::1]:9191".to_string()]).is_ok());
        assert!(parse_protected_sockets(&["[::]:9191".to_string()]).is_ok());
        assert!(parse_protected_sockets(&["::1:9191".to_string()]).is_err());
    }

    #[test]
    fn test_host_wildcard() {
        assert!(host_matches_any("foo.example.com", &[
            "*.example.com".to_string()
        ]));
        assert!(host_matches_any("a.b.example.com", &[
            "*.example.com".to_string()
        ]));
        assert!(!host_matches_any("example.com", &[
            "*.example.com".to_string()
        ]));
        assert!(host_matches_any(
            "example.com",
            &["example.com".to_string()]
        ));
        assert!(!host_matches_any("badexample.com", &[
            "*.example.com".to_string()
        ]));
    }

    #[test]
    fn test_storage_params_endpoints_includes_oss_presign() {
        // Both endpoint_url and presign_endpoint_url must flow through the
        // chokepoint so a future caller cannot smuggle an internal target via
        // the secondary URL field.
        use databend_common_meta_app::storage::StorageOssConfig;

        let sp = StorageParams::Oss(StorageOssConfig {
            endpoint_url: "https://oss.example.com".to_string(),
            presign_endpoint_url: "https://presign.example.com".to_string(),
            ..Default::default()
        });

        let endpoints = storage_params_endpoints(&sp);
        assert_eq!(endpoints, vec![
            "https://oss.example.com",
            "https://presign.example.com",
        ]);
    }

    #[test]
    fn test_storage_params_endpoints_skips_empty_oss_presign() {
        use databend_common_meta_app::storage::StorageOssConfig;

        let sp = StorageParams::Oss(StorageOssConfig {
            endpoint_url: "https://oss.example.com".to_string(),
            presign_endpoint_url: String::new(),
            ..Default::default()
        });

        let endpoints = storage_params_endpoints(&sp);
        assert_eq!(endpoints, vec!["https://oss.example.com"]);
    }

    #[test]
    fn test_storage_params_endpoints_empty_for_local_variants() {
        use databend_common_meta_app::storage::StorageFsConfig;

        for sp in [
            StorageParams::Fs(StorageFsConfig::default()),
            StorageParams::Memory,
        ] {
            assert!(storage_params_endpoints(&sp).is_empty());
        }
    }

    fn allowlist_only() -> CompiledPolicy {
        CompiledPolicy::try_from(&EndpointUrlPolicyConfig {
            policy: EndpointUrlPolicy::Allowlist,
            ..Default::default()
        })
        .unwrap()
    }

    #[test]
    fn test_allowlist_rejects_public_ip_without_allowlist() {
        let policy = allowlist_only();
        assert!(check(&policy, "https://93.184.216.34:443").is_err());
    }

    #[test]
    fn test_allowlist_rejects_private_ip_without_allowlist() {
        let policy = allowlist_only();
        assert!(check(&policy, "http://10.0.0.1:9000").is_err());
    }

    #[test]
    fn test_allowlist_allows_configured_cidr() {
        let policy = CompiledPolicy::try_from(&EndpointUrlPolicyConfig {
            policy: EndpointUrlPolicy::Allowlist,
            allowed_cidrs: vec!["93.184.216.0/24".to_string()],
            ..Default::default()
        })
        .unwrap();

        assert!(check(&policy, "https://93.184.216.34:443").is_ok());
        assert!(check(&policy, "https://8.8.8.8:443").is_err());
    }

    #[test]
    fn test_allowlist_allows_configured_host() {
        let policy = CompiledPolicy::try_from(&EndpointUrlPolicyConfig {
            policy: EndpointUrlPolicy::Allowlist,
            allowed_hosts: vec!["*.example.com".to_string()],
            ..Default::default()
        })
        .unwrap();

        assert!(check(&policy, "https://s3.example.com:443").is_ok());
        assert!(check(&policy, "https://evil.attacker.com:443").is_err());
    }

    #[test]
    fn test_allowlist_empty_allowlist_rejects_all() {
        let policy = allowlist_only();

        assert!(check(&policy, "https://93.184.216.34:443").is_err());
        assert!(check(&policy, "http://10.0.0.1:80").is_err());
        assert!(check(&policy, "http://127.0.0.1:80").is_err());
        assert!(check(&policy, "https://s3.amazonaws.com:443").is_err());
    }

    #[test]
    fn test_allowlist_blocked_hosts_still_rejected() {
        // Even if a host is in allowed_hosts, blocked_hosts takes precedence.
        let policy = CompiledPolicy::try_from(&EndpointUrlPolicyConfig {
            policy: EndpointUrlPolicy::Allowlist,
            allowed_hosts: vec!["*.example.com".to_string()],
            blocked_hosts: vec!["evil.example.com".to_string()],
            ..Default::default()
        })
        .unwrap();

        assert!(check(&policy, "https://good.example.com:443").is_ok());
        assert!(check(&policy, "https://evil.example.com:443").is_err());
    }

    #[test]
    fn test_allowlist_cidr_defers_hostname_to_dns_phase() {
        // When allowed_cidrs is configured, hostnames must not be early-rejected
        // so DNS resolution can check the resolved IP against the CIDR allowlist.
        let policy = CompiledPolicy::try_from(&EndpointUrlPolicyConfig {
            policy: EndpointUrlPolicy::Allowlist,
            allowed_cidrs: vec!["93.184.216.0/24".to_string()],
            ..Default::default()
        })
        .unwrap();

        // Hostname should pass the pre-DNS check (not early-rejected).
        let url = normalize_endpoint_url("https://example.com:443").unwrap();
        assert!(policy.check_url_without_dns(&url).is_ok());
    }

    #[test]
    fn test_allowlist_no_cidr_early_rejects_hostname() {
        // Without allowed_cidrs, hostnames not in allowed_hosts are
        // rejected early without DNS.
        let policy = CompiledPolicy::try_from(&EndpointUrlPolicyConfig {
            policy: EndpointUrlPolicy::Allowlist,
            allowed_hosts: vec!["good.example.com".to_string()],
            ..Default::default()
        })
        .unwrap();

        let url = normalize_endpoint_url("https://other.example.com:443").unwrap();
        assert!(policy.check_url_without_dns(&url).is_err());

        let url = normalize_endpoint_url("https://good.example.com:443").unwrap();
        assert!(policy.check_url_without_dns(&url).is_ok());
    }
}
