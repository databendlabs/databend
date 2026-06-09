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

use databend_common_base::base::GlobalInstance;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use log::warn;
use tokio::net::lookup_host;
use url::Url;

use crate::EndpointUrlPolicy;
use crate::EndpointUrlPolicyConfig;

const BUILTIN_METADATA_HOSTS: &[&str] = &["metadata.google.internal"];

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
        let port: u16 = port
            .parse()
            .map_err(|err| format!("invalid socket port {value}: {err}"))?;
        Ok(Self::new(host.trim_matches(['[', ']']), port))
    }
}

impl ProtectedSocket {
    fn new(host: impl ToString, port: u16) -> Self {
        let host = normalize_host(&host.to_string());
        let ip = host.parse::<IpAddr>().ok();
        Self { host, ip, port }
    }

    fn matches_host(&self, host: &str, port: u16) -> bool {
        self.port == port && self.host == normalize_host(host)
    }

    fn matches_ip(&self, ip: IpAddr, port: u16) -> bool {
        self.port == port && self.ip == Some(ip)
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
        let _ = CompiledPolicy::try_from(&config)?;
        GlobalInstance::set(config);
        Ok(())
    }

    fn config() -> EndpointUrlPolicyConfig {
        GlobalInstance::try_get().unwrap_or_default()
    }
}

pub async fn check_storage_endpoint_url(url: &Url) -> Result<()> {
    let config = EndpointUrlPolicyRegistry::config();
    let policy = CompiledPolicy::try_from(&config)?;
    policy.check_url_without_dns(url)?;

    let Some(host) = url.host_str() else {
        return Ok(());
    };
    if host.parse::<IpAddr>().is_ok() {
        return Ok(());
    }

    let port = endpoint_port(url)?;
    let lookup = lookup_host((host, port)).await.map_err(|err| {
        ErrorCode::InvalidConfig(format!(
            "Storage endpoint is denied by egress policy: host={host}, reason=dns lookup failed: {err}"
        ))
    })?;

    let mut resolved = Vec::new();
    for addr in lookup {
        let ip = addr.ip();
        resolved.push(ip);
        policy.check_ip(host, port, ip)?;
    }

    if resolved.is_empty() {
        return Err(ErrorCode::InvalidConfig(format!(
            "Storage endpoint is denied by egress policy: host={host}, reason=dns lookup returned no addresses"
        )));
    }

    Ok(())
}

pub fn check_storage_endpoint_url_without_dns(endpoint: &str) -> Result<()> {
    let url = normalize_endpoint_url(endpoint)?;
    let config = EndpointUrlPolicyRegistry::config();
    let policy = CompiledPolicy::try_from(&config)?;
    policy.check_url_without_dns(&url)
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

        if let Some(reason) = builtin_blocked_host_reason(&host) {
            if matches!(self.policy, EndpointUrlPolicy::Strict)
                && !host_matches_any(&host, &self.allowed_hosts)
            {
                return deny(&host, None, reason);
            }
            warn_permissive(&self.policy, &host, None, reason);
        }

        if let Ok(ip) = host.parse::<IpAddr>() {
            return self.check_ip(&host, port, ip);
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

        if let Some(reason) = builtin_blocked_ip_reason(ip) {
            if matches!(self.policy, EndpointUrlPolicy::Strict) && !allowed {
                return deny(host, Some(ip), reason);
            }
            warn_permissive(&self.policy, host, Some(ip), reason);
        }

        Ok(())
    }
}

fn endpoint_port(url: &Url) -> Result<u16> {
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
            host == suffix || host.ends_with(&format!(".{suffix}"))
        } else {
            host == *pattern
        }
    })
}

fn builtin_blocked_host_reason(host: &str) -> Option<&'static str> {
    BUILTIN_METADATA_HOSTS
        .iter()
        .any(|blocked| *blocked == host)
        .then_some("metadata host")
}

fn builtin_blocked_ip_reason(ip: IpAddr) -> Option<&'static str> {
    match ip {
        IpAddr::V4(ip) => blocked_ipv4_reason(ip),
        IpAddr::V6(ip) => blocked_ipv6_reason(ip),
    }
}

fn blocked_ipv4_reason(ip: Ipv4Addr) -> Option<&'static str> {
    let octets = ip.octets();
    if ip.is_loopback() {
        return Some("loopback");
    }
    if ip.is_private() {
        return Some("private");
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
    if ip == Ipv4Addr::new(169, 254, 169, 254) {
        return Some("metadata");
    }
    None
}

fn blocked_ipv6_reason(ip: Ipv6Addr) -> Option<&'static str> {
    let segments = ip.segments();
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

fn normalize_host(host: &str) -> String {
    host.trim()
        .trim_matches(['[', ']'])
        .trim_end_matches('.')
        .to_ascii_lowercase()
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
            "http://127.0.0.1:1",
            "http://10.0.0.1:1",
            "http://172.16.0.1:1",
            "http://192.168.0.1:1",
            "http://169.254.1.1:1",
            "http://169.254.169.254:80",
            "http://224.0.0.1:1",
            "http://0.0.0.0:1",
        ] {
            assert!(check(&policy, endpoint).is_err(), "{endpoint}");
        }
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

    #[test]
    fn test_host_wildcard() {
        assert!(host_matches_any("foo.example.com", &[
            "*.example.com".to_string()
        ]));
        assert!(host_matches_any("example.com", &[
            "*.example.com".to_string()
        ]));
        assert!(!host_matches_any("badexample.com", &[
            "*.example.com".to_string()
        ]));
    }
}
