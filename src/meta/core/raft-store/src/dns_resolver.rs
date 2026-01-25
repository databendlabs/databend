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

//! Crate-local DNS resolver for hostname resolution.

use std::io;
use std::net::IpAddr;
use std::sync::LazyLock;

use hickory_resolver::TokioResolver;

/// Global DNS resolver instance for this crate.
static DNS_RESOLVER: LazyLock<io::Result<TokioResolver>> = LazyLock::new(|| {
    TokioResolver::builder_tokio()
        .map(|b| b.build())
        .map_err(io::Error::other)
});

/// Resolves a hostname to IP addresses using the global resolver.
pub(crate) async fn resolve(hostname: &str) -> io::Result<impl Iterator<Item = IpAddr> + '_> {
    let resolver = DNS_RESOLVER
        .as_ref()
        .map_err(|e| io::Error::other(e.to_string()))?;

    let lookup = resolver
        .lookup_ip(hostname)
        .await
        .map_err(io::Error::other)?;

    Ok(lookup.into_iter())
}
