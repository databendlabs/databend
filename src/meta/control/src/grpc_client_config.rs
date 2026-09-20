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

use clap::Args;
use databend_meta_client::RpcClientTlsConfig;
use serde::Deserialize;
use serde::Serialize;

use crate::grpc_client_auth::GrpcClientAuth;
use crate::grpc_client_auth::GrpcClientAuthArgs;

/// Connection settings of an administrative gRPC client: the credentials
/// and, when TLS is enabled, the server root CA and domain name.
#[derive(Clone, Debug, Default)]
pub struct GrpcClientConfig {
    pub auth: GrpcClientAuth,
    /// `None` connects in plaintext.
    pub tls: Option<RpcClientTlsConfig>,
}

// Command-line inputs for `GrpcClientConfig`. Not a doc comment: clap applies
// a flattened `Args` struct's doc comment as the binary's `about` text.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq, Args)]
#[serde(default)]
pub struct GrpcClientConfigArgs {
    #[clap(flatten)]
    auth: GrpcClientAuthArgs,
}

impl GrpcClientConfigArgs {
    /// Loads and validates the effective administrative gRPC client settings.
    ///
    /// TLS cannot be configured from the command line yet, so the client
    /// always connects in plaintext.
    pub fn load(&self) -> anyhow::Result<GrpcClientConfig> {
        let auth = self.auth.load()?;
        Ok(GrpcClientConfig { auth, tls: None })
    }
}

#[cfg(test)]
mod tests {
    use super::GrpcClientConfigArgs;

    #[test]
    fn test_default_connects_in_plaintext() -> anyhow::Result<()> {
        let args = GrpcClientConfigArgs::default();

        let config = args.load()?;

        let username = config.auth.username();
        let tls_enabled = config.tls.is_some();
        assert_eq!(username, "root");
        assert!(!tls_enabled);
        Ok(())
    }
}
