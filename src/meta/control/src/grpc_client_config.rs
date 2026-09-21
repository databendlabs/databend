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

use anyhow::bail;
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

    /// PEM file with the root CA that signed the gRPC server certificate.
    /// Set together with --grpc-tls-domain-name to connect over TLS.
    #[clap(long, value_name = "PATH", global = true)]
    grpc_tls_ca_cert: Option<String>,

    /// Domain name the gRPC server certificate must be valid for.
    /// Set together with --grpc-tls-ca-cert to connect over TLS.
    #[clap(long, value_name = "DOMAIN", global = true)]
    grpc_tls_domain_name: Option<String>,
}

impl GrpcClientConfigArgs {
    /// Loads and validates the effective administrative gRPC client settings.
    pub fn load(&self) -> anyhow::Result<GrpcClientConfig> {
        let auth = self.auth.load()?;
        let tls = self.load_tls()?;
        Ok(GrpcClientConfig { auth, tls })
    }

    fn load_tls(&self) -> anyhow::Result<Option<RpcClientTlsConfig>> {
        let ca_cert = self.grpc_tls_ca_cert.as_deref();
        let domain_name = self.grpc_tls_domain_name.as_deref();
        let (ca_cert, domain_name) = match (ca_cert, domain_name) {
            (None, None) => return Ok(None),
            (Some(ca_cert), Some(domain_name)) => (ca_cert, domain_name),
            _ => bail!("--grpc-tls-ca-cert and --grpc-tls-domain-name must be set together"),
        };
        if ca_cert.is_empty() {
            bail!("gRPC TLS CA certificate path must not be empty");
        }
        if domain_name.is_empty() {
            bail!("gRPC TLS domain name must not be empty");
        }
        Ok(Some(RpcClientTlsConfig {
            rpc_tls_server_root_ca_cert: ca_cert.to_string(),
            domain_name: domain_name.to_string(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::GrpcClientConfig;
    use super::GrpcClientConfigArgs;

    /// A command line that flattens the client options, as the binaries do.
    #[derive(Parser)]
    struct TestCli {
        #[clap(flatten)]
        client_config: GrpcClientConfigArgs,
    }

    fn load(args: &[&str]) -> anyhow::Result<GrpcClientConfig> {
        let mut argv = vec!["metactl"];
        argv.extend_from_slice(args);
        let cli = TestCli::try_parse_from(argv)?;
        cli.client_config.load()
    }

    #[test]
    fn test_default_connects_in_plaintext() -> anyhow::Result<()> {
        let config = load(&[])?;

        let username = config.auth.username();
        let tls_enabled = config.tls.is_some();
        assert_eq!(username, "root");
        assert!(!tls_enabled);
        Ok(())
    }

    #[test]
    fn test_ca_cert_and_domain_name_enable_tls() -> anyhow::Result<()> {
        let config = load(&[
            "--grpc-tls-ca-cert",
            "ca.pem",
            "--grpc-tls-domain-name",
            "localhost",
        ])?;

        let tls = config.tls.unwrap();
        assert_eq!(tls.rpc_tls_server_root_ca_cert, "ca.pem");
        assert_eq!(tls.domain_name, "localhost");
        Ok(())
    }

    #[test]
    fn test_reject_ca_cert_without_domain_name() {
        let result = load(&["--grpc-tls-ca-cert", "ca.pem"]);

        let error = result.unwrap_err();
        let message = error.to_string();
        assert_eq!(
            message,
            "--grpc-tls-ca-cert and --grpc-tls-domain-name must be set together"
        );
    }

    #[test]
    fn test_reject_domain_name_without_ca_cert() {
        let result = load(&["--grpc-tls-domain-name", "localhost"]);

        let error = result.unwrap_err();
        let message = error.to_string();
        assert_eq!(
            message,
            "--grpc-tls-ca-cert and --grpc-tls-domain-name must be set together"
        );
    }

    #[test]
    fn test_reject_empty_ca_cert() {
        let result = load(&[
            "--grpc-tls-ca-cert",
            "",
            "--grpc-tls-domain-name",
            "localhost",
        ]);

        let error = result.unwrap_err();
        let message = error.to_string();
        assert_eq!(message, "gRPC TLS CA certificate path must not be empty");
    }

    #[test]
    fn test_reject_empty_domain_name() {
        let result = load(&["--grpc-tls-ca-cert", "ca.pem", "--grpc-tls-domain-name", ""]);

        let error = result.unwrap_err();
        let message = error.to_string();
        assert_eq!(message, "gRPC TLS domain name must not be empty");
    }
}
