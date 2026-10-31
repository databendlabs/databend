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

use std::sync::Arc;
use std::time::Duration;

use arrow_udf_python_worker::NetworkProxy;
use databend_common_base::base::GlobalInstance;
use databend_common_config::GlobalConfig;
use databend_common_config::InnerConfig;
use databend_common_meta_app::principal::ExternalAccessIntegration;
use databend_common_meta_app::principal::NetworkRule;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::UserApiProvider;
use databend_common_version::BUILD_INFO;
use databend_meta_client::RpcClientConf;
use databend_query_script_udf_support::python_udf_network_proxy;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::net::UnixStream;

struct AbortDriver(tokio::task::JoinHandle<()>);

impl Drop for AbortDriver {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn headers<S: tokio::io::AsyncRead + Unpin>(stream: &mut S) -> Vec<u8> {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        assert_eq!(stream.read(&mut byte).await.unwrap(), 1);
        bytes.push(byte[0]);
        assert!(bytes.len() < 16 * 1024);
    }
    bytes
}

async fn fixture_connection(
    listener: &TcpListener,
    marker: u8,
) -> anyhow::Result<(TcpStream, Vec<u8>)> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (mut stream, _) = listener.accept().await?;
        let mut byte = [0];
        stream.read_exact(&mut byte).await?;
        assert_eq!(byte[0], marker);
        let mut prefix = byte.to_vec();
        if marker == b'G' {
            prefix.extend(headers(&mut stream).await);
        }
        anyhow::Ok((stream, prefix))
    })
    .await?
}

async fn open(proxy: Arc<dyn NetworkProxy>, request: &str) -> (UnixStream, AbortDriver) {
    let (mut client, server) = UnixStream::pair().unwrap();
    let driver = AbortDriver(databend_common_base::runtime::spawn(async move {
        let _ = proxy.serve(server).await;
    }));
    client.write_all(request.as_bytes()).await.unwrap();
    (client, driver)
}

fn proxy(tenant: &Tenant, integration: &str) -> Arc<dyn NetworkProxy> {
    python_udf_network_proxy(tenant.tenant_name(), &[integration.into()])
        .unwrap()
        .unwrap()
}

// The test registry is immutable and keyed by thread name. Build each operator
// configuration in its own scope instead of overwriting global singletons.
fn configured_proxy(
    config: InnerConfig,
    tenant: Tenant,
    provider: Arc<UserApiProvider>,
    integration: &str,
) -> databend_common_exception::Result<Arc<dyn NetworkProxy>> {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let name = format!(
        "egress_config_{}",
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    );
    let integration = integration.to_owned();
    std::thread::Builder::new()
        .name(name.clone())
        .spawn(move || {
            GlobalInstance::init_testing(&name);
            GlobalConfig::init(&config, &BUILD_INFO)?;
            GlobalInstance::set(provider);
            let result =
                python_udf_network_proxy(tenant.tenant_name(), &[integration]).map(Option::unwrap);
            GlobalInstance::drop_testing(&name);
            result
        })
        .unwrap()
        .join()
        .unwrap()
}

async fn bind(
    provider: &UserApiProvider,
    tenant: &Tenant,
    name: &str,
    destination: String,
) -> anyhow::Result<()> {
    provider
        .add_network_rule(
            tenant,
            NetworkRule {
                name: name.into(),
                value_list: vec![destination],
            },
            &CreateOption::CreateOrReplace,
        )
        .await?;
    provider
        .add_external_access_integration(
            tenant,
            ExternalAccessIntegration {
                name: name.into(),
                allowed_network_rules: vec![name.into()],
                enabled: true,
            },
            &CreateOption::CreateOrReplace,
        )
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_forward_and_live_connection_revocation() -> anyhow::Result<()> {
    // Only this integration binary owns the registry; production code is imported
    // through its public factory instead of compiling it a second time via include!.
    let name = std::thread::current().name().unwrap().to_owned();
    GlobalInstance::init_testing(&name);
    assert!(python_udf_network_proxy("", &[])?.is_none());
    assert!(
        databend_common_base::runtime::catch_unwind(|| python_udf_network_proxy("tenant", &[
            "access".into()
        ]))
        .is_err()
    );
    let mut config = InnerConfig::default();
    assert!(!config.query.common.enable_python_udf_external_access);
    config.query.common.enable_python_udf_external_access = true;
    config.query.common.udf_server_allow_list =
        vec!["http://127.0.0.1".into(), "http://[::1]".into()];
    GlobalConfig::init(&config, &BUILD_INFO)?;
    let tenant = Tenant::new_literal("python_egress_tests");
    let provider = UserApiProvider::try_create_simple(RpcClientConf::empty(), &tenant).await?;
    GlobalInstance::set(provider.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    bind(&provider, &tenant, "access", format!("127.0.0.1:{port}")).await?;
    let authorized = proxy(&tenant, "access");

    assert!(
        configured_proxy(
            InnerConfig::default(),
            tenant.clone(),
            provider.clone(),
            "access"
        )
        .unwrap_err()
        .message()
        .contains("enable_python_udf_external_access")
    );
    let mut blocked_config = config.clone();
    blocked_config.query.common.udf_server_allow_list = vec!["https://denied.invalid".into()];
    let outside_ceiling = configured_proxy(
        blocked_config.clone(),
        tenant.clone(),
        provider.clone(),
        "access",
    )?;
    bind(&provider, &tenant, "other", "127.0.0.1:1".into()).await?;
    for blocked in [
        proxy(&Tenant::new_literal("foreign_tenant"), "access"),
        outside_ceiling,
        // The hostname ceiling permits this destination but the tenant rule does not.
        proxy(&tenant, "other"),
    ] {
        let (mut client, _driver) = open(
            blocked,
            &format!("CONNECT 127.0.0.1:{port} HTTP/1.1\r\nHost: attacker.invalid\r\n\r\n"),
        )
        .await;
        assert!(headers(&mut client).await.starts_with(b"HTTP/1.1 403"));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), listener.accept())
                .await
                .is_err(),
            "denied destination was connected"
        );
    }
    // Even an enabled operator switch and a tenant-approved loopback rule must
    // not allow a private connection without an explicit IP ceiling exception.
    blocked_config.query.common.udf_server_allow_list.clear();
    let no_ip_exception =
        configured_proxy(blocked_config, tenant.clone(), provider.clone(), "access")?;
    let (mut client, _driver) = open(
        no_ip_exception,
        &format!("CONNECT 127.0.0.1:{port} HTTP/1.1\r\nHost: ignored.invalid\r\n\r\n"),
    )
    .await;
    let mut bytes = Vec::new();
    client.read_to_end(&mut bytes).await?;
    assert!(String::from_utf8_lossy(&bytes).contains("non-public"));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err()
    );

    let mut backend = AbortDriver(databend_common_base::runtime::spawn(async move {
        let (mut stream, request) = fixture_connection(&listener, b'G').await.unwrap();
        let request = String::from_utf8(request).unwrap();
        assert!(request.starts_with("GET /events?k=v HTTP/1.1\r\n"));
        assert!(
            request
                .to_ascii_lowercase()
                .contains(&format!("host: 127.0.0.1:{port}\r\n"))
        );
        assert!(!request.contains("attacker.invalid"));
        assert!(!request.to_ascii_lowercase().contains("proxy-authorization"));
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n7")
            .await
            .unwrap();
    }));
    let (mut client, _driver) = open(authorized.clone(), &format!(
        "GET http://127.0.0.1:{port}/events?k=v HTTP/1.1\r\nHost: attacker.invalid\r\nProxy-Authorization: secret\r\n\r\n"
    )).await;
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), client.read_to_end(&mut bytes)).await??;
    assert!(
        bytes.starts_with(b"HTTP/1.1 200"),
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    assert!(bytes.contains(&b'7'));
    (&mut backend.0).await?;

    match TcpListener::bind("[::1]:0").await {
        Ok(listener) => {
            let port = listener.local_addr()?.port();
            bind(&provider, &tenant, "ipv6", format!("[::1]:{port}")).await?;
            let (mut client, _driver) = open(
                proxy(&tenant, "ipv6"),
                &format!("CONNECT [::1]:{port} HTTP/1.1\r\nHost: ignored.invalid\r\n\r\n"),
            )
            .await;
            assert!(headers(&mut client).await.starts_with(b"HTTP/1.1 200"));
            client.write_all(b"6").await?;
            fixture_connection(&listener, b'6').await?;
        }
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::AddrNotAvailable | std::io::ErrorKind::Unsupported
            ) =>
        {
            eprintln!("IPv6 loopback unavailable; skipping IPv6 transport case: {error}");
        }
        Err(error) => return Err(error.into()),
    }

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    bind(&provider, &tenant, "stream", format!("127.0.0.1:{port}")).await?;
    let mut backend = AbortDriver(databend_common_base::runtime::spawn(async move {
        let (mut stream, request) = fixture_connection(&listener, b'G').await.unwrap();
        assert!(request.starts_with(b"GET /hold HTTP/1.1\r\n"));
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100000\r\n\r\n7")
            .await
            .unwrap();
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(10), stream.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    }));
    let (mut client, _driver) = open(
        proxy(&tenant, "stream"),
        &format!("GET http://127.0.0.1:{port}/hold HTTP/1.1\r\nHost: ignored.invalid\r\n\r\n"),
    )
    .await;
    assert!(headers(&mut client).await.starts_with(b"HTTP/1.1 200"));
    provider
        .update_external_access_integration(&tenant, "stream", None, Some(false), false)
        .await?;
    let _closed =
        tokio::time::timeout(Duration::from_secs(10), client.read_to_end(&mut Vec::new())).await?;
    (&mut backend.0).await?;

    for change in ["disable", "change_rule", "drop_rule", "drop_integration"] {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        bind(&provider, &tenant, "access", format!("127.0.0.1:{port}")).await?;
        tokio::time::timeout(Duration::from_secs(10), async {
            while provider
                .validate_external_access(&tenant, &format!("https://127.0.0.1:{port}"), &[
                    "access".into(),
                ])
                .await
                .is_err()
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await?;
        let request = format!("CONNECT 127.0.0.1:{port} HTTP/1.1\r\nHost: ignored.invalid\r\n\r\n");
        let (mut client, _driver) = open(authorized.clone(), &request).await;
        assert!(headers(&mut client).await.starts_with(b"HTTP/1.1 200"));
        client.write_all(b"p").await?;
        let (mut upstream, _) = fixture_connection(&listener, b'p').await?;
        match change {
            "disable" => {
                provider
                    .update_external_access_integration(&tenant, "access", None, Some(false), false)
                    .await?
            }
            "change_rule" => {
                provider
                    .update_network_rule(
                        &tenant,
                        NetworkRule {
                            name: "access".into(),
                            value_list: vec!["denied.invalid:443".into()],
                        },
                        false,
                    )
                    .await?
            }
            "drop_rule" => provider.drop_network_rule(&tenant, "access", false).await?,
            "drop_integration" => {
                provider
                    .drop_external_access_integration(&tenant, "access", false)
                    .await?
            }
            _ => unreachable!(),
        }
        let mut byte = [0];
        let read = tokio::time::timeout(Duration::from_secs(10), client.read(&mut byte)).await?;
        assert!(
            matches!(read, Ok(0) | Err(_)),
            "{change}: existing tunnel survived"
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), upstream.read(&mut byte)).await??,
            0
        );
        let (mut denied, _driver) = open(authorized.clone(), &request).await;
        assert!(headers(&mut denied).await.starts_with(b"HTTP/1.1 403"));
    }
    Ok(())
}
