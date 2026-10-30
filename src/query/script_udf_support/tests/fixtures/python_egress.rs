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

mod tests {
    use databend_common_base::base::GlobalInstance;
    use databend_common_config::InnerConfig;
    use databend_common_meta_app::principal::ExternalAccessIntegration;
    use databend_common_meta_app::principal::NetworkRule;
    use databend_common_meta_app::schema::CreateOption;
    use databend_common_version::BUILD_INFO;
    use databend_meta_client::RpcClientConf;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    use super::*;

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

    // The development environment sends unsolicited Go HTTP discovery probes
    // to listening TCP ports, including non-default loopback addresses. Ignore
    // only that exact request; do not let it satisfy a proxy connection assertion.
    async fn non_probe_prefix(stream: &mut TcpStream) -> anyhow::Result<Option<Vec<u8>>> {
        let mut byte = [0];
        tokio::time::timeout(Duration::from_secs(2), stream.read_exact(&mut byte)).await??;
        let mut prefix = byte.to_vec();
        if byte == *b"G" {
            prefix.extend(tokio::time::timeout(Duration::from_secs(2), headers(stream)).await?);
        }
        let local = stream.local_addr()?;
        for authority in [local.to_string(), format!("localhost:{}", local.port())] {
            let probe = format!(
                "GET / HTTP/1.1\r\nHost: {authority}\r\nUser-Agent: Go-http-client/1.1\r\nConnection: close\r\nAccept-Encoding: gzip\r\n\r\n"
            );
            if prefix == probe.as_bytes() {
                stream
                    .write_all(b"HTTP/1.0 404 Not Found\r\nContent-Length: 0\r\n\r\n")
                    .await?;
                return Ok(None);
            }
        }
        Ok(Some(prefix))
    }

    async fn fixture_connection(
        listener: &TcpListener,
        marker: u8,
    ) -> anyhow::Result<(TcpStream, Vec<u8>)> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let (mut stream, _) = listener.accept().await?;
                if let Some(prefix) = non_probe_prefix(&mut stream).await? {
                    assert_eq!(
                        prefix[0],
                        marker,
                        "unexpected connection: {}",
                        String::from_utf8_lossy(&prefix)
                    );
                    return anyhow::Ok((stream, prefix));
                }
            }
        })
        .await?
    }

    async fn open(proxy: Arc<PythonEgressProxy>, request: &str) -> (UnixStream, AbortDriver) {
        let (mut client, server) = UnixStream::pair().unwrap();
        let driver = AbortDriver(databend_common_base::runtime::spawn(async move {
            let _ = proxy.serve_stream(server).await;
        }));
        client.write_all(request.as_bytes()).await.unwrap();
        (client, driver)
    }

    #[test]
    fn target_uses_proxy_uri_not_host_header() {
        for (method, uri) in [
            (Method::GET, "http://Example.COM:8080/events?k=v"),
            (Method::CONNECT, "example.com:443"),
            (Method::CONNECT, "[::1]:8443"),
        ] {
            let request = Request::builder()
                .method(method)
                .uri(uri)
                .header("Host", "attacker.invalid")
                .body(())
                .unwrap();
            assert!(Target::parse(&request).is_ok());
        }
        for (method, uri) in [
            (Method::GET, "/relative"),
            (Method::GET, "https://example.com/"),
            (Method::GET, "http://user:password@example.com/"),
            (Method::GET, "http://example.com:0/"),
            (Method::CONNECT, "example.com"),
            (Method::CONNECT, "example.com:0"),
            (Method::CONNECT, "https://example.com"),
        ] {
            let request = Request::builder().method(method).uri(uri).body(()).unwrap();
            assert!(Target::parse(&request).is_err(), "{uri}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_forward_and_live_connection_revocation() -> anyhow::Result<()> {
        assert!(PythonEgressProxy::for_bindings("", &[])?.is_none());
        assert!(PythonEgressProxy::for_bindings("", &["access".into()]).is_err());
        let name = std::thread::current().name().unwrap().to_owned();
        GlobalInstance::init_testing(&name);
        let mut config = InnerConfig::default();
        config.query.common.enable_udf_server = true;
        GlobalConfig::init(&config, &BUILD_INFO)?;
        let tenant = Tenant::new_literal("python_egress_tests");
        let provider = UserApiProvider::try_create_simple(RpcClientConf::empty(), &tenant).await?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        provider
            .add_network_rule(
                &tenant,
                NetworkRule {
                    name: "rule".into(),
                    value_list: vec![format!("127.0.0.1:{port}")],
                },
                &CreateOption::Create,
            )
            .await?;
        provider
            .add_external_access_integration(
                &tenant,
                ExternalAccessIntegration {
                    name: "access".into(),
                    allowed_network_rules: vec!["rule".into()],
                    enabled: true,
                },
                &CreateOption::Create,
            )
            .await?;
        let proxy = Arc::new(PythonEgressProxy {
            tenant: tenant.clone(),
            integrations: vec!["access".into()],
            provider: provider.clone(),
            ceiling: vec![],
        });
        for (tenant, ceiling) in [
            (Tenant::new_literal("foreign_tenant"), vec![]),
            (tenant.clone(), vec!["https://denied.invalid".to_owned()]),
        ] {
            let blocked = Arc::new(PythonEgressProxy {
                tenant,
                ceiling,
                provider: provider.clone(),
                integrations: vec!["access".into()],
            });
            let (mut client, _driver) = open(
                blocked,
                &format!("CONNECT 127.0.0.1:{port} HTTP/1.1\r\nHost: attacker.invalid\r\n\r\n"),
            )
            .await;
            assert!(headers(&mut client).await.starts_with(b"HTTP/1.1 403"));
            let deadline = tokio::time::Instant::now() + Duration::from_millis(50);
            while let Ok(connection) = tokio::time::timeout_at(deadline, listener.accept()).await {
                let (mut unexpected, _) = connection?;
                // A connection with no payload also fails: authorization must
                // precede TCP connect, not just application data forwarding.
                assert!(
                    non_probe_prefix(&mut unexpected).await?.is_none(),
                    "denied destination was connected"
                );
            }
        }
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
        let (mut client, _driver) = open(proxy.clone(), &format!("GET http://127.0.0.1:{port}/events?k=v HTTP/1.1\r\nHost: attacker.invalid\r\nProxy-Authorization: secret\r\n\r\n")).await;
        let mut bytes = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), client.read_to_end(&mut bytes)).await??;
        assert!(
            bytes.starts_with(b"HTTP/1.1 200"),
            "{}",
            String::from_utf8_lossy(&bytes)
        );
        assert!(bytes.contains(&b'7'));
        (&mut backend.0).await?;

        let ipv6_listener = match TcpListener::bind("[::1]:0").await {
            Ok(listener) => Some(listener),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::AddrNotAvailable | std::io::ErrorKind::Unsupported
                ) =>
            {
                eprintln!("IPv6 loopback unavailable; skipping IPv6 transport case: {error}");
                None
            }
            Err(error) => return Err(error.into()),
        };
        if let Some(ipv6_listener) = ipv6_listener {
            let port = ipv6_listener.local_addr()?.port();
            provider
                .add_network_rule(
                    &tenant,
                    NetworkRule {
                        name: "ipv6_rule".into(),
                        value_list: vec![format!("[::1]:{port}")],
                    },
                    &CreateOption::Create,
                )
                .await?;
            provider
                .add_external_access_integration(
                    &tenant,
                    ExternalAccessIntegration {
                        name: "ipv6_access".into(),
                        allowed_network_rules: vec!["ipv6_rule".into()],
                        enabled: true,
                    },
                    &CreateOption::Create,
                )
                .await?;
            let ipv6 = Arc::new(PythonEgressProxy {
                tenant: tenant.clone(),
                provider: provider.clone(),
                integrations: vec!["ipv6_access".into()],
                ceiling: vec![],
            });
            let (mut client, _driver) = open(
                ipv6,
                &format!("CONNECT [::1]:{port} HTTP/1.1\r\nHost: ignored.invalid\r\n\r\n"),
            )
            .await;
            assert!(headers(&mut client).await.starts_with(b"HTTP/1.1 200"));
            client.write_all(b"6").await?;
            let (_upstream, _) = fixture_connection(&ipv6_listener, b'6').await?;
        }

        let stream_listener = TcpListener::bind("127.0.0.1:0").await?;
        let stream_port = stream_listener.local_addr()?.port();
        provider
            .add_network_rule(
                &tenant,
                NetworkRule {
                    name: "stream_rule".into(),
                    value_list: vec![format!("127.0.0.1:{stream_port}")],
                },
                &CreateOption::Create,
            )
            .await?;
        provider
            .add_external_access_integration(
                &tenant,
                ExternalAccessIntegration {
                    name: "stream_access".into(),
                    allowed_network_rules: vec!["stream_rule".into()],
                    enabled: true,
                },
                &CreateOption::Create,
            )
            .await?;
        let streaming = Arc::new(PythonEgressProxy {
            tenant: tenant.clone(),
            provider: provider.clone(),
            integrations: vec!["stream_access".into()],
            ceiling: vec![],
        });
        let mut backend = AbortDriver(databend_common_base::runtime::spawn(async move {
            let (mut stream, request) = fixture_connection(&stream_listener, b'G').await.unwrap();
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
        let (mut client, _stream_driver) = open(
            streaming,
            &format!(
                "GET http://127.0.0.1:{stream_port}/hold HTTP/1.1\r\nHost: ignored.invalid\r\n\r\n"
            ),
        )
        .await;
        assert!(headers(&mut client).await.starts_with(b"HTTP/1.1 200"));
        provider
            .update_external_access_integration(&tenant, "stream_access", None, Some(false), false)
            .await?;
        let mut rest = Vec::new();
        let _closed =
            tokio::time::timeout(Duration::from_secs(10), client.read_to_end(&mut rest)).await?;
        (&mut backend.0).await?;

        // Every revocation path closes an existing, otherwise idle CONNECT
        // stream, not just the next request or the next worker startup.
        for change in ["disable", "change_rule", "drop_rule", "drop_integration"] {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let port = listener.local_addr()?.port();
            provider
                .add_network_rule(
                    &tenant,
                    NetworkRule {
                        name: "rule".into(),
                        value_list: vec![format!("127.0.0.1:{port}")],
                    },
                    &CreateOption::CreateOrReplace,
                )
                .await?;
            provider
                .add_external_access_integration(
                    &tenant,
                    ExternalAccessIntegration {
                        name: "access".into(),
                        allowed_network_rules: vec!["rule".into()],
                        enabled: true,
                    },
                    &CreateOption::CreateOrReplace,
                )
                .await?;
            // Wait for the same watch cache used by remote UDFs (if enabled).
            let target = Target {
                url: Url::parse(&format!("https://127.0.0.1:{port}"))?,
            };
            tokio::time::timeout(Duration::from_secs(10), async {
                while proxy.authorize(&target).await.is_err() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await?;
            let (mut client, _driver) = open(
                proxy.clone(),
                &format!("CONNECT 127.0.0.1:{port} HTTP/1.1\r\nHost: ignored.invalid\r\n\r\n"),
            )
            .await;
            let header = headers(&mut client).await;
            assert!(
                header.starts_with(b"HTTP/1.1 200"),
                "{}",
                String::from_utf8_lossy(&header)
            );
            client.write_all(b"p").await?;
            let (mut upstream, _) = fixture_connection(&listener, b'p').await?;
            let mut byte = [0];
            match change {
                "disable" => {
                    provider
                        .update_external_access_integration(
                            &tenant,
                            "access",
                            None,
                            Some(false),
                            false,
                        )
                        .await?
                }
                "change_rule" => {
                    provider
                        .update_network_rule(
                            &tenant,
                            NetworkRule {
                                name: "rule".into(),
                                value_list: vec!["denied.invalid:443".into()],
                            },
                            false,
                        )
                        .await?
                }
                "drop_rule" => provider.drop_network_rule(&tenant, "rule", false).await?,
                "drop_integration" => {
                    provider
                        .drop_external_access_integration(&tenant, "access", false)
                        .await?
                }
                _ => unreachable!(),
            }
            let read =
                tokio::time::timeout(Duration::from_secs(10), client.read(&mut byte)).await?;
            assert!(
                matches!(read, Ok(0) | Err(_)),
                "{change}: existing tunnel survived"
            );
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), upstream.read(&mut byte)).await??,
                0
            );
            let (mut denied, _driver) = open(
                proxy.clone(),
                &format!("CONNECT 127.0.0.1:{port} HTTP/1.1\r\nHost: ignored.invalid\r\n\r\n"),
            )
            .await;
            assert!(headers(&mut denied).await.starts_with(b"HTTP/1.1 403"));
        }
        Ok(())
    }
}
