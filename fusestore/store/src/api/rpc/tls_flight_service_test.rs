// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_flights::MetaApi;
use common_flights::RpcClientTlsConfig;
use common_flights::StoreClient;
use common_runtime::tokio;
use pretty_assertions::assert_eq;

use crate::tests::service::new_test_context;
use crate::tests::start_store_server_with_context;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_tls() -> anyhow::Result<()> {
    common_tracing::init_default_tracing();

    let mut tc = new_test_context();

    tc.config.rpc_tls_server_key = "../../tests/data/certs/server.key".to_owned();
    tc.config.rpc_tls_server_cert = "../../tests/data/certs/server.pem".to_owned();

    let r = start_store_server_with_context(&mut tc).await;
    assert!(r.is_ok());

    let addr = tc.config.flight_api_address.clone();

    let tls_conf = RpcClientTlsConfig {
        rpc_tls_server_root_ca_cert: "../../tests/data/certs/ca.pem".to_string(),
        domain_name: "localhost".to_string(),
    };

    let mut client =
        StoreClient::with_tls_conf(addr.as_str(), "root", "xxx", Some(tls_conf)).await?;

    let r = client
        .get_table("do not care".to_owned(), "do not care".to_owned())
        .await;
    assert!(r.is_err());
    let e = r.unwrap_err();
    assert_eq!(e.code(), ErrorCode::UnknownDatabase("").code());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_tls_server_config_failure() -> anyhow::Result<()> {
    common_tracing::init_default_tracing();

    let mut tc = new_test_context();

    tc.config.rpc_tls_server_key = "../../tests/data/certs/not_exist.key".to_owned();
    tc.config.rpc_tls_server_cert = "../../tests/data/certs/not_exist.pem".to_owned();

    let r = start_store_server_with_context(&mut tc).await;
    assert!(r.is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_tls_client_config_failure() -> anyhow::Result<()> {
    common_tracing::init_default_tracing();

    let tls_conf = RpcClientTlsConfig {
        rpc_tls_server_root_ca_cert: "../../tests/data/certs/not_exist.pem".to_string(),
        domain_name: "localhost".to_string(),
    };

    let r = StoreClient::with_tls_conf("addr", "root", "xxx", Some(tls_conf)).await;

    assert!(r.is_err());
    if let Err(e) = r {
        assert_eq!(e.code(), ErrorCode::TLSConfigurationFailuer("").code());
    }
    Ok(())
}
