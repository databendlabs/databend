// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::net::SocketAddr;

use common_exception::ErrorCode;
use common_exception::Result;
use common_runtime::tokio;
use metrics::counter;
use warp::http::Uri;
use warp::hyper::Client;

use crate::metrics::MetricService;

pub static METRIC_TEST: &str = "metrics.test";

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_metrics_server() -> Result<()> {
    let mut service = MetricService::create();
    let listening = "0.0.0.0:0".parse::<SocketAddr>()?;
    let listening = service.start(listening).await?;

    assert_eq!(do_get(listening).await?.find("metrics_test 1"), None);
    counter!(METRIC_TEST, 1);
    assert!(do_get(listening).await?.find("metrics_test 1").is_some());
    Ok(())
}

async fn do_get(address: SocketAddr) -> Result<String> {
    let uri = match format!("http://{}", address).parse::<Uri>() {
        Ok(uri) => uri,
        Err(error) => {
            return Err(ErrorCode::LogicalError(format!(
                "Cannot parse uri {}",
                error
            )))
        }
    };

    let client = Client::new();
    match client.get(uri).await {
        Err(error) => Err(ErrorCode::LogicalError(format!(
            "Cannot request uri {}",
            error
        ))),
        Ok(mut response) => match warp::hyper::body::to_bytes(response.body_mut()).await {
            Err(error) => Err(ErrorCode::LogicalError(format!(
                "Cannot parse response body {}",
                error
            ))),
            Ok(body) => match std::str::from_utf8(body.as_ref()) {
                Ok(str) => Ok(str.to_string()),
                Err(error) => Err(ErrorCode::LogicalError(format!(
                    "Cannot from utf8 {}",
                    error
                ))),
            },
        },
    }
}
