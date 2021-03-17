// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_executor_service() -> Result<(), Box<dyn std::error::Error>> {
    use pretty_assertions::assert_eq;

    use crate::protobuf::executor_client::ExecutorClient;
    use crate::protobuf::{PingRequest, PingResponse};

    // Test service starts.
    let addr = crate::tests::try_start_service(1).await?[0].clone();

    let mut client = ExecutorClient::connect(format!("http://{}", addr)).await?;
    let request = tonic::Request::new(PingRequest {
        name: String::from("datafuse"),
    });
    let actual = client.ping(request).await?.into_inner();
    let expect = PingResponse {
        message: "Hello datafuse!".to_string(),
    };
    assert_eq!(actual, expect);

    Ok(())
}
