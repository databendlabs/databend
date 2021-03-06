// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_executor_service() -> Result<(), Box<dyn std::error::Error>> {
    use tonic::transport::Server;

    use crate::protobuf::executor_client::ExecutorClient;
    use crate::protobuf::{PingRequest, PingResponse};
    use crate::rpcs::rpc::ExecutorRPCService;

    let addr = "127.0.0.1:50051";
    let socket = addr.parse::<std::net::SocketAddr>()?;

    tokio::spawn(async move {
        Server::builder()
            .add_service(ExecutorRPCService::make_server())
            .serve(socket)
            .await
            .unwrap()
    });
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

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
