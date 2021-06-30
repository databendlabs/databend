// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_arrow::arrow::datatypes::Schema;
use common_arrow::arrow::ipc::convert;
use common_arrow::arrow_flight::flight_descriptor::DescriptorType;
use common_arrow::arrow_flight::flight_service_server::FlightService;
use common_arrow::arrow_flight::Action;
use common_arrow::arrow_flight::Empty;
use common_arrow::arrow_flight::FlightData;
use common_arrow::arrow_flight::FlightDescriptor;
use common_arrow::arrow_flight::Ticket;
use common_datavalues::DataField;
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;
use common_runtime::tokio;
use tokio_stream::StreamExt;
use tonic::Request;

use crate::api::rpc::flight_dispatcher::Request as DispatcherRequest;
use crate::api::rpc::flight_service_new::FuseQueryService;
use crate::api::rpc::from_status;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_service_list_actions() -> Result<()> {
    let (sender, _) = tokio::sync::mpsc::channel(1);
    let service = FuseQueryService::create(sender);
    let response = service.list_actions(Request::new(Empty {})).await;

    assert!(response.is_ok());
    let list_actions = response.unwrap().into_inner().collect::<Vec<_>>().await;
    assert_eq!(list_actions.len(), 1);
    assert_eq!(
        list_actions[0].as_ref().unwrap().r#type,
        "PrepareQueryStage".to_string()
    );
    assert_eq!(
        list_actions[0].as_ref().unwrap().description,
        "Prepare a query stage that can be sent to the remote after receiving data from remote"
            .to_string()
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_prepare_query_stage() -> Result<()> {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let service = FuseQueryService::create(sender);
    let join_handler = tokio::spawn(async move {
        match receiver.recv().await.unwrap() {
            DispatcherRequest::PrepareQueryStage(info, sss) => {
                // To avoid deadlock, we first return the result
                let send_result = sss.send(Ok(())).await;
                if let Err(error) = send_result {
                    assert!(false, "Cannot push in test_prepare_query_stage: {}", error);
                }

                // Validate prepare stage info
                assert_eq!(info.query_id, "query_id");
                assert_eq!(info.stage_id, "stage_id");
                assert_eq!(info.plan.name(), "EmptyPlan");
                assert_eq!(info.scatters, vec!["stream_1", "stream_2"]);
            }
            _ => panic!("expect PrepareQueryStage"),
        }
    });

    let response = service.do_action(Request::new(
        Action {
            r#type: "PrepareQueryStage".to_string(),
            body: "{\"query_id\":\"query_id\",\"stage_id\":\"stage_id\",\"plan\":{\"Empty\":{\"schema\":{\"fields\":[]}}},\"scatters\":[\"stream_1\",\"stream_2\"], \"scatters_action\":{\"Literal\":{\"UInt64\":1}},\"subquery_res_map\":{}}".as_bytes().to_vec(),
        }
    )).await;

    match response {
        Err(error) => assert!(false, "test_prepare_query_stage error: {:?}", error),
        Ok(_) => join_handler
            .await
            .expect("Receive unexpect prepare stage info"),
    };

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_do_get_stream() -> Result<()> {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let service = FuseQueryService::create(sender);
    let join_handler = tokio::spawn(async move {
        match receiver.recv().await.unwrap() {
            DispatcherRequest::GetStream(stream_id, sender) => {
                // To avoid deadlock, we first return the result
                let (data_sender, data_receiver) = tokio::sync::mpsc::channel(3);
                let send_result = sender.send(Ok(data_receiver)).await;

                if let Err(error) = send_result {
                    assert!(false, "Cannot push in test_do_get_stream: {}", error);
                }

                // Validate prepare stage info
                assert_eq!(stream_id, "stream_id");
                // Push some data
                let send_result = data_sender
                    .send(Err(ErrorCode::Ok("test_error_code".to_string())))
                    .await;

                if let Err(error) = send_result {
                    assert!(false, "Cannot push in test_do_get_stream: {}", error);
                }

                let send_result = data_sender
                    .send(Ok(FlightData {
                        flight_descriptor: None,
                        data_header: vec![1],
                        app_metadata: vec![2],
                        data_body: vec![3],
                    }))
                    .await;

                if let Err(error) = send_result {
                    assert!(false, "Cannot push in test_do_get_stream: {}", error);
                }
            }
            _ => panic!("expect GetStream"),
        }
    });

    let response = service
        .do_get(Request::new(Ticket {
            ticket: "stream_id".as_bytes().to_vec(),
        }))
        .await;

    // First:check error status
    join_handler
        .await
        .expect("Receive unexpect prepare stage info");
    assert!(response.is_ok());

    let mut data_stream = response.unwrap().into_inner();
    let error_codes = from_status(data_stream.next().await.unwrap().unwrap_err());
    assert_eq!(error_codes.code(), 0);
    assert_eq!(error_codes.message(), "test_error_code");

    assert_eq!(data_stream.next().await.unwrap().unwrap(), FlightData {
        flight_descriptor: None,
        data_header: vec![1],
        app_metadata: vec![2],
        data_body: vec![3]
    });
    assert!(data_stream.next().await.is_none());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_do_get_schema() -> Result<()> {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let service = FuseQueryService::create(sender);
    let join_handler = tokio::spawn(async move {
        match receiver.recv().await.unwrap() {
            DispatcherRequest::GetSchema(stream_id, sender) => {
                // To avoid deadlock, we first return the result
                let send_result = sender
                    .send(Ok(Arc::new(Schema::new(vec![DataField::new(
                        "field",
                        DataType::Int8,
                        true,
                    )]))))
                    .await;

                // Validate prepare stage info
                match send_result {
                    Ok(_) => assert_eq!(stream_id, "test_a/test_b"),
                    Err(error) => assert!(false, "{}", error),
                };
            }
            _ => panic!("expect GetSchema"),
        }
    });

    let response = service
        .get_schema(Request::new(FlightDescriptor {
            r#type: DescriptorType::Path as i32,
            cmd: vec![],
            path: vec!["test_a".to_string(), "test_b".to_string()],
        }))
        .await;

    // First:check error status
    join_handler
        .await
        .expect("Receive unexpect prepare stage info");
    assert!(response.is_ok());

    let schema = convert::schema_from_bytes(&response.unwrap().into_inner().schema);
    assert!(schema.is_ok());
    assert_eq!(
        schema.unwrap(),
        Schema::new(vec![DataField::new("field", DataType::Int8, true)])
    );

    Ok(())
}
