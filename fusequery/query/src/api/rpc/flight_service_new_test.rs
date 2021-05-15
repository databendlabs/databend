// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::api::rpc::flight_service_new::FuseQueryService;
use common_arrow::arrow_flight::flight_service_server::FlightService;
use tonic::{Request, Status};
use common_arrow::arrow_flight::{Empty, ActionType, Action, Ticket, FlightData, FlightDescriptor};
use crate::api::rpc::flight_dispatcher::{Request as DispatcherRequest, PrepareStageInfo};
use common_exception::{Result, ErrorCodes};
use tokio_stream::StreamExt;
use common_planners::{PlanNode, EmptyPlan};
use std::sync::Arc;
use common_arrow::arrow::datatypes::{Schema, Field};
use common_flights::ExecutePlanWithShuffleAction;
use common_arrow::parquet::data_type::AsBytes;
use common_arrow::arrow_flight::flight_descriptor::DescriptorType;
use common_datavalues::DataType;
use common_arrow::arrow::ipc::convert;
use warp::reply::Response;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_service_list_actions() -> Result<()> {
    let (sender, _) = tokio::sync::mpsc::channel(1);
    let service = FuseQueryService::create(sender);
    let response = service.list_actions(Request::new(Empty {})).await;

    assert!(response.is_ok());
    let list_actions = response.unwrap().into_inner().collect::<Vec<_>>().await;
    assert_eq!(list_actions.len(), 1);
    assert_eq!(list_actions[0].as_ref().unwrap().r#type, "PrepareQueryStage".to_string());
    assert_eq!(list_actions[0].as_ref().unwrap().description, "Prepare a query stage that can be sent to the remote after receiving data from remote".to_string());

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
                sss.send(Ok(())).await;

                // Validate prepare stage info
                assert_eq!(info.0, "query_id");
                assert_eq!(info.1, "stage_id");
                assert_eq!(info.2.name(), "EmptyPlan");
                assert_eq!(info.3, vec!["stream_1", "stream_2"]);
            }
            other => panic!("expect PrepareQueryStage")
        }
    });

    let response = service.do_action(Request::new(
        Action {
            r#type: "PrepareQueryStage".to_string(),
            body: "{\"query_id\":\"query_id\",\"stage_id\":\"stage_id\",\"plan\":{\"Empty\":{\"schema\":{\"fields\":[]}}},\"scatters\":[\"stream_1\",\"stream_2\"], \"scatters_plan\":{\"Empty\":{\"schema\":{\"fields\":[]}}}}".as_bytes().to_vec(),
        }
    )).await;

    match response {
        Err(error) => assert!(false, "test_prepare_query_stage error: {:?}", error),
        Ok(_) => join_handler.await.expect("Receive unexpect prepare stage info"),
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
                sender.send(Ok(data_receiver)).await;

                // Validate prepare stage info
                assert_eq!(stream_id, "stream_id");
                // Push some data
                data_sender.send(Err(ErrorCodes::Ok("test_error_code".to_string()))).await;
                data_sender.send(Ok(FlightData {
                    flight_descriptor: None,
                    data_header: vec![1],
                    app_metadata: vec![2],
                    data_body: vec![3],
                })).await;
            }
            other => panic!("expect GetStream")
        }
    });

    let response = service.do_get(
        Request::new(Ticket { ticket: "stream_id".as_bytes().to_vec() })).await;

    // First:check error status
    join_handler.await.expect("Receive unexpect prepare stage info");
    assert!(response.is_ok());

    let mut data_stream = response.unwrap().into_inner();
    assert_eq!(data_stream.next().await.unwrap().unwrap_err().message(), "Code: 0, displayText = test_error_code.");
    assert_eq!(data_stream.next().await.unwrap().unwrap(), FlightData {
        flight_descriptor: None,
        data_header: vec![1],
        app_metadata: vec![2],
        data_body: vec![3],
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
                sender.send(Ok(Arc::new(Schema::new(vec![Field::new("field", DataType::Int8, true)])))).await;

                // Validate prepare stage info
                assert_eq!(stream_id, "test_a/test_b");
            }
            other => panic!("expect GetSchema")
        }
    });

    let response = service.get_schema(Request::new(
        FlightDescriptor {
            r#type: DescriptorType::Path as i32,
            cmd: vec![],
            path: vec!["test_a".to_string(), "test_b".to_string()],
        }
    )).await;

    // First:check error status
    join_handler.await.expect("Receive unexpect prepare stage info");
    assert!(response.is_ok());

    let schema = convert::schema_from_bytes(&response.unwrap().into_inner().schema);
    assert!(schema.is_ok());
    assert_eq!(schema.unwrap(), Schema::new(vec![Field::new("field", DataType::Int8, true)]));

    Ok(())
}
