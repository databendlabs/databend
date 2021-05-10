// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::api::rpc::flight_service_new::FuseQueryService;
use common_arrow::arrow_flight::flight_service_server::FlightService;
use tonic::{Request, Status};
use common_arrow::arrow_flight::{Empty, ActionType};
use common_exception::Result;
use tokio_stream::StreamExt;

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