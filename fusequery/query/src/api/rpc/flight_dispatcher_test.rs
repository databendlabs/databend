// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::{Result, ErrorCodes};
use crate::api::rpc::FlightDispatcher;
use crate::configs::Config;
use crate::clusters::Cluster;
use crate::sessions::Session;
use crate::api::rpc::flight_dispatcher::{Request, PrepareStageInfo};
use tokio::sync::mpsc::{channel, Sender, Receiver};
use common_planners::{PlanBuilder, PlanNode};
use tokio_stream::wrappers::ReceiverStream;
use futures::StreamExt;
use common_arrow::arrow_flight::FlightData;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_prepare_stage_with_one_shard() -> Result<()> {
    let stream_id = "query_id/stage_id/stream_id".to_string();
    let (dispatcher, request_sender) = create_dispatcher();

    let receive_res = get_stream_from_dispatcher(&request_sender, &stream_id).await;
    assert!(receive_res.is_err());
    assert_eq!(format!("{}", receive_res.err().unwrap()), "Code: 27, displayText = Stream query_id/stage_id/stream_id is not found.");

    let ctx = crate::tests::try_create_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());
    let plan = PlanBuilder::from(&PlanNode::ReadSource(test_source.number_read_source_plan_for_test(5)?)).build()?;

    let query_id = "query_id".to_string();
    let stage_id = "stage_id".to_string();
    let streams_name = vec!["stream_id".to_string()];
    let prepare_receiver = prepare_stage(&request_sender, plan, query_id, stage_id, streams_name).await;

    assert!(prepare_receiver.is_ok());

    let receive_res = get_stream_from_dispatcher(&request_sender, &stream_id).await;
    assert!(receive_res.is_ok());

    Ok(())
}

async fn prepare_stage(request_sender: &Sender<Request>, plan: PlanNode, query_id: String, stage_id: String, streams: Vec<String>) -> Result<()> {
    let (prepare_sender, mut prepare_receiver) = channel(1);
    request_sender.send(Request::PrepareStage(PrepareStageInfo::create(
        query_id,
        stage_id,
        plan,
        streams,
    ), prepare_sender)).await;

    prepare_receiver.recv().await.unwrap()
}

fn create_dispatcher() -> (FlightDispatcher, Sender<Request>) {
    let conf = Config::default();
    let sessions = Session::create();
    let cluster = Cluster::create(conf.clone());
    let dispatcher = FlightDispatcher::new(conf, cluster, sessions);
    let sender = dispatcher.run();
    (dispatcher, sender)
}

async fn get_stream_from_dispatcher(request_sender: &Sender<Request>, stream_id: &String) -> Result<Receiver<Result<FlightData>>> {
    let (sender_v, mut receiver) = channel(1);
    request_sender.send(Request::GetStream(stream_id.clone(), sender_v)).await;
    receiver.recv().await.unwrap()
}
