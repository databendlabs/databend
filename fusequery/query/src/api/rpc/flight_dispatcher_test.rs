// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryInto;

use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

use common_arrow::arrow_flight::FlightData;
use common_arrow::arrow_flight::utils::flight_data_to_arrow_batch;
use common_datablocks::{DataBlock, assert_blocks_eq};
use common_exception::{ErrorCodes, Result};
use common_planners::{PlanBuilder, PlanNode};

use crate::api::rpc::flight_dispatcher::{PrepareStageInfo, Request};
use crate::api::rpc::FlightDispatcher;
use crate::clusters::Cluster;
use crate::configs::Config;
use crate::sessions::Session;
use crate::api::rpc::flight_data_stream::FlightDataStream;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_get_stream_with_non_exists_stream() -> Result<()> {
    let stream_id = "query_id/stage_id/stream_id".to_string();
    let (dispatcher, request_sender) = create_dispatcher();

    let (sender_v, mut receiver) = channel(1);
    request_sender.send(Request::GetStream(stream_id.clone(), sender_v)).await;
    match receiver.recv().await.unwrap() {
        Ok(_) => assert!(false, "Return Ok in test_get_stream_with_non_exists_stream."),
        Err(error) => {
            assert_eq!(error.code(), 28);
            assert_eq!(error.message(), "Stream query_id/stage_id/stream_id is not found");
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_prepare_stage_with_no_scatter() -> Result<()> {
    if let (Some(query_id), Some(stage_id), Some(stream_id)) = generate_uuids(3) {
        let stream_full_id = format!("{}/{}/{}", query_id, stage_id, stream_id);
        let create_prepare_query_stage = |sender: Sender<Result<()>>| {
            let ctx = crate::tests::try_create_context()?;
            let test_source = crate::tests::NumberTestData::create(ctx.clone());
            let read_source_plan = test_source.number_read_source_plan_for_test(5)?;
            let plan = PlanBuilder::from(&PlanNode::ReadSource(read_source_plan)).build()?;
            Ok((plan.schema().clone(), Request::PrepareQueryStage(
                PrepareStageInfo::create(
                    query_id.clone(),
                    stage_id.clone(),
                    plan,
                    vec![stream_id.clone()],
                ), sender,
            )))
        };

        let (dispatcher, request_sender) = create_dispatcher();

        let (prepare_stage_sender, mut prepare_stage_receiver) = channel(1);

        let (schema, prepare_query_stage) = create_prepare_query_stage(prepare_stage_sender)?;
        request_sender.send(prepare_query_stage).await;
        prepare_stage_receiver.recv().await.transpose()?;

        // GetStream and collect items
        let (sender_v, mut receiver) = channel(1);
        request_sender.send(Request::GetStream(stream_full_id.clone(), sender_v)).await;
        match receiver.recv().await.unwrap() {
            Err(error) => assert!(false, "{}", error),
            Ok(data_receiver) => {
                let blocks = FlightDataStream::from_receiver(schema, data_receiver)
                    .collect::<Result<Vec<_>>>().await;

                let expect = vec![
                    "+--------+",
                    "| number |",
                    "+--------+",
                    "| 0      |",
                    "| 1      |",
                    "| 2      |",
                    "| 3      |",
                    "| 4      |",
                    "+--------+"
                ];

                assert_blocks_eq(expect, &blocks?)
            },
        }
    }

    Ok(())
}

fn create_dispatcher() -> (FlightDispatcher, Sender<Request>) {
    let conf = Config::default();
    let sessions = Session::create();
    let cluster = Cluster::create(conf.clone());
    let dispatcher = FlightDispatcher::new(conf, cluster, sessions);
    let sender = dispatcher.run();
    (dispatcher, sender)
}

fn generate_uuids(size: usize) -> (Option<String>, Option<String>, Option<String>) {
    match size {
        1 => (Some(uuid::Uuid::new_v4().to_string()), None, None),
        2 => (Some(uuid::Uuid::new_v4().to_string()), Some(uuid::Uuid::new_v4().to_string()), None),
        3 => (Some(uuid::Uuid::new_v4().to_string()), Some(uuid::Uuid::new_v4().to_string()), Some(uuid::Uuid::new_v4().to_string())),
        _ => panic!("Logic error for generate_uuids.")
    }
}
