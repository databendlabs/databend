// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;

use common_datablocks::assert_blocks_eq;
use common_datavalues::DataValue;
use common_exception::Result;
use common_planners::Expression;
use common_planners::PlanBuilder;
use common_planners::PlanNode;
use common_runtime::tokio;
use common_runtime::tokio::sync::mpsc::channel;
use common_runtime::tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;

use crate::api::rpc::flight_data_stream::FlightDataStream;
use crate::api::rpc::flight_dispatcher::PrepareStageInfo;
use crate::api::rpc::flight_dispatcher::Request;
use crate::api::rpc::FlightDispatcher;
use crate::configs::Config;
use crate::sessions::SessionMgr;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_get_stream_with_non_exists_stream() -> Result<()> {
    let stream_id = "query_id/stage_id/stream_id".to_string();
    let (_dispatcher, request_sender) = create_dispatcher()?;

    let (sender_v, mut receiver) = channel(1);
    let send_result = request_sender
        .send(Request::GetStream(stream_id.clone(), sender_v))
        .await;

    if let Err(error) = send_result {
        assert!(
            false,
            "Cannot push in test_get_stream_with_non_exists_stream: {}",
            error
        );
    }

    match receiver.recv().await.unwrap() {
        Ok(_) => assert!(
            false,
            "Return Ok in test_get_stream_with_non_exists_stream."
        ),
        Err(error) => {
            assert_eq!(error.code(), 29);
            assert_eq!(
                error.message(),
                "Stream query_id/stage_id/stream_id is not found"
            );
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
            let source_plan = test_source.number_read_source_plan_for_test(5)?;
            let plan = PlanBuilder::from(&PlanNode::ReadSource(source_plan)).build()?;
            Result::Ok((
                plan.schema().clone(),
                Request::PrepareQueryStage(
                    PrepareStageInfo::create(
                        query_id.clone(),
                        stage_id.clone(),
                        plan.clone(),
                        vec![stream_id.clone()],
                        Expression::Literal(DataValue::UInt64(Some(1))),
                        HashMap::<String, bool>::new(),
                    ),
                    sender,
                ),
            ))
        };

        let (_dispatcher, request_sender) = create_dispatcher()?;

        let (prepare_stage_sender, mut prepare_stage_receiver) = channel(1);

        let (schema, prepare_query_stage) = create_prepare_query_stage(prepare_stage_sender)?;
        let send_result = request_sender.send(prepare_query_stage).await;

        if let Err(error) = send_result {
            assert!(
                false,
                "Cannot push in test_prepare_stage_with_scatter: {}",
                error
            );
        }

        prepare_stage_receiver.recv().await.transpose()?;

        // GetStream and collect items
        let (sender_v, mut receiver) = channel(1);
        let send_result = request_sender
            .send(Request::GetStream(stream_full_id.clone(), sender_v))
            .await;

        if let Err(error) = send_result {
            assert!(
                false,
                "Cannot push in test_prepare_stage_with_scatter: {}",
                error
            );
        }

        match receiver.recv().await.unwrap() {
            Err(error) => assert!(false, "{}", error),
            Ok(data_receiver) => {
                let blocks = FlightDataStream::from_receiver(schema, data_receiver)
                    .collect::<Result<Vec<_>>>()
                    .await;

                let expect = vec![
                    "+--------+",
                    "| number |",
                    "+--------+",
                    "| 0      |",
                    "| 1      |",
                    "| 2      |",
                    "| 3      |",
                    "| 4      |",
                    "+--------+",
                ];

                assert_blocks_eq(expect, &blocks?)
            }
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_prepare_stage_with_scatter() -> Result<()> {
    if let (Some(query_id), Some(stage_id), None) = generate_uuids(2) {
        let stream_prefix = format!("{}/{}/", query_id, stage_id);
        let create_prepare_query_stage = |sender: Sender<Result<()>>| {
            let ctx = crate::tests::try_create_context()?;
            let test_source = crate::tests::NumberTestData::create(ctx.clone());
            let source_plan = test_source.number_read_source_plan_for_test(5)?;
            let plan = PlanBuilder::from(&PlanNode::ReadSource(source_plan)).build()?;

            Result::Ok((
                plan.schema().clone(),
                Request::PrepareQueryStage(
                    PrepareStageInfo::create(
                        query_id.clone(),
                        stage_id.clone(),
                        plan.clone(),
                        vec!["stream_1".to_string(), "stream_2".to_string()],
                        Expression::Column("number".to_string()),
                        HashMap::<String, bool>::new(),
                    ),
                    sender,
                ),
            ))
        };

        let (_dispatcher, request_sender) = create_dispatcher()?;
        let (prepare_stage_sender, mut prepare_stage_receiver) = channel(1);

        let (schema, prepare_query_stage) = create_prepare_query_stage(prepare_stage_sender)?;
        let send_result = request_sender.send(prepare_query_stage).await;

        if let Err(error) = send_result {
            assert!(
                false,
                "Cannot push in test_prepare_stage_with_scatter: {}",
                error
            );
        }

        prepare_stage_receiver.recv().await.transpose()?;

        // GetStream and collect items
        let (sender_v, mut receiver) = channel(1);

        let stream_name = stream_prefix.clone() + "stream_1";
        let send_result = request_sender
            .send(Request::GetStream(stream_name, sender_v.clone()))
            .await;

        if let Err(error) = send_result {
            assert!(
                false,
                "Cannot push in test_prepare_stage_with_scatter: {}",
                error
            );
        }

        match receiver.recv().await.unwrap() {
            Err(error) => assert!(false, "{}", error),
            Ok(data_receiver) => {
                let mut stream = FlightDataStream::from_receiver(schema.clone(), data_receiver);

                let expect = vec![
                    "+--------+",
                    "| number |",
                    "+--------+",
                    "| 0      |",
                    "| 2      |",
                    "| 4      |",
                    "+--------+",
                ];

                assert_blocks_eq(expect, &[(stream.next().await.unwrap()?)])
            }
        }

        let stream_name = stream_prefix.clone() + "stream_2";
        let send_result = request_sender
            .send(Request::GetStream(stream_name, sender_v.clone()))
            .await;

        if let Err(error) = send_result {
            assert!(
                false,
                "Cannot push in test_prepare_stage_with_scatter: {}",
                error
            );
        }

        match receiver.recv().await.unwrap() {
            Err(error) => assert!(false, "{}", error),
            Ok(data_receiver) => {
                let mut stream = FlightDataStream::from_receiver(schema.clone(), data_receiver);

                let expect = vec![
                    "+--------+",
                    "| number |",
                    "+--------+",
                    "| 1      |",
                    "| 3      |",
                    "+--------+",
                ];

                assert_blocks_eq(expect, &[stream.next().await.unwrap()?])
            }
        }
    }

    Ok(())
}

fn create_dispatcher() -> Result<(FlightDispatcher, Sender<Request>)> {
    let conf = Config::default();
    let sessions = SessionMgr::from_conf(conf.clone())?;
    let dispatcher = FlightDispatcher::new(conf, sessions);
    let sender = dispatcher.run();
    Ok((dispatcher, sender))
}

fn generate_uuids(size: usize) -> (Option<String>, Option<String>, Option<String>) {
    match size {
        1 => (Some(uuid::Uuid::new_v4().to_string()), None, None),
        2 => (
            Some(uuid::Uuid::new_v4().to_string()),
            Some(uuid::Uuid::new_v4().to_string()),
            None,
        ),
        3 => (
            Some(uuid::Uuid::new_v4().to_string()),
            Some(uuid::Uuid::new_v4().to_string()),
            Some(uuid::Uuid::new_v4().to_string()),
        ),
        _ => panic!("Logic error for generate_uuids."),
    }
}
