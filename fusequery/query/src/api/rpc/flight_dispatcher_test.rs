// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datablocks::assert_blocks_eq;
use common_datavalues::DataValue;
use common_exception::Result;
use common_planners::Expression;
use common_runtime::tokio;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use crate::api::rpc::flight_tickets::StreamTicket;
use crate::api::rpc::FuseQueryFlightDispatcher;
use crate::api::FlightAction;
use crate::api::ShuffleAction;
use crate::tests::parse_query;
use crate::tests::try_create_sessions;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_get_stream_with_non_exists_stream() -> Result<()> {
    let dispatcher = FuseQueryFlightDispatcher::create();

    let stream = stream_ticket("query_id", "stage_id", "stream_id");
    let get_stream = dispatcher.get_stream(&stream);

    match get_stream {
        Ok(_) => assert!(
            false,
            "Return Ok in test_get_stream_with_non_exists_stream."
        ),
        Err(error) => {
            assert_eq!(error.code(), 29);
            assert_eq!(error.message(), "Stream is not found");
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_run_shuffle_action_with_no_scatters() -> Result<()> {
    if let (Some(query_id), Some(stage_id), Some(stream_id)) = generate_uuids(3) {
        let flight_dispatcher = FuseQueryFlightDispatcher::create();

        let sessions = try_create_sessions()?;
        let rpc_session = sessions.create_rpc_session(query_id.clone(), false)?;

        flight_dispatcher.shuffle_action(
            rpc_session,
            FlightAction::PrepareShuffleAction(ShuffleAction {
                query_id: query_id.clone(),
                stage_id: stage_id.clone(),
                plan: parse_query("SELECT number FROM numbers(5)")?,
                sinks: vec![stream_id.clone()],
                scatters_expression: Expression::create_literal(DataValue::UInt64(Some(1))),
            }),
        )?;

        let stream = stream_ticket(&query_id, &stage_id, &stream_id);
        let receiver = flight_dispatcher.get_stream(&stream)?;
        let receiver_stream = ReceiverStream::new(receiver);
        let collect_data_blocks = receiver_stream.collect::<Result<Vec<_>>>();

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

        assert_blocks_eq(expect, &collect_data_blocks.await?);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_run_shuffle_action_with_scatter() -> Result<()> {
    if let (Some(query_id), Some(stage_id), None) = generate_uuids(2) {
        let flight_dispatcher = FuseQueryFlightDispatcher::create();

        let sessions = try_create_sessions()?;
        let rpc_session = sessions.create_rpc_session(query_id.clone(), false)?;

        flight_dispatcher.shuffle_action(
            rpc_session,
            FlightAction::PrepareShuffleAction(ShuffleAction {
                query_id: query_id.clone(),
                stage_id: stage_id.clone(),
                plan: parse_query("SELECT number FROM numbers(5)")?,
                sinks: vec!["stream_1".to_string(), "stream_2".to_string()],
                scatters_expression: Expression::Column("number".to_string()),
            }),
        )?;

        let stream_1 = stream_ticket(&query_id, &stage_id, "stream_1");
        let receiver = flight_dispatcher.get_stream(&stream_1)?;
        let receiver_stream = ReceiverStream::new(receiver);
        let collect_data_blocks = receiver_stream.collect::<Result<Vec<_>>>();

        let expect = vec![
            "+--------+",
            "| number |",
            "+--------+",
            "| 0      |",
            "| 2      |",
            "| 4      |",
            "+--------+",
        ];

        assert_blocks_eq(expect, &collect_data_blocks.await?);

        let stream_2 = stream_ticket(&query_id, &stage_id, "stream_2");
        let receiver = flight_dispatcher.get_stream(&stream_2)?;
        let receiver_stream = ReceiverStream::new(receiver);
        let collect_data_blocks = receiver_stream.collect::<Result<Vec<_>>>();

        let expect = vec![
            "+--------+",
            "| number |",
            "+--------+",
            "| 1      |",
            "| 3      |",
            "+--------+",
        ];

        assert_blocks_eq(expect, &collect_data_blocks.await?);
    }

    Ok(())
}

fn stream_ticket(query_id: &str, stage_id: &str, stream: &str) -> StreamTicket {
    StreamTicket {
        query_id: query_id.to_string(),
        stage_id: stage_id.to_string(),
        stream: stream.to_string(),
    }
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
