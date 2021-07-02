// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryInto;
use std::sync::Arc;

use common_arrow::arrow_flight::flight_service_server::FlightService;
use common_datavalues::DataValue;
use common_exception::Result;
use common_planners::Expression;
use common_runtime::tokio;
use tonic::Request;

use crate::api::rpc::flight_actions::FlightAction;
use crate::api::rpc::flight_tickets::StreamTicket;
use crate::api::rpc::FuseQueryFlightDispatcher;
use crate::api::rpc::FuseQueryFlightService;
use crate::api::FlightTicket;
use crate::api::ShuffleAction;
use crate::tests::parse_query;
use crate::tests::try_create_sessions;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_do_shuffle_action() -> Result<()> {
    let sessions = try_create_sessions()?;
    let dispatcher = Arc::new(FuseQueryFlightDispatcher::create());
    let service = FuseQueryFlightService::create(dispatcher, sessions);

    let shuffle_action = FlightAction::PrepareShuffleAction(ShuffleAction {
        query_id: String::from("query_id"),
        stage_id: String::from("stage_id"),
        plan: parse_query("SELECT number FROM numbers(5)")?,
        sinks: vec![String::from("stream_id")],
        scatters_expression: Expression::Literal(DataValue::UInt64(Some(1))),
    });

    service
        .do_action(Request::new(shuffle_action.try_into()?))
        .await?;

    let stream_ticket = FlightTicket::StreamTicket(StreamTicket {
        query_id: String::from("query_id"),
        stage_id: String::from("stage_id"),
        stream: String::from("stream_id"),
    });

    service
        .do_get(Request::new(stream_ticket.try_into()?))
        .await?;

    Ok(())
}
