// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryInto;

use common_arrow::arrow_flight::Action;
use common_datavalues::DataValue;
use common_exception::Result;
use common_planners::Expression;
use common_runtime::tokio;

use crate::api::rpc::flight_actions::FlightAction;
use crate::api::ShuffleAction;
use crate::tests::parse_query;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_shuffle_action_try_into() -> Result<()> {
    let shuffle_action = ShuffleAction {
        query_id: String::from("query_id"),
        stage_id: String::from("stage_id"),
        plan: parse_query("SELECT number FROM numbers(5)")?,
        sinks: vec![String::from("stream_id")],
        scatters_expression: Expression::create_literal(DataValue::UInt64(Some(1))),
    };

    let from_action = FlightAction::PrepareShuffleAction(shuffle_action);
    let to_action: Action = from_action.try_into()?;
    let from_action: FlightAction = to_action.try_into()?;
    match from_action {
        FlightAction::BroadcastAction(_) => assert!(false),
        FlightAction::PrepareShuffleAction(action) => {
            assert_eq!(action.query_id, "query_id");
            assert_eq!(action.stage_id, "stage_id");
            assert_eq!(action.plan, parse_query("SELECT number FROM numbers(5)")?);
            assert_eq!(action.sinks, vec![String::from("stream_id")]);
            assert_eq!(
                action.scatters_expression,
                Expression::create_literal(DataValue::UInt64(Some(1)))
            );
        }
    }

    Ok(())
}
