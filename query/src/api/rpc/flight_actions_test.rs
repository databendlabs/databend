// Copyright 2020 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::convert::TryInto;

use common_arrow::arrow_format::flight::data::Action;
use common_base::tokio;
use common_datavalues::DataValue;
use common_exception::Result;
use common_planners::Expression;

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
        FlightAction::CancelAction(_) => panic!(),
        FlightAction::BroadcastAction(_) => panic!(),
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
