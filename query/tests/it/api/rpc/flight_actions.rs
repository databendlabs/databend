// Copyright 2021 Datafuse Labs.
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
use databend_query::api::FlightAction;
use databend_query::api::ShuffleAction;
use databend_query::sql::PlanParser;

use crate::tests::create_query_context;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_shuffle_action_try_into() -> Result<()> {
    let ctx = create_query_context()?;

    let shuffle_action = ShuffleAction {
        query_id: String::from("query_id"),
        stage_id: String::from("stage_id"),
        plan: PlanParser::parse(ctx.clone(), "SELECT number FROM numbers(5)").await?,
        sinks: vec![String::from("stream_id")],
        scatters_expression: Expression::create_literal(DataValue::UInt64(1)),
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
            assert_eq!(
                action.plan,
                PlanParser::parse(ctx.clone(), "SELECT number FROM numbers(5)").await?
            );
            assert_eq!(action.sinks, vec![String::from("stream_id")]);
            assert_eq!(
                action.scatters_expression,
                Expression::create_literal(DataValue::UInt64(1))
            );
        }
    }

    Ok(())
}
