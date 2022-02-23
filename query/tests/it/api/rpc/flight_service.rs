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
use std::sync::Arc;

use common_arrow::arrow_format::flight::data::Action;
use common_arrow::arrow_format::flight::data::Ticket;
use common_arrow::arrow_format::flight::service::flight_service_server::FlightService;
use common_base::tokio;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ABORT_SESSION;
use common_planners::Expression;
use databend_query::api::DatabendQueryFlightDispatcher;
use databend_query::api::DatabendQueryFlightService;
use databend_query::api::FlightAction;
use databend_query::api::FlightTicket;
use databend_query::api::ShuffleAction;
use databend_query::api::StreamTicket;
use databend_query::sql::PlanParser;
use tonic::Request;

use crate::tests::create_query_context;
use crate::tests::SessionManagerBuilder;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_do_flight_action_with_shared_session() -> Result<()> {
    let sessions = SessionManagerBuilder::create().build()?;
    let dispatcher = Arc::new(DatabendQueryFlightDispatcher::create());
    let service = DatabendQueryFlightService::create(dispatcher, sessions);

    for index in 0..2 {
        let query_id = "query_id";
        let stage_id = format!("stage_id_{}", index);
        let request = do_action_request(query_id, &stage_id).await;
        service.do_action(request?).await?;
    }

    for index in 0..2 {
        let query_id = "query_id";
        let stage_id = format!("stage_id_{}", index);
        let request = do_get_request(query_id, &stage_id);
        service.do_get(request?).await?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_do_flight_action_with_different_session() -> Result<()> {
    let sessions = SessionManagerBuilder::create().build()?;
    let dispatcher = Arc::new(DatabendQueryFlightDispatcher::create());
    let service = DatabendQueryFlightService::create(dispatcher, sessions);

    for index in 0..2 {
        let query_id = format!("query_id_{}", index);
        let stage_id = format!("stage_id_{}", index);
        let request = do_action_request(&query_id, &stage_id).await;
        service.do_action(request?).await?;
    }

    for index in 0..2 {
        let query_id = format!("query_id_{}", index);
        let stage_id = format!("stage_id_{}", index);
        let request = do_get_request(&query_id, &stage_id);
        service.do_get(request?).await?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_do_flight_action_with_abort_session() -> Result<()> {
    let sessions = SessionManagerBuilder::create().build()?;
    let dispatcher = Arc::new(DatabendQueryFlightDispatcher::create());
    let service = DatabendQueryFlightService::create(dispatcher.clone(), sessions);

    for index in 0..2 {
        let query_id = "query_id_1";
        let stage_id = format!("stage_id_{}", index);
        let request = do_action_request(query_id, &stage_id).await;
        service.do_action(request?).await?;
    }

    dispatcher.abort();

    for index in 2..4 {
        let query_id = "query_id_1";
        let stage_id = format!("stage_id_{}", index);
        let request = do_action_request(query_id, &stage_id).await;
        service.do_action(request?).await?;
    }

    for index in 0..4 {
        let query_id = "query_id_1";
        let stage_id = format!("stage_id_{}", index);
        let request = do_get_request(query_id, &stage_id);
        service.do_get(request?).await?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_do_flight_action_with_abort_and_new_session() -> Result<()> {
    let sessions = SessionManagerBuilder::create().build()?;
    let dispatcher = Arc::new(DatabendQueryFlightDispatcher::create());
    let service = DatabendQueryFlightService::create(dispatcher.clone(), sessions);

    for index in 0..2 {
        let query_id = "query_id_1";
        let stage_id = format!("stage_id_{}", index);
        let request = do_action_request(query_id, &stage_id).await;
        service.do_action(request?).await?;
    }

    dispatcher.abort();

    let query_id = "query_id_2";
    let stage_id = "stage_id_1";
    let request = do_action_request(query_id, stage_id).await;
    match service.do_action(request?).await {
        Ok(_) => panic!("Aborted rpc service must be cannot create new session"),
        Err(error) => {
            let error_code = ErrorCode::from(error);
            assert_eq!(error_code.code(), ABORT_SESSION);
            assert_eq!(error_code.message(), "Aborting server.");
        }
    }

    for index in 0..2 {
        let query_id = "query_id_1";
        let stage_id = format!("stage_id_{}", index);
        let request = do_get_request(query_id, &stage_id);
        service.do_get(request?).await?;
    }

    Ok(())
}

fn do_get_request(query_id: &str, stage_id: &str) -> Result<Request<Ticket>> {
    let stream_ticket = FlightTicket::StreamTicket(StreamTicket {
        query_id: String::from(query_id),
        stage_id: String::from(stage_id),
        stream: String::from("stream_id"),
    });

    Ok(Request::new(stream_ticket.try_into()?))
}

async fn do_action_request(query_id: &str, stage_id: &str) -> Result<Request<Action>> {
    let ctx = create_query_context()?;
    let flight_action = FlightAction::PrepareShuffleAction(ShuffleAction {
        query_id: String::from(query_id),
        stage_id: String::from(stage_id),
        plan: PlanParser::parse(ctx.clone(), "SELECT number FROM numbers(5)").await?,
        sinks: vec![String::from("stream_id")],
        scatters_expression: Expression::create_literal(DataValue::UInt64(1)),
    });

    Ok(Request::new(flight_action.try_into()?))
}
