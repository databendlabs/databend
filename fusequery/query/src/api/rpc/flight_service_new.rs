use std::convert::TryInto;

use common_arrow::arrow_flight::flight_service_server::FlightService;
use tonic::{Status, Response, Request, Streaming};
use common_arrow::arrow_flight::{Action, Ticket, FlightData, FlightInfo, Criteria, ActionType, SchemaResult, HandshakeResponse, Empty, FlightDescriptor, PutResult, HandshakeRequest};
use crate::api::rpc::{FlightStream, Queries, StagePtr, Stages, FlightDataReceiver};
use common_arrow::arrow_flight::Result as FlightResult;
use super::FlightAction;
use common_flights::{QueryDoAction, ExecutePlanWithShuffleAction};
use tokio_stream::StreamExt;
use std::borrow::BorrowMut;
use std::sync::Arc;
use common_infallible::RwLock;
use std::cell::RefCell;
use std::collections::HashMap;
use tokio_stream::wrappers::ReceiverStream;
use tokio::runtime::Runtime;
use super::QueryInfoPtr;

pub struct FuseQueryService {
    pub queries: Queries
    // conf: Config,
    // cluster: ClusterRef,
    // session_manager: SessionRef,
}

impl FlightService for FuseQueryService {
    type HandshakeStream = FlightStream<HandshakeResponse>;

    async fn handshake(&self, request: Request<Streaming<HandshakeRequest>>) -> Result<Response<Self::HandshakeStream>, Status> {
        unimplemented!()
    }

    type ListFlightsStream = FlightStream<FlightInfo>;

    async fn list_flights(&self, request: Request<Criteria>) -> Result<Response<Self::ListFlightsStream>, Status> {
        let criteria = request.into_inner();
        let expression = criteria.expression.into_string()?;

        fn get_flight(query_id: &String, stage_id: &String, flight_id: &String) -> FlightInfo {
            FlightInfo {
                schema:,
                endpoint: vec![],
                flight_descriptor: None,
                total_records: -1,
                total_bytes: -1,
            }
        }

        fn get_flights(query_id: &String, v: (&String, &StagePtr), expressions: &Vec<&str>) -> Vec<FlightInfo> {
            let (stage_id, stage) = v;
            if expressions.len() < 3 {
                return stage.flights
                    .iter()
                    .map(|(id, _)| get_flight(query_id, stage_id, id))
                    .collect_vec();
            }

            stage.flights
                .iter()
                .filter(|(id, _)| id.starts_with(expressions[2]))
                .map(|(id, _)| get_flight(query_id, stage_id, id))
                .collect_vec()
        }

        fn get_stage_flights(v: (&String, &QueryInfoPtr), expressions: &Vec<&str>) -> Vec<FlightInfo> {
            let (query_id, query) = v;
            if expressions.len() < 2 {
                return query.stages
                    .iter()
                    .flat_map(|v| get_flights(query_id, v, expressions))
                    .collect_vec();
            }

            query.stages
                .iter()
                .filter(|(id, _)| id.starts_with(expressions[1]))
                .flat_map(|v| get_flights(query_id, v, expressions))
                .collect_vec()
        }

        fn get_queries_flights(queries: &Queries, expressions: &Vec<&str>) -> Vec<FlightInfo> {
            match expressions.len() {
                0 => {
                    queries
                        .read()
                        .iter()
                        .flat_map(|v| get_stage_flights(v, expressions))
                },
                _ => {
                    queries
                        .read()
                        .iter()
                        .filter(|(id, _)| id.starts_with(expressions[0]))
                        .flat_map(|v| get_stage_flights(v, expressions))
                }
            }.collect_vec()
        }

        let expressions = expression.trim_start_matches("/").split("/").collect_vec();
        let stream = futures::stream::iter(flightsget_queries_flights(&self.queries, &expressions).iter().map(Result::Ok));
        Ok(Response::new(Box::pin(stream) as Self::DoActionStream))
    }

    async fn get_flight_info(&self, request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        unimplemented!()
    }

    async fn get_schema(&self, request: Request<FlightDescriptor>) -> Result<Response<SchemaResult>, Status> {
        unimplemented!()
    }

    type DoGetStream = FlightStream<FlightData>;

    async fn do_get(&self, request: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket: Ticket = request.try_into()?;
        let ticket = ticket.ticket.to_str()?.to_string();

        println!("Get Flight Stream {}", ticket);

        // Flight ticket = query_id/stage_id/stream_id
        match ticket.trim_start_matches("/").split("/").collect_vec() {
            tickets if tickets.len() != 3 => Err(Status::invalid_argument("")),
            tickets => {
                let (query_id, stage_id, stream_id) = (tickets[0], tickets[1], tickets[2]);

                let queries = self.queries.read();
                let flight_info = queries.get(query_id)
                    .and_then(|query_info| query_info.stages.get(stage_id))
                    .and_then(|query_stage_info| query_stage_info.flights.remove(stream_id));

                // TODO: Tracking progress is shown in the system.shuffles table
                match flight_info {
                    None => Err(Status::not_found("")),
                    Some(receiver) => Ok(Response::new(Box::pin(
                        ReceiverStream::new(receiver)) as Self::DoGetStream)),
                }
            }
        }
    }

    type DoPutStream = FlightStream<PutResult>;

    async fn do_put(&self, request: Request<Streaming<FlightData>>) -> Result<Response<Self::DoPutStream>, Status> {
        unimplemented!()
    }

    type DoExchangeStream = FlightStream<FlightData>;

    async fn do_exchange(&self, request: Request<Streaming<FlightData>>) -> Result<Response<Self::DoExchangeStream>, Status> {
        unimplemented!()
    }

    type DoActionStream = FlightStream<FlightResult>;

    async fn do_action(&self, request: Request<Action>) -> Result<Response<Self::DoActionStream>, Status> {
        use super::ActionService;
        let action: QueryDoAction = request.try_into()?;
        match action {
            QueryDoAction::FetchPartition(v) => Result::Err(Status::unimplemented("")),
            QueryDoAction::ExecutePlanWithShuffle(v) => {
                ActionService::<ExecutePlanWithShuffleAction>::process(self.queries.clone(), v)
            }
        }
    }

    type ListActionsStream = FlightStream<ActionType>;

    async fn list_actions(&self, request: Request<Empty>) -> Result<Response<Self::ListActionsStream>, Status> {
        unimplemented!()
    }
}

