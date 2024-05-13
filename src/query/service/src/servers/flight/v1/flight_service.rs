// Copyright 2021 Datafuse Labs
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
use std::pin::Pin;

use databend_common_arrow::arrow_format::flight::data::Action;
use databend_common_arrow::arrow_format::flight::data::ActionType;
use databend_common_arrow::arrow_format::flight::data::Criteria;
use databend_common_arrow::arrow_format::flight::data::Empty;
use databend_common_arrow::arrow_format::flight::data::FlightData;
use databend_common_arrow::arrow_format::flight::data::FlightDescriptor;
use databend_common_arrow::arrow_format::flight::data::FlightInfo;
use databend_common_arrow::arrow_format::flight::data::HandshakeRequest;
use databend_common_arrow::arrow_format::flight::data::HandshakeResponse;
use databend_common_arrow::arrow_format::flight::data::PutResult;
use databend_common_arrow::arrow_format::flight::data::Result as FlightResult;
use databend_common_arrow::arrow_format::flight::data::SchemaResult;
use databend_common_arrow::arrow_format::flight::data::Ticket;
use databend_common_arrow::arrow_format::flight::service::flight_service_server::FlightService;
use databend_common_base::match_join_handle;
use databend_common_base::runtime::TrySpawn;
use databend_common_catalog::table_context::TableContext;
use databend_common_config::GlobalConfig;
use databend_common_settings::Settings;
use minitrace::full_name;
use minitrace::prelude::*;
use tokio_stream::Stream;
use tonic::Request;
use tonic::Response as RawResponse;
use tonic::Status;
use tonic::Streaming;

use crate::interpreters::Interpreter;
use crate::interpreters::KillInterpreter;
use crate::interpreters::SetPriorityInterpreter;
use crate::interpreters::TruncateTableInterpreter;
use crate::servers::flight::request_builder::RequestGetter;
use crate::servers::flight::v1::actions::FlightAction;
use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;

pub type FlightStream<T> =
    Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send + Sync + 'static>>;

pub struct DatabendQueryFlightService;

impl DatabendQueryFlightService {
    pub fn create() -> Self {
        DatabendQueryFlightService {}
    }
}

type Response<T> = Result<RawResponse<T>, Status>;
type StreamReq<T> = Request<Streaming<T>>;

#[async_trait::async_trait]
impl FlightService for DatabendQueryFlightService {
    type HandshakeStream = FlightStream<HandshakeResponse>;

    #[async_backtrace::framed]
    async fn handshake(&self, _: StreamReq<HandshakeRequest>) -> Response<Self::HandshakeStream> {
        Result::Err(Status::unimplemented(
            "DatabendQuery does not implement handshake.",
        ))
    }

    type ListFlightsStream = FlightStream<FlightInfo>;

    #[async_backtrace::framed]
    async fn list_flights(&self, _: Request<Criteria>) -> Response<Self::ListFlightsStream> {
        Result::Err(Status::unimplemented(
            "DatabendQuery does not implement list_flights.",
        ))
    }

    #[async_backtrace::framed]
    async fn get_flight_info(&self, _: Request<FlightDescriptor>) -> Response<FlightInfo> {
        Err(Status::unimplemented(
            "DatabendQuery does not implement get_flight_info.",
        ))
    }

    #[async_backtrace::framed]
    async fn get_schema(&self, _: Request<FlightDescriptor>) -> Response<SchemaResult> {
        Err(Status::unimplemented(
            "DatabendQuery does not implement get_schema.",
        ))
    }

    type DoGetStream = FlightStream<FlightData>;

    type DoPutStream = FlightStream<PutResult>;

    #[async_backtrace::framed]
    async fn do_put(&self, _req: StreamReq<FlightData>) -> Response<Self::DoPutStream> {
        Err(Status::unimplemented("unimplemented do_put"))
    }

    type DoExchangeStream = FlightStream<FlightData>;

    #[async_backtrace::framed]
    async fn do_get(&self, request: Request<Ticket>) -> Response<Self::DoGetStream> {
        let root = databend_common_tracing::start_trace_for_remote_request(full_name!(), &request);
        let _guard = root.set_local_parent();

        match request.get_metadata("x-type")?.as_str() {
            "request_server_exchange" => {
                let target = request.get_metadata("x-target")?;
                let query_id = request.get_metadata("x-query-id")?;
                Ok(RawResponse::new(Box::pin(
                    DataExchangeManager::instance().handle_statistics_exchange(query_id, target)?,
                )))
            }
            "exchange_fragment" => {
                let target = request.get_metadata("x-target")?;
                let query_id = request.get_metadata("x-query-id")?;
                let fragment = request
                    .get_metadata("x-fragment-id")?
                    .parse::<usize>()
                    .unwrap();

                Ok(RawResponse::new(Box::pin(
                    DataExchangeManager::instance()
                        .handle_exchange_fragment(query_id, target, fragment)?,
                )))
            }
            exchange_type => Err(Status::unimplemented(format!(
                "Unimplemented exchange type: {:?}",
                exchange_type
            ))),
        }
    }

    #[async_backtrace::framed]
    async fn do_exchange(&self, _: StreamReq<FlightData>) -> Response<Self::DoExchangeStream> {
        Err(Status::unimplemented("unimplemented do_exchange"))
    }

    type DoActionStream = FlightStream<FlightResult>;

    #[async_backtrace::framed]
    async fn do_action(&self, request: Request<Action>) -> Response<Self::DoActionStream> {
        let root = databend_common_tracing::start_trace_for_remote_request(full_name!(), &request);

        async {
            let action = request.into_inner();
            let flight_action: FlightAction = action.try_into()?;

            let action_result = match flight_action {
                FlightAction::InitQueryFragmentsPlan(init_query_fragments_plan) => {
                    let config = GlobalConfig::instance();
                    let session_manager = SessionManager::instance();
                    let settings = Settings::create(config.query.tenant_id.clone());
                    unsafe {
                        // Keep settings
                        settings.unchecked_apply_changes(
                            &init_query_fragments_plan.executor_packet.changed_settings,
                        );
                    }
                    let session =
                        session_manager.create_with_settings(SessionType::FlightRPC, settings)?;
                    let session = session_manager.register_session(session)?;

                    let ctx = session.create_query_context().await?;
                    // Keep query id
                    ctx.set_id(init_query_fragments_plan.executor_packet.query_id.clone());
                    ctx.attach_query_str(
                        init_query_fragments_plan.executor_packet.query_kind,
                        "".to_string(),
                    );

                    let spawner = ctx.clone();
                    let query_id = init_query_fragments_plan.executor_packet.query_id.clone();
                    if let Err(cause) = match_join_handle(
                        spawner.spawn(
                            ctx.get_id(),
                            async move {
                                DataExchangeManager::instance().init_query_fragments_plan(
                                    &ctx,
                                    &init_query_fragments_plan.executor_packet,
                                )
                            }
                            .in_span(Span::enter_with_local_parent(full_name!())),
                        ),
                    )
                    .await
                    {
                        DataExchangeManager::instance().on_finished_query(&query_id);
                        return Err(cause.into());
                    }

                    FlightResult { body: vec![] }
                }
                FlightAction::InitNodesChannel(init_nodes_channel) => {
                    let publisher_packet = &init_nodes_channel.init_nodes_channel_packet;
                    if let Err(cause) = DataExchangeManager::instance()
                        .init_nodes_channel(publisher_packet)
                        .await
                    {
                        let query_id = &init_nodes_channel.init_nodes_channel_packet.query_id;
                        DataExchangeManager::instance().on_finished_query(query_id);
                        return Err(cause.into());
                    }

                    FlightResult { body: vec![] }
                }
                FlightAction::ExecutePartialQuery(query_id) => {
                    if let Err(cause) =
                        DataExchangeManager::instance().execute_partial_query(&query_id)
                    {
                        DataExchangeManager::instance().on_finished_query(&query_id);
                        return Err(cause.into());
                    }

                    FlightResult { body: vec![] }
                }
                FlightAction::TruncateTable(truncate_table) => {
                    let config = GlobalConfig::instance();
                    let session_manager = SessionManager::instance();

                    let settings = Settings::create(config.query.tenant_id.clone());

                    let session =
                        session_manager.create_with_settings(SessionType::FlightRPC, settings)?;

                    let session = session_manager.register_session(session)?;

                    let ctx = session.create_query_context().await?;

                    let interpreter =
                        TruncateTableInterpreter::from_flight(ctx, truncate_table.packet)?;
                    interpreter.execute2().await?;
                    FlightResult { body: vec![] }
                }
                FlightAction::KillQuery(kill_query) => {
                    let config = GlobalConfig::instance();
                    let session_manager = SessionManager::instance();

                    let settings = Settings::create(config.query.tenant_id.clone());

                    let session =
                        session_manager.create_with_settings(SessionType::FlightRPC, settings)?;

                    let session = session_manager.register_session(session)?;

                    let ctx = session.create_query_context().await?;

                    let interpreter = KillInterpreter::from_flight(ctx, kill_query.packet)?;
                    interpreter.execute2().await?;
                    FlightResult { body: vec![] }
                }
                FlightAction::SetPriority(set_priority) => {
                    let config = GlobalConfig::instance();
                    let session_manager = SessionManager::instance();

                    let settings = Settings::create(config.query.tenant_id.clone());

                    let session =
                        session_manager.create_with_settings(SessionType::FlightRPC, settings)?;

                    let session = session_manager.register_session(session)?;

                    let ctx = session.create_query_context().await?;

                    let interpreter =
                        SetPriorityInterpreter::from_flight(ctx, set_priority.packet)?;
                    interpreter.execute2().await?;
                    FlightResult { body: vec![] }
                }
            };

            Ok(RawResponse::new(
                Box::pin(tokio_stream::once(Ok(action_result))) as FlightStream<FlightResult>,
            ))
        }
        .in_span(root)
        .await
    }

    type ListActionsStream = FlightStream<ActionType>;

    #[async_backtrace::framed]
    async fn list_actions(&self, request: Request<Empty>) -> Response<Self::ListActionsStream> {
        let root = databend_common_tracing::start_trace_for_remote_request(full_name!(), &request);

        async {
            Result::Ok(RawResponse::new(
                Box::pin(tokio_stream::iter(vec![
                    Ok(ActionType {
                        r#type: "PrepareShuffleAction".to_string(),
                        description: "Prepare a query stage that can be sent to the remote after receiving data from remote".to_string(),
                    })
                ])) as FlightStream<ActionType>
            ))
        }
            .in_span(root)
            .await
    }
}
