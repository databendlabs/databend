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

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::server::PeekableFlightDataStream;
use arrow_flight::sql::ActionBeginSavepointRequest;
use arrow_flight::sql::ActionBeginSavepointResult;
use arrow_flight::sql::ActionBeginTransactionRequest;
use arrow_flight::sql::ActionBeginTransactionResult;
use arrow_flight::sql::ActionCancelQueryRequest;
use arrow_flight::sql::ActionCancelQueryResult;
use arrow_flight::sql::ActionClosePreparedStatementRequest;
use arrow_flight::sql::ActionCreatePreparedStatementRequest;
use arrow_flight::sql::ActionCreatePreparedStatementResult;
use arrow_flight::sql::ActionCreatePreparedSubstraitPlanRequest;
use arrow_flight::sql::ActionEndSavepointRequest;
use arrow_flight::sql::ActionEndTransactionRequest;
use arrow_flight::sql::Any;
use arrow_flight::sql::CommandGetCatalogs;
use arrow_flight::sql::CommandGetCrossReference;
use arrow_flight::sql::CommandGetDbSchemas;
use arrow_flight::sql::CommandGetExportedKeys;
use arrow_flight::sql::CommandGetImportedKeys;
use arrow_flight::sql::CommandGetPrimaryKeys;
use arrow_flight::sql::CommandGetSqlInfo;
use arrow_flight::sql::CommandGetTableTypes;
use arrow_flight::sql::CommandGetTables;
use arrow_flight::sql::CommandGetXdbcTypeInfo;
use arrow_flight::sql::CommandPreparedStatementQuery;
use arrow_flight::sql::CommandPreparedStatementUpdate;
use arrow_flight::sql::CommandStatementQuery;
use arrow_flight::sql::CommandStatementSubstraitPlan;
use arrow_flight::sql::CommandStatementUpdate;
use arrow_flight::sql::DoPutPreparedStatementResult;
use arrow_flight::sql::DoPutUpdateResult;
use arrow_flight::sql::ProstMessageExt;
use arrow_flight::sql::SqlInfo;
use arrow_flight::sql::TicketStatementQuery;
use arrow_flight::Action;
use arrow_flight::FlightDescriptor;
use arrow_flight::FlightEndpoint;
use arrow_flight::FlightInfo;
use arrow_flight::HandshakeRequest;
use arrow_flight::HandshakeResponse;
use arrow_flight::IpcMessage;
use arrow_flight::Location;
use arrow_flight::PutResult;
use arrow_flight::SchemaAsIpc;
use arrow_flight::Ticket;
use arrow_ipc::writer::IpcWriteOptions;
use databend_common_base::base::uuid::Uuid;
use databend_common_expression::DataSchema;
use futures::Stream;
use log::info;
use prost::Message;
use tonic::metadata::MetadataValue;
use tonic::server::NamedService;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;

use super::status;
use crate::servers::flight_sql::flight_sql_service::FlightSqlServiceImpl;

fn try_unpack_any<T: ProstMessageExt>(message: Any) -> std::result::Result<T, Status> {
    message
        .unpack()
        .map_err(|e| Status::invalid_argument(format!("{e:?}")))?
        .ok_or_else(|| {
            Status::invalid_argument(format!(
                "expect {}, got {}",
                T::type_url(),
                message.type_url
            ))
        })
}

fn simple_flight_info<T: ProstMessageExt>(message: T) -> Response<FlightInfo> {
    let loc = Location {
        uri: "location_not_used".to_string(),
    };

    let buf = message.as_any().encode_to_vec().into();
    let ticket = Ticket { ticket: buf };
    let endpoint = FlightEndpoint {
        ticket: Some(ticket),
        location: vec![loc],
        expiration_time: None,
        app_metadata: Default::default(),
    };
    let endpoints = vec![endpoint];

    let flight_desc = FlightDescriptor {
        r#type: DescriptorType::Cmd.into(),
        cmd: Default::default(),
        path: vec![],
    };
    let info = FlightInfo {
        schema: Default::default(),
        flight_descriptor: Some(flight_desc),
        endpoint: endpoints,
        total_records: -1,
        total_bytes: -1,
        ordered: false,
        app_metadata: Default::default(),
    };
    Response::new(info)
}

impl NamedService for FlightSqlServiceImpl {
    const NAME: &'static str = "FlightSqlService";
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceImpl {
    type FlightService = FlightSqlServiceImpl;

    #[async_backtrace::framed]
    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> std::result::Result<
        Response<
            Pin<Box<dyn Stream<Item = std::result::Result<HandshakeResponse, Status>> + Send>>,
        >,
        Status,
    > {
        let (user, password) = FlightSqlServiceImpl::get_user_password(request.metadata())
            .map_err(Status::invalid_argument)?;
        let client_ip = request.remote_addr().map(|a| a.ip().to_string());
        let session =
            FlightSqlServiceImpl::auth_user_password(user, password, client_ip.as_deref()).await?;
        let token = Uuid::new_v4().to_string();
        let result = HandshakeResponse {
            protocol_version: 0,
            payload: token.as_bytes().to_vec().into(),
        };
        let result = Ok(result);
        let str = format!("Bearer {token}");
        let output = futures::stream::iter(vec![result]);
        let mut resp: Response<Pin<Box<dyn Stream<Item = std::result::Result<_, _>> + Send>>> =
            Response::new(Box::pin(output));
        let metadata = MetadataValue::try_from(str)
            .map_err(|_| Status::internal("authorization not parsable"))?;
        resp.metadata_mut().insert("authorization", metadata);

        session.get_status().write().is_native_client =
            FlightSqlServiceImpl::get_header_value(request.metadata(), "bendsql").is_some();

        let session_keep_alive =
            FlightSqlServiceImpl::get_header_value(request.metadata(), "session_keep_alive")
                .map(|v| v.parse::<u64>().unwrap_or(360))
                .unwrap_or(360);

        self.sessions.lock().insert(
            token,
            session,
            Some(Duration::from_secs(session_keep_alive)),
        );

        Ok(resp)
    }

    #[async_backtrace::framed]
    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        message: Any,
    ) -> std::result::Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let session = self.get_session(&request)?;
        let fetch_results: FetchResults = try_unpack_any(message)?;

        let handle = Uuid::try_parse(&fetch_results.handle).map_err(|e| {
            Status::internal(format!(
                "do_get_fallback Error decoding handle: {e} {:?}",
                fetch_results.handle
            ))
        })?;

        info!("do_get_fallback with handle={handle}");

        let handle_plan = self.statements.get(&handle).unwrap();
        let stream = self
            .execute_query(session, &handle_plan.value().0, &handle_plan.value().1)
            .await
            .map_err(|e| status!("fail to execute", e))?;
        let resp = Response::new(stream);
        Ok(resp)
    }

    #[async_backtrace::framed]
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_sql_info(query={})", query.query);
        let _session = self.get_session(&request)?;
        Ok(simple_flight_info(query))
    }

    #[async_backtrace::framed]
    async fn get_flight_info_prepared_statement(
        &self,
        cmd: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        let _session = self.get_session(&request);
        let handle = Uuid::from_slice(cmd.prepared_statement_handle.as_ref())
            .map_err(|e| Status::internal(format!("Error decoding handle: {e}")))?;

        info!("get_flight_info_prepared_statement with handle={handle}");

        let handle_plan_ref = self.statements.get(&handle).unwrap();
        let schema = handle_plan_ref.value().0.schema().as_ref().into();
        let loc = Location {
            uri: "grpc+tcp://127.0.0.1".to_string(),
        };
        let fetch = FetchResults {
            handle: handle.to_string(),
        };
        let buf = fetch.as_any().encode_to_vec().into();
        let ticket = Ticket { ticket: buf };
        let endpoint = FlightEndpoint {
            ticket: Some(ticket),
            location: vec![loc],
            expiration_time: None,
            app_metadata: Default::default(),
        };
        let endpoints = vec![endpoint];

        let message = SchemaAsIpc::new(&schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e| status!("Unable to serialize schema", e))?;
        let IpcMessage(schema_bytes) = message;

        let flight_desc = FlightDescriptor {
            r#type: DescriptorType::Cmd.into(),
            cmd: Default::default(),
            path: vec![],
        };
        let info = FlightInfo {
            schema: schema_bytes,
            flight_descriptor: Some(flight_desc),
            endpoint: endpoints,
            total_records: -1,
            total_bytes: -1,
            ordered: false,
            app_metadata: Default::default(),
        };
        let resp = Response::new(info);
        Ok(resp)
    }

    #[async_backtrace::framed]
    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_catalogs()");
        let _session = self.get_session(&request)?;
        Ok(simple_flight_info(query))
    }

    #[async_backtrace::framed]
    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_schemas({query:?})");
        let _session = self.get_session(&request)?;
        Ok(simple_flight_info(query))
    }

    #[async_backtrace::framed]
    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_tables({query:?})");
        let _session = self.get_session(&request)?;
        Ok(simple_flight_info(query))
    }

    #[async_backtrace::framed]
    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_table_types()");
        let _session = self.get_session(&request)?;
        Ok(simple_flight_info(query))
    }

    #[async_backtrace::framed]
    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_sql_info({query:?})");
        let _session = self.get_session(&request)?;
        Ok(simple_flight_info(query))
    }

    #[async_backtrace::framed]
    async fn get_flight_info_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_primary_keys({query:?})",);
        Err(Status::unimplemented(
            "get_flight_info_primary_keys not implemented",
        ))
    }

    #[async_backtrace::framed]
    async fn get_flight_info_exported_keys(
        &self,
        query: CommandGetExportedKeys,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_exported_keys({query:?})");
        Err(Status::unimplemented(
            "get_flight_info_exported_keys not implemented",
        ))
    }

    #[async_backtrace::framed]
    async fn get_flight_info_imported_keys(
        &self,
        query: CommandGetImportedKeys,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_imported_keys({query:?})");
        Err(Status::unimplemented(
            "get_flight_info_imported_keys not implemented",
        ))
    }

    #[async_backtrace::framed]
    async fn get_flight_info_cross_reference(
        &self,
        query: CommandGetCrossReference,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_cross_reference({query:?})");
        Err(Status::unimplemented(
            "get_flight_info_imported_keys not implemented",
        ))
    }

    // do_get
    #[async_backtrace::framed]
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> std::result::Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_statement({ticket:?}");
        Err(Status::unimplemented("do_get_statement not implemented"))
    }

    #[async_backtrace::framed]
    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        _request: Request<Ticket>,
    ) -> std::result::Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_prepared_statement({query:?}");
        Err(Status::unimplemented(
            "do_get_prepared_statement not implemented",
        ))
    }

    #[async_backtrace::framed]
    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> std::result::Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_catalogs()");
        Err(Status::unimplemented("do_get_catalogs not implemented"))
    }

    #[async_backtrace::framed]
    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> std::result::Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_schemas({query:?}");
        Err(Status::unimplemented("do_get_schemas not implemented"))
    }

    #[async_backtrace::framed]
    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_tables({query:?})");
        let session = self.get_session(&request)?;
        let context = session
            .create_query_context()
            .await
            .map_err(|e| status!("Could not create_query_context", e))?;
        Ok(Response::new(
            super::CatalogInfoProvider::get_tables(context.clone(), query.catalog.clone(), None)
                .await?,
        ))
    }

    #[async_backtrace::framed]
    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> std::result::Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_table_types()");
        Err(Status::unimplemented("do_get_table_types not implemented"))
    }

    #[async_backtrace::framed]
    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> std::result::Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_sql_info({query:?})");
        Ok(Response::new(super::SqlInfoProvider::all_info()?))
    }

    #[async_backtrace::framed]
    async fn do_get_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        _request: Request<Ticket>,
    ) -> std::result::Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_primary_keys({query:?})");
        Err(Status::unimplemented("do_get_primary_keys not implemented"))
    }

    #[async_backtrace::framed]
    async fn do_get_exported_keys(
        &self,
        query: CommandGetExportedKeys,
        _request: Request<Ticket>,
    ) -> std::result::Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_exported_keys({query:?})");
        Err(Status::unimplemented(
            "do_get_exported_keys not implemented",
        ))
    }

    #[async_backtrace::framed]
    async fn do_get_imported_keys(
        &self,
        query: CommandGetImportedKeys,
        _request: Request<Ticket>,
    ) -> std::result::Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_imported_keys({query:?})");
        Err(Status::unimplemented(
            "do_get_imported_keys not implemented",
        ))
    }

    #[async_backtrace::framed]
    async fn do_get_cross_reference(
        &self,
        query: CommandGetCrossReference,
        _request: Request<Ticket>,
    ) -> std::result::Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_cross_reference({query:?})");
        Err(Status::unimplemented(
            "do_get_cross_reference not implemented",
        ))
    }

    // called by rust FlightSqlServiceClient, which is used in unit test.
    #[async_backtrace::framed]
    async fn do_put_statement_update(
        &self,
        ticket: CommandStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> std::result::Result<i64, Status> {
        let session = self.get_session(&request)?;
        let query = ticket.query;
        info!("do_put_statement_update with query = {query}");

        let (plan, plan_extras) = self
            .plan_sql(&session, &query)
            .await
            .map_err(|e| status!("Error getting result schema", e))?;
        let res = self
            .execute_update(session, &plan, &plan_extras)
            .await
            .map_err(|e| status!("fail to execute", e))?;
        Ok(res)
    }

    #[async_backtrace::framed]
    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<PeekableFlightDataStream>,
    ) -> std::result::Result<DoPutPreparedStatementResult, Status> {
        let session = self.get_session(&request)?;
        let handle = Uuid::from_slice(query.prepared_statement_handle.as_ref())
            .map_err(|e| Status::internal(format!("Error decoding handle: {e}")))?;

        info!("do_put_prepared_statement_query with handle={handle}");

        let handle_plan = self.statements.get(&handle).unwrap();
        let record_count = self
            .execute_update(session, &handle_plan.value().0, &handle_plan.value().1)
            .await
            .map_err(|e| status!("fail to execute", e))?;
        let result = DoPutUpdateResult { record_count };
        let result = PutResult {
            app_metadata: result.as_any().encode_to_vec().into(),
        };

        Ok(DoPutPreparedStatementResult {
            prepared_statement_handle: Some(result.encode_to_vec().into()),
        })
    }

    // called by JDBC
    #[async_backtrace::framed]
    async fn do_put_prepared_statement_update(
        &self,
        query: CommandPreparedStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> std::result::Result<i64, Status> {
        let session = self.get_session(&request)?;
        let handle = Uuid::from_slice(query.prepared_statement_handle.as_ref())
            .map_err(|e| Status::internal(format!("Error decoding handle: {e}")))?;

        info!("do_put_prepared_statement_update with handle={handle}");

        let handle_plan = self.statements.get(&handle).unwrap();
        let res = self
            .execute_update(session, &handle_plan.value().0, &handle_plan.value().1)
            .await
            .map_err(|e| status!("fail to execute", e))?;

        info!("do_put_prepared_statement_update with handle={handle} return {res}");
        Ok(res)
    }

    #[async_backtrace::framed]
    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> std::result::Result<ActionCreatePreparedStatementResult, Status> {
        let session = self.get_session(&request)?;
        let sql = query.query.clone();
        let handle = Uuid::new_v4();
        let plan = self
            .plan_sql(&session, &sql)
            .await
            .map_err(|e| status!("Error getting result schema", e))?;
        info!(
            "do_action_create_prepared_statement with handler={handle} query={:?}",
            query.query
        );
        // JDBC client use call put when schema.fields == 0
        let data_schema = if plan.0.has_result_set() {
            plan.0.schema()
        } else {
            Arc::new(DataSchema::empty())
        };
        info!(
            "do_action_create_prepared_statement with handler={handle}, query={:?}, return schema={data_schema:?}",
            query.query
        );
        let schema = (&*data_schema).into();
        self.statements.insert(handle, plan);
        let message = SchemaAsIpc::new(&schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e| status!("Unable to serialize schema", e))?;
        let IpcMessage(schema_bytes) = message;
        let res = ActionCreatePreparedStatementResult {
            prepared_statement_handle: handle.as_bytes().to_vec().into(),
            dataset_schema: schema_bytes,
            parameter_schema: Default::default(), // TODO: parameters
        };
        Ok(res)
    }

    #[async_backtrace::framed]
    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        request: Request<Action>,
    ) -> std::result::Result<(), Status> {
        let handle = query.prepared_statement_handle.as_ref();
        if let Ok(handle) = std::str::from_utf8(handle) {
            info!(
                "do_action_close_prepared_statement with handle {:?}",
                handle
            );
            match Uuid::try_parse(handle) {
                Ok(handle) => {
                    if self.get_session(&request).is_ok() {
                        self.statements.remove(&handle);
                    }
                }
                Err(e) => {
                    return Err(Status::internal(format!(
                        "do_action_close_prepared_statement Error decoding handle: {e} {handle:?}"
                    )));
                }
            }
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn register_sql_info(&self, id: i32, result: &SqlInfo) {
        info!("register_sql_info({id}, {result:?})");
    }

    /// Get a FlightInfo to extract information about the supported XDBC types.
    async fn get_flight_info_xdbc_type_info(
        &self,
        _query: CommandGetXdbcTypeInfo,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        unimplemented!()
    }

    /// Get a FlightDataStream containing the data related to the supported XDBC types.
    async fn do_get_xdbc_type_info(
        &self,
        _query: CommandGetXdbcTypeInfo,
        _request: Request<Ticket>,
    ) -> std::result::Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        unimplemented!()
    }

    async fn get_flight_info_substrait_plan(
        &self,
        _query: CommandStatementSubstraitPlan,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        unimplemented!()
    }

    async fn do_put_substrait_plan(
        &self,
        _query: CommandStatementSubstraitPlan,
        _request: Request<PeekableFlightDataStream>,
    ) -> std::result::Result<i64, Status> {
        unimplemented!()
    }

    async fn do_action_create_prepared_substrait_plan(
        &self,
        _query: ActionCreatePreparedSubstraitPlanRequest,
        _request: Request<Action>,
    ) -> std::result::Result<ActionCreatePreparedStatementResult, Status> {
        unimplemented!()
    }

    async fn do_action_begin_transaction(
        &self,
        _query: ActionBeginTransactionRequest,
        _request: Request<Action>,
    ) -> std::result::Result<ActionBeginTransactionResult, Status> {
        unimplemented!()
    }

    async fn do_action_end_transaction(
        &self,
        _query: ActionEndTransactionRequest,
        _request: Request<Action>,
    ) -> std::result::Result<(), Status> {
        unimplemented!()
    }

    async fn do_action_begin_savepoint(
        &self,
        _query: ActionBeginSavepointRequest,
        _request: Request<Action>,
    ) -> std::result::Result<ActionBeginSavepointResult, Status> {
        unimplemented!()
    }

    async fn do_action_end_savepoint(
        &self,
        _query: ActionEndSavepointRequest,
        _request: Request<Action>,
    ) -> std::result::Result<(), Status> {
        unimplemented!()
    }

    async fn do_action_cancel_query(
        &self,
        _query: ActionCancelQueryRequest,
        _request: Request<Action>,
    ) -> std::result::Result<ActionCancelQueryResult, Status> {
        unimplemented!()
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchResults {
    #[prost(string, tag = "1")]
    pub handle: ::prost::alloc::string::String,
}

impl ProstMessageExt for FetchResults {
    fn type_url() -> &'static str {
        "type.googleapis.com/arrow.flight.protocol.sql.FetchResults"
    }

    fn as_any(&self) -> Any {
        Any {
            type_url: FetchResults::type_url().to_string(),
            value: ::prost::Message::encode_to_vec(self).into(),
        }
    }
}
