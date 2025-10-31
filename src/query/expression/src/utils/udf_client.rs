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

use std::error::Error as StdError;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::FlightDescriptor;
use arrow_schema::ArrowError;
use arrow_select::concat::concat_batches;
use databend_common_base::headers::HEADER_FUNCTION;
use databend_common_base::headers::HEADER_FUNCTION_HANDLER;
use databend_common_base::headers::HEADER_QUERY_ID;
use databend_common_base::headers::HEADER_TENANT;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_grpc::DNSService;
use databend_common_metrics::external_server::record_connect_external_duration;
use databend_common_metrics::external_server::record_request_external_batch_rows;
use databend_common_metrics::external_server::record_request_external_duration;
use databend_common_metrics::external_server::record_running_requests_external_finish;
use databend_common_metrics::external_server::record_running_requests_external_start;
use futures::stream;
use futures::StreamExt;
use futures::TryStreamExt;
use hyper_util::client::legacy::connect::HttpConnector;
use tonic::metadata::KeyAndValueRef;
use tonic::metadata::MetadataKey;
use tonic::metadata::MetadataMap;
use tonic::metadata::MetadataValue;
use tonic::transport::channel::Channel;
use tonic::transport::ClientTlsConfig;
use tonic::transport::Endpoint;
use tonic::Request;
use tonic::Status;

use crate::types::DataType;
use crate::variant_transform::contains_variant;
use crate::variant_transform::transform_variant;
use crate::BlockEntry;
use crate::DataBlock;
use crate::DataField;
use crate::DataSchema;

const UDF_TCP_KEEP_ALIVE_SEC: u64 = 30;
const UDF_HTTP2_KEEP_ALIVE_INTERVAL_SEC: u64 = 60;
const UDF_KEEP_ALIVE_TIMEOUT_SEC: u64 = 20;
// 4MB by default, we use 16G
// max_encoding_message_size is usize::max by default
const MAX_DECODING_MESSAGE_SIZE: usize = 16 * 1024 * 1024 * 1024;
const TRANSPORT_ERROR_SNIPPETS: &[&str] = &[
    "h2 protocol error",
    "broken pipe",
    "connection reset",
    "error reading a body from connection",
];

#[derive(Debug)]
enum FlightDecodeIssue {
    TransportInterrupted,
    ServerStatus(String),
    SchemaMismatch,
    MalformedData,
    Other,
}

#[derive(Debug, Clone)]
pub struct UDFFlightClient {
    inner: FlightServiceClient<Channel>,
    batch_rows: usize,
    headers: MetadataMap,
}

impl UDFFlightClient {
    pub fn build_endpoint(
        addr: &str,
        conn_timeout: u64,
        request_timeout: u64,
        user_agent: &str,
    ) -> Result<Arc<Endpoint>> {
        let tls_config = ClientTlsConfig::new().with_native_roots();
        let _ = rustls::crypto::ring::default_provider().install_default();
        let endpoint = Endpoint::from_shared(addr.to_string())
            .map_err(|err| {
                ErrorCode::UDFServerConnectError(format!("Invalid UDF Server address: {err}"))
            })?
            .user_agent(user_agent)
            .map_err(|err| {
                ErrorCode::UDFServerConnectError(format!("Invalid UDF Client User Agent: {err}"))
            })?
            .connect_timeout(Duration::from_secs(conn_timeout))
            .timeout(Duration::from_secs(request_timeout))
            .tcp_keepalive(Some(Duration::from_secs(UDF_TCP_KEEP_ALIVE_SEC)))
            .http2_keep_alive_interval(Duration::from_secs(UDF_HTTP2_KEEP_ALIVE_INTERVAL_SEC))
            .keep_alive_timeout(Duration::from_secs(UDF_KEEP_ALIVE_TIMEOUT_SEC))
            .keep_alive_while_idle(true)
            .tls_config(tls_config)
            .map_err(|err| {
                ErrorCode::UDFServerConnectError(format!("Invalid UDF Client TLS Config: {err}"))
            })?;

        Ok(Arc::new(endpoint))
    }

    #[async_backtrace::framed]
    pub async fn connect(
        func_name: &str,
        endpoint: Arc<Endpoint>,
        conn_timeout: u64,
        batch_rows: usize,
    ) -> Result<UDFFlightClient> {
        let instant = Instant::now();

        let mut connector = HttpConnector::new_with_resolver(DNSService);
        connector.enforce_http(false);
        connector.set_nodelay(true);
        connector.set_keepalive(Some(Duration::from_secs(UDF_TCP_KEEP_ALIVE_SEC)));
        connector.set_connect_timeout(Some(Duration::from_secs(conn_timeout)));
        connector.set_reuse_address(true);

        let channel = endpoint
            .connect_with_connector(connector)
            .await
            .map_err(|err| {
                ErrorCode::UDFServerConnectError(format!(
                    "Cannot connect to UDF Server {}: {:?}",
                    endpoint.uri(),
                    err
                ))
            })?;
        let inner =
            FlightServiceClient::new(channel).max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE);

        let connect_duration = instant.elapsed();
        record_connect_external_duration(func_name, connect_duration);

        Ok(UDFFlightClient {
            inner,
            batch_rows,
            headers: MetadataMap::new(),
        })
    }

    pub fn with_headers<S: AsRef<str>, H: IntoIterator<Item = (S, S)>>(
        mut self,
        headers: H,
    ) -> Result<Self> {
        for (key, value) in headers.into_iter() {
            let key = key.as_ref();
            let value = value.as_ref();
            let key = MetadataKey::from_str(key)
                .map_err(|err| ErrorCode::UDFDataError(format!("Parse key {key} error: {err}")))?;
            let value = MetadataValue::from_str(value).map_err(|err| {
                ErrorCode::UDFDataError(format!("Parse value {value} error: {err}"))
            })?;
            self.headers.insert(key, value);
        }
        Ok(self)
    }

    /// Set tenant for the UDF client.
    pub fn with_tenant(self, tenant: &str) -> Result<Self> {
        self.with_headers([(HEADER_TENANT, tenant)])
    }

    /// Set function name for the UDF client.
    pub fn with_func_name(self, func_name: &str) -> Result<Self> {
        self.with_headers([(HEADER_FUNCTION, func_name)])
    }

    pub fn with_handler_name(self, handler_name: &str) -> Result<Self> {
        self.with_headers([(HEADER_FUNCTION_HANDLER, handler_name)])
    }

    /// Set query id for the UDF client.
    pub fn with_query_id(self, query_id: &str) -> Result<Self> {
        self.with_headers([(HEADER_QUERY_ID, query_id)])
    }

    fn make_request<T>(&self, t: T) -> Request<T> {
        let mut request = Request::new(t);
        for k_v in self.headers.iter() {
            match k_v {
                KeyAndValueRef::Ascii(key, value) => {
                    request.metadata_mut().insert(key, value.clone());
                }
                KeyAndValueRef::Binary(key, value) => {
                    request.metadata_mut().insert_bin(key, value.clone());
                }
            }
        }

        request
    }

    #[async_backtrace::framed]
    pub async fn check_schema(
        &mut self,
        func_name: &str,
        arg_types: &[DataType],
        return_type: &DataType,
    ) -> Result<()> {
        // DataType::StageLocation is only used to pass the stage location parameter to the external UDF.
        // It will be passed to the Python server in the UDF headers and skipped when passing this parameter to the UDF server.
        // That is why it is skipped.
        fn eq_skip_stage(remote_args: &[DataType], input_args: &[DataType]) -> bool {
            remote_args
                .iter()
                .zip(
                    input_args
                        .iter()
                        .filter(|ty| ty.remove_nullable() != DataType::StageLocation),
                )
                .all(|(x, y)| x == y)
        }

        let descriptor = FlightDescriptor::new_path(vec![func_name.to_string()]);
        let request = self.make_request(descriptor);
        let flight_info = self.inner.get_flight_info(request).await?.into_inner();
        let schema = flight_info
            .try_decode_schema()
            .map_err(|err| {
                ErrorCode::UDFDataError(format!(
                    "Decode UDF schema failed on UDF function {func_name}: {err}"
                ))
            })
            .and_then(|schema| DataSchema::try_from(&schema))?;

        let fields_num = schema.fields().len();
        if fields_num == 0 {
            return Err(ErrorCode::UDFSchemaMismatch(format!(
                "UDF Server should return at least one column on UDF function {func_name}"
            )));
        }

        let (input_fields, output_fields) = schema.fields().split_at(fields_num - 1);
        let remote_arg_types = input_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>();
        let expect_return_type = output_fields
            .iter()
            .map(|f| f.data_type())
            .collect::<Vec<_>>();
        if !eq_skip_stage(&remote_arg_types, arg_types) {
            return Err(ErrorCode::UDFSchemaMismatch(format!(
                "UDF arg types mismatch on UDF function {}, remote arg types: ({:?}), defined arg types: ({:?})",
                func_name,
                remote_arg_types
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", "),
                arg_types
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }

        if expect_return_type[0] != return_type {
            return Err(ErrorCode::UDFSchemaMismatch(format!(
                "UDF return type mismatch on UDF function {}, expected return type: {}, actual return type: {}",
                func_name,
                expect_return_type[0],
                return_type
            )));
        }

        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn do_exchange(
        &mut self,
        name: &str,
        func_name: &str,
        num_rows: usize,
        args: Vec<BlockEntry>,
        return_type: &DataType,
    ) -> Result<BlockEntry> {
        let instant = Instant::now();

        Profile::record_usize_profile(ProfileStatisticsName::ExternalServerRequestCount, 1);
        record_running_requests_external_start(name, 1);
        record_request_external_batch_rows(func_name, num_rows);

        let args = args
            .into_iter()
            .map(|arg| {
                if contains_variant(&arg.data_type()) {
                    let new_arg = BlockEntry::new(transform_variant(&arg.value(), true)?, || {
                        (arg.data_type(), arg.len())
                    });
                    Ok(new_arg)
                } else {
                    Ok(arg)
                }
            })
            .collect::<Result<Vec<_>>>()?;

        let fields = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| DataField::new(&format!("arg{}", idx + 1), arg.data_type()))
            .collect::<Vec<_>>();
        let data_schema = DataSchema::new(fields);

        let input_batch = DataBlock::new(args, num_rows)
            .to_record_batch_with_dataschema(&data_schema)
            .map_err(|err| ErrorCode::from_string(format!("{err}")))?;

        let result_batch = self.inner_do_exchange(func_name, input_batch).await;

        let request_duration = instant.elapsed();
        record_running_requests_external_finish(name, 1);
        record_request_external_duration(func_name, request_duration);

        let result_batch = result_batch?;
        let schema = DataSchema::try_from(&(*result_batch.schema()))?;
        let (result_block, result_schema) = DataBlock::from_record_batch(&schema, &result_batch)
            .map_err(|err| {
                ErrorCode::UDFDataError(format!(
                    "Cannot convert arrow record batch to data block: {err}"
                ))
            })?;

        let result_fields = result_schema.fields();
        if result_fields.is_empty() || result_block.is_empty() {
            return Err(ErrorCode::EmptyDataFromServer(
                "Get empty data from UDF Server",
            ));
        }

        if result_fields[0].data_type() != return_type {
            return Err(ErrorCode::UDFSchemaMismatch(format!(
                "UDF server return incorrect type, expected: {}, but got: {}",
                return_type,
                result_fields[0].data_type()
            )));
        }
        if result_block.num_rows() != num_rows {
            return Err(ErrorCode::UDFDataError(format!(
                "UDF server should return {} rows, but it returned {} rows",
                num_rows,
                result_block.num_rows()
            )));
        }

        if contains_variant(return_type) {
            let value = transform_variant(&result_block.get_by_offset(0).value(), false)?;
            Ok(BlockEntry::Column(value.as_column().unwrap().clone()))
        } else {
            Ok(result_block.get_by_offset(0).clone())
        }
    }

    #[async_backtrace::framed]
    async fn inner_do_exchange(
        &mut self,
        func_name: &str,
        input_batch: RecordBatch,
    ) -> Result<RecordBatch> {
        let descriptor = FlightDescriptor::new_path(vec![func_name.to_string()]);
        let batch_rows = self.batch_rows;
        let batches = (0..input_batch.num_rows())
            .step_by(batch_rows)
            .map(move |start| {
                Ok(input_batch.slice(start, batch_rows.min(input_batch.num_rows() - start)))
            });

        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(descriptor))
            .build(stream::iter(batches))
            .map(|data| data.unwrap());
        let request = self.make_request(flight_data_stream);
        let flight_data_stream = self.inner.do_exchange(request).await?.into_inner();
        let record_batch_stream = FlightRecordBatchStream::new_from_flight_data(
            flight_data_stream.map_err(|err| err.into()),
        )
        .map_err(|err| handle_flight_decode_error(func_name, err));

        let batches: Vec<RecordBatch> = record_batch_stream.try_collect().await?;
        if batches.is_empty() {
            return Err(ErrorCode::EmptyDataFromServer(format!(
                "Get empty data from UDF Server on UDF function {func_name}"
            )));
        }

        let schema = batches[0].schema();
        concat_batches(&schema, batches.iter())
            .map_err(|err| ErrorCode::UDFDataError(err.to_string()))
    }
}

fn handle_flight_decode_error(func_name: &str, err: FlightError) -> ErrorCode {
    let issue = classify_flight_error(&err);
    let err_text = err.to_string();

    match issue {
        FlightDecodeIssue::TransportInterrupted => ErrorCode::UDFDataError(format!(
            "The user-defined function \"{func_name}\" stopped responding before it finished. Retry the query; if it keeps failing, ensure the UDF server is running or review its logs. (details: {err_text})"
        )),
        FlightDecodeIssue::ServerStatus(status) => ErrorCode::UDFDataError(format!(
            "The user-defined function \"{func_name}\" reported an error: {status}. Review the UDF server logs."
        )),
        FlightDecodeIssue::SchemaMismatch => ErrorCode::UDFDataError(format!(
            "The user-defined function \"{func_name}\" returned an unexpected schema. Ensure the UDF definition matches the server output. (details: {err_text})"
        )),
        FlightDecodeIssue::MalformedData => ErrorCode::UDFDataError(format!(
            "The user-defined function \"{func_name}\" returned data that Databend could not parse. Check the UDF implementation or its logs. (details: {err_text})"
        )),
        FlightDecodeIssue::Other => ErrorCode::UDFDataError(format!(
            "Decode record batch failed on UDF function {func_name}: {err_text}"
        )),
    }
}

fn classify_flight_error(err: &FlightError) -> FlightDecodeIssue {
    match err {
        FlightError::Arrow(arrow_err) => classify_arrow_error(arrow_err),
        FlightError::Tonic(status) => classify_status(status),
        FlightError::ExternalError(source) => classify_external_error(source.as_ref()),
        FlightError::ProtocolError(_) | FlightError::DecodeError(_) => {
            FlightDecodeIssue::MalformedData
        }
        FlightError::NotYetImplemented(_) => FlightDecodeIssue::Other,
    }
}

fn classify_arrow_error(err: &ArrowError) -> FlightDecodeIssue {
    match err {
        ArrowError::SchemaError(_) => FlightDecodeIssue::SchemaMismatch,
        ArrowError::ExternalError(source) => classify_external_error(source.as_ref()),
        ArrowError::IoError(message, _) => classify_error_message(message),
        ArrowError::ParseError(_)
        | ArrowError::InvalidArgumentError(_)
        | ArrowError::ComputeError(_)
        | ArrowError::JsonError(_)
        | ArrowError::CsvError(_)
        | ArrowError::IpcError(_)
        | ArrowError::CDataInterface(_)
        | ArrowError::ParquetError(_) => FlightDecodeIssue::MalformedData,
        _ => FlightDecodeIssue::Other,
    }
}

fn classify_status(status: &Status) -> FlightDecodeIssue {
    classify_error_message(status.message())
}

fn classify_external_error(error: &(dyn StdError + Send + Sync + 'static)) -> FlightDecodeIssue {
    if let Some(flight_err) = error.downcast_ref::<FlightError>() {
        classify_flight_error(flight_err)
    } else if let Some(arrow_err) = error.downcast_ref::<ArrowError>() {
        classify_arrow_error(arrow_err)
    } else if let Some(status) = error.downcast_ref::<Status>() {
        classify_status(status)
    } else if let Some(io_error) = error.downcast_ref::<std::io::Error>() {
        classify_error_message(&io_error.to_string())
    } else {
        classify_error_message(&error.to_string())
    }
}

fn classify_error_message(message: &str) -> FlightDecodeIssue {
    if is_transport_error_message(message) {
        FlightDecodeIssue::TransportInterrupted
    } else {
        FlightDecodeIssue::ServerStatus(message.to_string())
    }
}

pub fn is_transport_error_message(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    TRANSPORT_ERROR_SNIPPETS
        .iter()
        .any(|snippet| lower.contains(snippet))
}
pub fn error_kind(message: &str) -> &str {
    let message = message.to_ascii_lowercase();
    if message.contains("timeout") || message.contains("timedout") {
        // Error(Connect, Custom)
        if message.contains("connect,") {
            "ConnectTimeout"
        } else {
            "RequestTimeout"
        }
    } else if message.contains("cannot connect") {
        "ConnectError"
    } else if message.contains("stream closed because of a broken pipe") {
        "ServerClosed"
    } else if message.contains("dns error") || message.contains("lookup address") {
        "DnsError"
    } else {
        "Other"
    }
}

#[cfg(test)]
mod tests {
    use tonic::Code;

    use super::*;

    #[test]
    fn transport_error_returns_interrupt_hint() {
        let err = handle_flight_decode_error(
            "test_udf",
            FlightError::Tonic(Box::new(Status::new(
                Code::Internal,
                "h2 protocol error: error reading a body from connection",
            ))),
        );
        let message = err.message();
        assert!(
            message.contains("stopped responding before it finished"),
            "unexpected transport hint: {message}"
        );
    }

    #[test]
    fn server_status_is_preserved() {
        let err = handle_flight_decode_error(
            "test_udf",
            FlightError::Tonic(Box::new(Status::new(
                Code::Internal,
                "remote handler returned validation error",
            ))),
        );
        let message = err.message();
        assert!(
            message.contains("reported an error: remote handler returned validation error"),
            "unexpected server status message: {message}"
        );
    }

    #[test]
    fn schema_mismatch_detected() {
        let err = handle_flight_decode_error(
            "test_udf",
            FlightError::Arrow(ArrowError::SchemaError(
                "expected Int32, got Utf8".to_string(),
            )),
        );
        let message = err.message();
        assert!(
            message.contains("returned an unexpected schema"),
            "schema mismatch hint missing: {message}"
        );
    }

    #[test]
    fn malformed_data_reported() {
        let err = handle_flight_decode_error(
            "test_udf",
            FlightError::Arrow(ArrowError::ParseError("bad payload".to_string())),
        );
        let message = err.message();
        assert!(
            message.contains("could not parse"),
            "malformed data hint missing: {message}"
        );
    }
}
