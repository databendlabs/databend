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

#![allow(non_snake_case)]

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::net::AddrParseError;
use std::string::FromUtf8Error;
use std::sync::Arc;

use backtrace::Backtrace;
use thiserror::Error;
use tonic::Code;
use tonic::Status;

pub static ABORT_SESSION: u16 = 42;
pub static ABORT_QUERY: u16 = 43;

#[derive(Clone)]
pub enum ErrorCodeBacktrace {
    Serialized(Arc<String>),
    Origin(Arc<Backtrace>),
}

impl ToString for ErrorCodeBacktrace {
    fn to_string(&self) -> String {
        match self {
            ErrorCodeBacktrace::Serialized(backtrace) => Arc::as_ref(backtrace).clone(),
            ErrorCodeBacktrace::Origin(backtrace) => {
                format!("{:?}", backtrace)
            }
        }
    }
}

#[derive(Error)]
pub struct ErrorCode {
    code: u16,
    display_text: String,
    // cause is only used to contain an `anyhow::Error`.
    // TODO: remove `cause` when we completely get rid of `anyhow::Error`.
    cause: Option<Box<dyn std::error::Error + Sync + Send>>,
    backtrace: Option<ErrorCodeBacktrace>,
}

impl ErrorCode {
    pub fn code(&self) -> u16 {
        self.code
    }

    pub fn message(&self) -> String {
        self.cause
            .as_ref()
            .map(|cause| format!("{}\n{:?}", self.display_text, cause))
            .unwrap_or_else(|| self.display_text.clone())
    }

    pub fn add_message(self, msg: impl AsRef<str>) -> Self {
        Self {
            code: self.code(),
            display_text: format!("{}\n{}", msg.as_ref(), self.display_text),
            cause: self.cause,
            backtrace: self.backtrace,
        }
    }

    pub fn add_message_back(self, msg: impl AsRef<str>) -> Self {
        Self {
            code: self.code(),
            display_text: format!("{}{}", self.display_text, msg.as_ref()),
            cause: self.cause,
            backtrace: self.backtrace,
        }
    }

    pub fn backtrace(&self) -> Option<ErrorCodeBacktrace> {
        self.backtrace.clone()
    }

    pub fn backtrace_str(&self) -> String {
        match self.backtrace.as_ref() {
            None => "".to_string(),
            Some(backtrace) => backtrace.to_string(),
        }
    }
}

macro_rules! build_exceptions {
    ($($body:ident($code:expr)),*$(,)*) => {
            impl ErrorCode {
                $(
                pub fn $body(display_text: impl Into<String>) -> ErrorCode {
                    ErrorCode {
                        code: $code,
                        display_text: display_text.into(),
                        cause: None,
                        backtrace: Some(ErrorCodeBacktrace::Origin(Arc::new(Backtrace::new()))),
                    }
                }
                paste::item! {
                    pub fn [< $body:snake _ code >] ()  -> u16{
                        $code
                    }

                    pub fn [< $body  Code >] ()  -> u16{
                        $code
                    }
                }
                )*
            }
    }
}

build_exceptions! {
    Ok(0),
    UnknownTypeOfQuery(1),
    UnImplement(2),
    UnknownDatabase(3),
    UnknownSetting(4),
    SyntaxException(5),
    BadArguments(6),
    IllegalDataType(7),
    UnknownFunction(8),
    IllegalFunctionState(9),
    BadDataValueType(10),
    UnknownPlan(11),
    IllegalPipelineState(12),
    BadTransformType(13),
    IllegalTransformConnectionState(14),
    LogicalError(15),
    EmptyData(16),
    DataStructMissMatch(17),
    BadDataArrayLength(18),
    UnknownContextID(19),
    UnknownVariable(20),
    UnknownTableFunction(21),
    BadOption(22),
    CannotReadFile(23),
    ParquetError(24),
    UnknownTable(25),
    IllegalAggregateExp(26),
    UnknownAggregateFunction(27),
    NumberArgumentsNotMatch(28),
    NotFoundStream(29),
    EmptyDataFromServer(30),
    NotFoundLocalNode(31),
    PlanScheduleError(32),
    BadPlanInputs(33),
    DuplicateClusterNode(34),
    NotFoundClusterNode(35),
    BadAddressFormat(36),
    DnsParseError(37),
    CannotConnectNode(38),
    DuplicateGetStream(39),
    Timeout(40),
    TooManyUserConnections(41),
    AbortedSession(ABORT_SESSION),
    AbortedQuery(ABORT_QUERY),
    NotFoundSession(44),
    CannotListenerPort(45),
    BadBytes(46),
    InitPrometheusFailure(47),
    ScalarSubqueryBadRows(48),
    Overflow(49),
    InvalidMetaBinaryFormat(50),
    AuthenticateFailure(51),
    TLSConfigurationFailure(52),
    UnknownSession(53),
    UnexpectedError(54),
    DateTimeParseError(55),
    BadPredicateRows(56),
    SHA1CheckFailed(57),

    // uncategorized
    UnexpectedResponseType(600),

    UnknownException(1000),
    TokioError(1001),
}

// Store errors
build_exceptions! {

    FileMetaNotFound(2001),
    FileDamaged(2002),

    // dfs node errors

    UnknownNode(2101),

    // meta service errors

    // meta service does not work.
    MetaServiceError(2201),
    // meta service is shut down.
    MetaServiceShutdown(2202),
    // meta service is unavailable for now.
    MetaServiceUnavailable(2203),

    // config errors

    InvalidConfig(2301),

    // meta store errors

    MetaStoreDamaged(2401),
    MetaStoreAlreadyExists(2402),
    MetaStoreNotFound(2403),

    ConcurrentSnapshotInstall(2404),
    IllegalSnapshot(2405),
    UnknownTableId(2406),
    TableVersionMissMatch(2407),

    // KVSrv server error

    KVSrvError(2501),

    // FS error

    IllegalFileName(2601),

    // Store server error

    DatabendStoreError(2701),

    // TODO
    // We may need to separate front-end errors from API errors (and system errors?)
    // That may depend which components are using these error codes, and for what purposes,
    // let's figure it out latter.

    // user-api error codes
    UnknownUser(3000),
    UserAlreadyExists(3001),
    IllegalUserInfoFormat(3002),

    // meta-api error codes
    DatabaseAlreadyExists(4001),
    TableAlreadyExists(4003),
    IllegalMetaOperationArgument(4004),
    IllegalSchema(4005),
    IllegalMetaState(4006),
    MetaNodeInternalError(4007),
    TruncateTableFailedError(4008),
    CommitTableError(4009),

    // namespace error.
    NamespaceUnknownNode(4058),
    NamespaceNodeAlreadyExists(4059),
    NamespaceIllegalNodeFormat(4050),

    // storage-api error codes
    ReadFileError(5001),
    BrokenChannel(5002),

    // kv-api error codes
    UnknownKey(6000),


    // DAL error
    DALTransportError(7000),
    UnknownStorageSchemeName(7001),
    SecretKeyNotSet(7002),


    // datasource error
    DuplicatedTableEngineProvider(8000),
    UnknownDatabaseEngine(8001),
    UnknownTableEngine(8002),
    DuplicatedDatabaseEngineProvider(8003),

}
// General errors
build_exceptions! {

    // A task that already stopped and can not stop twice.
    AlreadyStarted(7101),

    // A task that already started and can not start twice.
    AlreadyStopped(7102),

    // Trying to cast to a invalid type
    InvalidCast(7201),
}

pub type Result<T> = std::result::Result<T, ErrorCode>;

impl Debug for ErrorCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Code: {}, displayText = {}.",
            self.code(),
            self.message(),
        )?;

        match self.backtrace.as_ref() {
            None => Ok(()), // no backtrace
            Some(backtrace) => {
                // TODO: Custom stack frame format for print
                match backtrace {
                    ErrorCodeBacktrace::Origin(backtrace) => write!(f, "\n\n{:?}", backtrace),
                    ErrorCodeBacktrace::Serialized(backtrace) => write!(f, "\n\n{:?}", backtrace),
                }
            }
        }
    }
}

impl Display for ErrorCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Code: {}, displayText = {}.",
            self.code(),
            self.message(),
        )
    }
}

#[derive(Error)]
enum OtherErrors {
    AnyHow { error: anyhow::Error },
}

impl Display for OtherErrors {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OtherErrors::AnyHow { error } => write!(f, "{}", error),
        }
    }
}

impl Debug for OtherErrors {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OtherErrors::AnyHow { error } => write!(f, "{:?}", error),
        }
    }
}

impl From<anyhow::Error> for ErrorCode {
    fn from(error: anyhow::Error) -> Self {
        ErrorCode {
            code: 1002,
            display_text: format!("{}, source: {:?}", error, error.source()),
            cause: Some(Box::new(OtherErrors::AnyHow { error })),
            backtrace: Some(ErrorCodeBacktrace::Origin(Arc::new(Backtrace::new()))),
        }
    }
}

impl From<std::num::ParseIntError> for ErrorCode {
    fn from(error: std::num::ParseIntError) -> Self {
        ErrorCode::from_std_error(error)
    }
}

impl From<std::num::ParseFloatError> for ErrorCode {
    fn from(error: std::num::ParseFloatError) -> Self {
        ErrorCode::from_std_error(error)
    }
}

impl From<common_arrow::arrow::error::ArrowError> for ErrorCode {
    fn from(error: common_arrow::arrow::error::ArrowError) -> Self {
        ErrorCode::from_std_error(error)
    }
}

impl From<serde_json::Error> for ErrorCode {
    fn from(error: serde_json::Error) -> Self {
        ErrorCode::from_std_error(error)
    }
}

impl From<sqlparser::parser::ParserError> for ErrorCode {
    fn from(error: sqlparser::parser::ParserError) -> Self {
        ErrorCode::from_std_error(error)
    }
}

impl From<std::io::Error> for ErrorCode {
    fn from(error: std::io::Error) -> Self {
        ErrorCode::from_std_error(error)
    }
}

impl From<std::net::AddrParseError> for ErrorCode {
    fn from(error: AddrParseError) -> Self {
        ErrorCode::BadAddressFormat(format!("Bad address format, cause: {}", error))
    }
}

impl From<FromUtf8Error> for ErrorCode {
    fn from(error: FromUtf8Error) -> Self {
        ErrorCode::BadBytes(format!(
            "Bad bytes, cannot parse bytes with UTF8, cause: {}",
            error
        ))
    }
}

impl From<prost::EncodeError> for ErrorCode {
    fn from(error: prost::EncodeError) -> Self {
        ErrorCode::BadBytes(format!(
            "Bad bytes, cannot parse bytes with prost, cause: {}",
            error
        ))
    }
}

impl ErrorCode {
    pub fn from_std_error<T: std::error::Error>(error: T) -> Self {
        ErrorCode {
            code: 1002,
            display_text: format!("{}", error),
            cause: None,
            backtrace: Some(ErrorCodeBacktrace::Origin(Arc::new(Backtrace::new()))),
        }
    }

    pub fn create(
        code: u16,
        display_text: String,
        backtrace: Option<ErrorCodeBacktrace>,
    ) -> ErrorCode {
        ErrorCode {
            code,
            display_text,
            cause: None,
            backtrace,
        }
    }
}

/// Provides the `map_err_to_code` method for `Result`.
///
/// ```
/// use common_exception::ToErrorCode;
/// use common_exception::ErrorCode;
///
/// let x: std::result::Result<(), std::fmt::Error> = Err(std::fmt::Error {});
/// let y: common_exception::Result<()> =
///     x.map_err_to_code(ErrorCode::UnknownException, || 123);
///
/// assert_eq!(
///     "Code: 1000, displayText = 123, cause: an error occurred when formatting an argument.",
///     format!("{}", y.unwrap_err())
/// );
/// ```
pub trait ToErrorCode<T, E, CtxFn>
where E: Display + Send + Sync + 'static
{
    /// Wrap the error value with ErrorCode. It is lazily evaluated:
    /// only when an error does occur.
    ///
    /// `err_code_fn` is one of the ErrorCode builder function such as `ErrorCode::Ok`.
    /// `context_fn` builds display_text for the ErrorCode.
    fn map_err_to_code<ErrFn, D>(self, err_code_fn: ErrFn, context_fn: CtxFn) -> Result<T>
    where
        ErrFn: FnOnce(String) -> ErrorCode,
        D: Display,
        CtxFn: FnOnce() -> D;
}

impl<T, E, CtxFn> ToErrorCode<T, E, CtxFn> for std::result::Result<T, E>
where E: Display + Send + Sync + 'static
{
    fn map_err_to_code<ErrFn, D>(self, make_exception: ErrFn, context_fn: CtxFn) -> Result<T>
    where
        ErrFn: FnOnce(String) -> ErrorCode,
        D: Display,
        CtxFn: FnOnce() -> D,
    {
        self.map_err(|error| {
            let err_text = format!("{}, cause: {}", context_fn(), error);
            make_exception(err_text)
        })
    }
}

// ===  ser/de to/from tonic::Status ===

#[derive(serde::Serialize, serde::Deserialize)]
struct SerializedError {
    code: u16,
    message: String,
    backtrace: String,
}

impl From<&Status> for ErrorCode {
    fn from(status: &Status) -> Self {
        match status.code() {
            tonic::Code::Unknown => {
                let details = status.details();
                if details.is_empty() {
                    return ErrorCode::UnknownException(status.message());
                }
                match serde_json::from_slice::<SerializedError>(details) {
                    Err(error) => ErrorCode::from(error),
                    Ok(serialized_error) => match serialized_error.backtrace.len() {
                        0 => {
                            ErrorCode::create(serialized_error.code, serialized_error.message, None)
                        }
                        _ => ErrorCode::create(
                            serialized_error.code,
                            serialized_error.message,
                            Some(ErrorCodeBacktrace::Serialized(Arc::new(
                                serialized_error.backtrace,
                            ))),
                        ),
                    },
                }
            }
            _ => ErrorCode::UnImplement(status.to_string()),
        }
    }
}

impl From<Status> for ErrorCode {
    fn from(status: Status) -> Self {
        (&status).into()
    }
}

impl From<ErrorCode> for Status {
    fn from(err: ErrorCode) -> Self {
        let rst_json = serde_json::to_vec::<SerializedError>(&SerializedError {
            code: err.code(),
            message: err.message(),
            backtrace: {
                let mut str = err.backtrace_str();
                str.truncate(2 * 1024);
                str
            },
        });

        match rst_json {
            Ok(serialized_error_json) => {
                // Code::Internal will be used by h2, if something goes wrong internally.
                // To distinguish from that, we use Code::Unknown here
                Status::with_details(Code::Unknown, err.message(), serialized_error_json.into())
            }
            Err(error) => Status::unknown(error.to_string()),
        }
    }
}

impl Clone for ErrorCode {
    fn clone(&self) -> Self {
        ErrorCode::create(self.code(), self.message(), self.backtrace())
    }
}
