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

#![allow(non_snake_case)]

use std::sync::Arc;

use backtrace::Backtrace;

use crate::exception::ErrorCodeBacktrace;
use crate::ErrorCode;

pub static ABORT_SESSION: u16 = 42;
pub static ABORT_QUERY: u16 = 43;

macro_rules! build_exceptions {
    ($($body:ident($code:expr)),*$(,)*) => {
            impl ErrorCode {
                $(
                pub fn $body(display_text: impl Into<String>) -> ErrorCode {
                    ErrorCode::create(
                        $code,
                        display_text.into(),
                        None,
                        Some(ErrorCodeBacktrace::Origin(Arc::new(Backtrace::new())))
                    )
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

// Internal errors [0, 2000].
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
    UnknownColumn(58),
    InvalidSourceFormat(59),
    StrParseError(60),
    IllegalGrant(61),
    ProxyModePermissionDenied(62),
    PermissionDenied(63),
    UnmarshalError(64),
    SemanticError(65),

    // uncategorized
    UnexpectedResponseType(66),
    UnknownException(67),
    TokioError(68),

    // http query error
    HttpNotFound(72),

    // network error
    NetworkRequestError(73),
}

// Metasvr errors [2001, 3000].
build_exceptions! {
    // meta service does not work.
    MetaServiceError(2001),
    InvalidConfig(2002),
    UnknownNode(2003),

    // meta store errors
    MetaStoreDamaged(2004),
    MetaStoreAlreadyExists(2005),
    MetaStoreNotFound(2006),

    ConcurrentSnapshotInstall(2007),
    UnknownTableId(2008),
    TableVersionMissMatch(2009),
    UnknownDatabaseId(1010),

    // KVSrv server error
    MetaSrvError(2101),
    TransactionAbort(2102),
    TransactionError(2103),

    // user-api error codes
    UnknownUser(2201),
    UserAlreadyExists(2202),
    IllegalUserInfoFormat(2203),

    // meta-api error codes
    DatabaseAlreadyExists(2301),
    TableAlreadyExists(2302),
    IllegalMetaOperationArgument(2303),
    IllegalMetaState(2304),
    MetaNodeInternalError(2305),

    // cluster error.
    ClusterUnknownNode(2401),
    ClusterNodeAlreadyExists(2402),

    // stage error.
    UnknownStage(2501),
    StageAlreadyExists(2502),

    // user defined function error.
    IllegalUDFFormat(2601),
    UnknownUDF(2602),
    UDFAlreadyExists(2603),


    // database error.
    UnknownDatabaseEngine(2701),
    UnknownTableEngine(2702),
}

// Storage errors [3001, 4000].
build_exceptions! {
    UnknownStorageSchemeName(3001),
    SecretKeyNotSet(3002),
    DalTransportError(3003),
    DalPathNotFound(3004),
}

// Cache errors [4001, 5000].
build_exceptions! {
    DiskCacheIOError(4001),
    DiskCacheFileTooLarge(4002),
    DiskCacheFileNotInCache(4003),
}

// Service errors [5001,6000].
build_exceptions! {
    // A task that already stopped and can not stop twice.
    AlreadyStarted(5001),

    // A task that already started and can not start twice.
    AlreadyStopped(5002),
}
