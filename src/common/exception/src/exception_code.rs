// Copyright 2022 Datafuse Labs.
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

use std::backtrace::Backtrace;
use std::sync::Arc;

use crate::exception::ErrorCodeBacktrace;
use crate::ErrorCode;

pub static ABORT_SESSION: u16 = 1042;
pub static ABORT_QUERY: u16 = 1043;

macro_rules! build_exceptions {
    ($($body:ident($code:expr)),*$(,)*) => {
            impl ErrorCode {
                $(
                pub fn $body(display_text: impl Into<String>) -> ErrorCode {
                    let bt = Some(ErrorCodeBacktrace::Origin(Arc::new(Backtrace::capture())));
                    ErrorCode::create(
                        $code,
                        display_text.into(),
                        None,
                        bt,
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
    UnknownTypeOfQuery(1001),
    UnImplement(1002),
    UnknownDatabase(1003),
    UnknownDatabaseId(1004),
    SyntaxException(1005),
    BadArguments(1006),
    IllegalDataType(1007),
    UnknownFunction(1008),
    IllegalFunctionState(1009),
    BadDataValueType(1010),
    UnknownPlan(1011),
    IllegalPipelineState(1012),
    BadTransformType(1013),
    IllegalTransformConnectionState(1014),
    LogicalError(1015),
    EmptyData(1016),
    DataStructMissMatch(1017),
    BadDataArrayLength(1018),
    UnknownContextID(1019),
    UnknownTableId(1020),
    UnknownTableFunction(1021),
    BadOption(1022),
    CannotReadFile(1023),
    ParquetError(1024),
    UnknownTable(1025),
    IllegalAggregateExp(1026),
    UnknownAggregateFunction(1027),
    NumberArgumentsNotMatch(1028),
    NotFoundStream(1029),
    EmptyDataFromServer(1030),
    NotFoundLocalNode(1031),
    PlanScheduleError(1032),
    BadPlanInputs(1033),
    DuplicateClusterNode(1034),
    NotFoundClusterNode(1035),
    BadAddressFormat(1036),
    DnsParseError(1037),
    CannotConnectNode(1038),
    DuplicateGetStream(1039),
    Timeout(1040),
    TooManyUserConnections(1041),
    AbortedSession(ABORT_SESSION),
    AbortedQuery(ABORT_QUERY),
    NotFoundSession(1044),
    CannotListenerPort(1045),
    BadBytes(1046),
    InitPrometheusFailure(1047),
    ScalarSubqueryBadRows(1048),
    Overflow(1049),
    InvalidMetaBinaryFormat(1050),
    AuthenticateFailure(1051),
    TLSConfigurationFailure(1052),
    UnknownSession(1053),
    UnexpectedError(1054),
    DateTimeParseError(1055),
    BadPredicateRows(1056),
    SHA1CheckFailed(1057),
    UnknownColumn(1058),
    InvalidSourceFormat(1059),
    StrParseError(1060),
    IllegalGrant(1061),
    ManagementModePermissionDenied(1062),
    PermissionDenied(1063),
    UnmarshalError(1064),
    SemanticError(1065),

    // Uncategorized error codes.
    UnexpectedResponseType(1066),
    UnknownException(1067),
    TokioError(1068),

    // Pipeline executor error codes.
    PipelineAreadlyStarted(1069),
    PipelineNotStarted(1070),
    PipelineUnInitialized(1071),

    // Http query error codes.
    HttpNotFound(1072),

    // Network error codes.
    NetworkRequestError(1073),

    UnknownFormat(1074),
    UnknownCompressionType(1075),
    InvalidCompressionData(1076),

    InvalidAuthInfo(1077),
    InvalidTimezone(1078),
    InvalidDate(1079),
    InvalidTimestamp(1080),
    InvalidClusterKeys(1081),
    UnknownFragmentExchange(1082),

    // Tenant error codes.
    TenantIsEmpty(1101),
    IndexOutOfBounds(1102),

    // Layout error code.
    LayoutError(1103),

    PanicError(1104),

    // Async insert error codes
    AsyncInsertTimeoutError(1105),

    TableInfoError(1106),
    ReadTableDataError(1107),
}

// Metasvr errors [2001, 3000].
build_exceptions! {
    // Meta service does not work.
    MetaServiceError(2001),
    InvalidConfig(2002),
    MetaStorageError(2003),
    InvalidArgument(2004),

    TableVersionMismatched(2009),
    OCCRetryFailure(2011),
    TableNotWritable(2012),
    TableHistoricalDataNotFound(2013),

    // User api error codes.
    UnknownUser(2201),
    UserAlreadyExists(2202),
    IllegalUserInfoFormat(2203),
    UnknownRole(2204),
    IllegalUserSettingFormat(2205),
    InvalidRole(2206),

    // Meta api error codes.
    DatabaseAlreadyExists(2301),
    TableAlreadyExists(2302),
    IllegalMetaState(2304),
    MetaNodeInternalError(2305),
    ViewAlreadyExists(2306),
    CreateTableWithDropTime(2307),
    UndropTableAlreadyExists(2308),
    UndropTableHasNoHistory(2309),
    CreateDatabaseWithDropTime(2310),
    UndropDbHasNoHistory(2312),
    UndropTableWithNoDropTime(2313),
    DropTableWithDropTime(2314),
    DropDbWithDropTime(2315),
    UndropDbWithNoDropTime(2316),
    TxnRetryMaxTimes(2317),

    // Cluster error codes.
    ClusterUnknownNode(2401),
    ClusterNodeAlreadyExists(2402),

    // Stage error codes.
    UnknownStage(2501),
    StageAlreadyExists(2502),
    IllegalUserStageFormat(2503),
    StageFileAlreadyExists(2504),
    IllegalStageFileFormat(2505),

    // User defined function error codes.
    IllegalUDFFormat(2601),
    UnknownUDF(2602),
    UdfAlreadyExists(2603),

    // Database error codes.
    UnknownDatabaseEngine(2701),
    UnknownTableEngine(2702),
    UnsupportedEngineParams(2703),

    // Share error codes.
    ShareAlreadyExists(2705),
    UnknownShare(2706),
    UnknownShareId(2707),
    ShareAccountsAlreadyExists(2708),
    UnknownShareAccounts(2709),
    WrongShareObject(2710),
    WrongShare(2711),
    ShareHasNoGrantedDatabase(2712),
    ShareHasNoGrantedPrivilege(2713),

    // Variable error codes.
    UnknownVariable(2801),
    OnlySupportAsciiChars(2802),
    WrongValueForVariable(2803),

    // Tenant quota error codes.
    IllegalTenantQuotaFormat(2901),
    TenantQuotaUnknown(2902),
    TenantQuotaExceeded(2903),

}

// Storage errors [3001, 4000].
build_exceptions! {
    StorageNotFound(3001),
    StoragePermissionDenied(3002),
    StorageUnavailable(3901),
    StorageUnsupported(3902),
    StorageInsecure(3903),
    StorageOther(4000),
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
