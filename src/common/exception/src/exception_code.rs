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

#![allow(non_snake_case)]

use crate::exception_backtrace::capture;
use crate::ErrorCode;

macro_rules! build_exceptions {
    ($($(#[$meta:meta])* $body:ident($code:expr, $is_retryable:expr)),*$(,)*) => {
        impl ErrorCode {
            $(

                paste::item! {
                    $(
                        #[$meta]
                    )*
                    pub const [< $body:snake:upper >]: u16 = $code;
                }
                $(
                    #[$meta]
                )*
                pub fn $body(display_text: impl Into<String>) -> ErrorCode {
                    let bt = capture();
                    ErrorCode::create(
                        $code,
                        stringify!($body),
                        display_text.into(),
                        String::new(),
                        $is_retryable,
                        None,
                        bt,
                    )
                }
            )*
        }
    }
}

// Internal errors [0, 2000].
build_exceptions! {
    Ok(0, false),

    /// Internal means this is the internal error that no action
    /// can be taken by neither developers or users.
    /// In most of the time, they are code bugs.
    ///
    /// If there is an error that are unexpected and no other actions
    /// to taken, please use this error code.
    ///
    /// # Notes
    ///
    /// This error should never be used to for error checking. An error
    /// that returns as internal error could be assigned a separate error
    /// code at anytime.
    Internal(1001, false),

    /// Unimplemented means this is a not implemented feature.
    ///
    /// Developers could implement the feature to resolve this error at anytime,
    ///
    /// # Notes
    ///
    /// It's OK to use this error code for not implemented feature in
    /// our dependences. For example, in arrow.
    Unimplemented(1002, false),

    // Legacy error codes, we will refactor them one by one.

    UnknownDatabase(1003, false),
    UnknownDatabaseId(1004, false),
    SyntaxException(1005, false),
    BadArguments(1006, false),
    IllegalDataType(1007, false),
    UnknownFunction(1008, false),
    BadDataValueType(1010, false),
    EmptyData(1016, false),
    DataStructMissMatch(1017, false),
    BadDataArrayLength(1018, false),
    UnknownTableId(1020, false),
    UnknownTable(1025, false),
    UnknownView(1026, false),
    UnknownAggregateFunction(1027, false),
    NumberArgumentsNotMatch(1028, false),
    EmptyDataFromServer(1030, false),
    NotFoundClusterNode(1035, false),
    BadAddressFormat(1036, false),
    DnsParseError(1037, false),
    CannotConnectNode(1038, true),
    TooManyUserConnections(1041, false),
    AbortedSession(1042, false),
    AbortedQuery(1043, false),
    ClosedQuery(1044, false),
    CannotListenerPort(1045, false),
    BadBytes(1046, false),
    InitPrometheusFailure(1047, false),
    Overflow(1049, false),
    TLSConfigurationFailure(1052, false),
    UnknownSession(1053, false),
    SHA1CheckFailed(1057, false),
    UnknownColumn(1058, false),
    StrParseError(1060, false),
    IllegalGrant(1061, false),
    ManagementModePermissionDenied(1062, false),
    PermissionDenied(1063, false),
    UnmarshalError(1064, false),
    SemanticError(1065, false),
    NeedChangePasswordDenied(1066, false),
    UnknownException(1067, false),
    TokioError(1068, false),
    HttpNotFound(1072, false),
    UnknownFormat(1074, false),
    UnknownCompressionType(1075, false),
    InvalidCompressionData(1076, false),
    InvalidTimezone(1078, false),
    InvalidDate(1079, false),
    InvalidTimestamp(1080, false),
    InvalidClusterKeys(1081, false),
    UnknownFragmentExchange(1082, false),
    TenantIsEmpty(1101, false),
    IndexOutOfBounds(1102, false),
    LayoutError(1103, false),
    PanicError(1104, false),
    TableInfoError(1106, false),
    ReadTableDataError(1107, false),
    AddColumnExistError(1108, false),
    DropColumnEmptyError(1109, false),
    // create table or alter table add column with internal column name
    TableWithInternalColumnName(1110, false),
    EmptyShareEndpointConfig(1111, false),
    LicenceDenied(1112, false),
    UnknownDatamask(1113, false),
    UnmatchColumnDataType(1114, false),
    VirtualColumnNotFound(1115, false),
    VirtualColumnAlreadyExists(1116, false),
    ColumnReferencedByComputedColumn(1117, false),
    ColumnReferencedByInvertedIndex(1118, false),
    // The table is not a clustered table.
    UnclusteredTable(1118, false),
    UnknownCatalog(1119, false),
    UnknownCatalogType(1120, false),
    UnmatchMaskPolicyReturnType(1121, false),
    Timeout(1122, false),
    Outdated(1123, false),
    // sequence
    OutofSequenceRange(1124, false),
    WrongSequenceCount(1125, false),
    UnknownSequence(1126, false),
    UnknownQuery(1127, false),

    // Data Related Errors

    /// ParquetFileInvalid is used when given parquet file is invalid.
    ParquetFileInvalid(1201, false),
    /// InvalidUtf8String is used when given string is not a valid utf8 string.
    InvalidUtf8String(1202, false),

    // Table related errors starts here.

    /// TableOptionInvalid is used when users input an invalid option.
    ///
    /// For example: try to set a reserved table option.
    TableOptionInvalid(1301, false),
    /// TableEngineMismatch is used when users try to do not supported
    /// operations on specified engine.
    ///
    /// For example: drop on `view` engine.
    TableEngineNotSupported(1302, false),
    /// TableSchemaMismatch is used when table's schema is not match with input
    ///
    /// For example: try to with 3 columns into a table with 4 columns.
    TableSchemaMismatch(1303, false),

    // License related errors starts here

    /// LicenseKeyParseError is used when license key cannot be pared by the jwt public key
    ///
    /// For example: license key is not valid
    LicenseKeyParseError(1401, false),

    /// LicenseKeyInvalid is used when license key verification error occurs
    ///
    /// For example: license key is expired
    LicenseKeyInvalid(1402, false),
    EnterpriseFeatureNotEnable(1403, false),
    LicenseKeyExpired(1404, false),

    BackgroundJobAlreadyExists(1501, false),
    UnknownBackgroundJob(1502, false),

    InvalidRowIdIndex(1503, false),
    // Index related errors.
    UnsupportedIndex(1601, false),
    RefreshIndexError(1602, false),
    IndexOptionInvalid(1603, false),

    // Cloud control error codes
    CloudControlConnectError(1701, true),
    CloudControlNotEnabled(1702, false),
    IllegalCloudControlMessageFormat(1703, false),

    // Geometry errors.
    GeometryError(1801, false),
    InvalidGeometryFormat(1802, false),
    // Tantivy errors.
    TantivyError(1901, false),
    TantivyOpenReadError(1902, false),
    TantivyQueryParserError(1903, false),

    ReqwestError(1910, false)
}

// Meta service errors [2001, 3000].
build_exceptions! {
    // Meta service does not work.
    MetaServiceError(2001, false),
    InvalidConfig(2002, false),
    MetaStorageError(2003, false),
    InvalidArgument(2004, false),
    // Meta service replied with invalid data
    InvalidReply(2005, false),

    TableVersionMismatched(2009, false),
    OCCRetryFailure(2011, false),
    TableNotWritable(2012, false),
    TableHistoricalDataNotFound(2013, false),
    DuplicatedUpsertFiles(2014, false),
    TableAlreadyLocked(2015, false),
    TableLockExpired(2016, false),

    // User api error codes.
    UnknownUser(2201, false),
    UserAlreadyExists(2202, false),
    IllegalUserInfoFormat(2203, false),
    UnknownRole(2204, false),
    InvalidRole(2206, false),
    UnknownNetworkPolicy(2207, false),
    NetworkPolicyAlreadyExists(2208, false),
    IllegalNetworkPolicy(2209, false),
    NetworkPolicyIsUsedByUser(2210, false),
    UnknownPasswordPolicy(2211, false),
    PasswordPolicyAlreadyExists(2212, false),
    IllegalPasswordPolicy(2213, false),
    PasswordPolicyIsUsedByUser(2214, false),
    InvalidPassword(2215, false),
    RoleAlreadyExists(2216, false),
    IllegalRole(2217, false),
    IllegalUser(2218, false),

    // Meta api error codes.
    DatabaseAlreadyExists(2301, false),
    TableAlreadyExists(2302, false),
    ViewAlreadyExists(2306, false),
    CreateTableWithDropTime(2307, false),
    UndropTableAlreadyExists(2308, false),
    UndropTableHasNoHistory(2309, false),
    CreateDatabaseWithDropTime(2310, false),
    UndropDbHasNoHistory(2312, false),
    UndropTableWithNoDropTime(2313, false),
    DropTableWithDropTime(2314, false),
    DropDbWithDropTime(2315, false),
    UndropDbWithNoDropTime(2316, false),
    TxnRetryMaxTimes(2317, false),
    /// `CatalogNotSupported` should be raised when defining a catalog, which is:
    /// - not supported by the application, like creating a `HIVE` catalog but `HIVE` feature not enabled;
    /// - forbidden to create, like creating a `DEFAULT` catalog
    CatalogNotSupported(2318, false),
    /// `CatalogAlreadyExists` should be raised when defining a catalog, which is:
    /// - having the same name as a already exist, like `default`
    /// - and without `IF NOT EXISTS`
    CatalogAlreadyExists(2319, false),
    /// `CatalogNotFound` should be raised when trying to drop a catalog that is:
    /// - not exists.
    /// - and without `IF EXISTS`
    CatalogNotFound(2320, false),
    /// data mask error codes
    DatamaskAlreadyExists(2321, false),

    CommitTableMetaError(2322, false),
    CreateAsDropTableWithoutDropTime(2323, false),


    // Cluster error codes.
    ClusterUnknownNode(2401, false),
    ClusterNodeAlreadyExists(2402, false),

    // Stage error codes.
    UnknownStage(2501, false),
    StageAlreadyExists(2502, false),
    IllegalUserStageFormat(2503, false),
    StageFileAlreadyExists(2504, false),
    IllegalStageFileFormat(2505, false),
    StagePermissionDenied(2506, false),

    // FileFormat error codes.
    UnknownFileFormat(2507, false),
    IllegalFileFormat(2508, false),
    FileFormatAlreadyExists(2509, false),

    // Connection error codes.
    UnknownConnection(2510, false),
    IllegalConnection(2511, false),
    ConnectionAlreadyExists(2512, false),

    // User defined function error codes.
    IllegalUDFFormat(2601, false),
    UnknownUDF(2602, false),
    UdfAlreadyExists(2603, false),
    UDFServerConnectError(2604, true),
    UDFSchemaMismatch(2605, false),
    UnsupportedDataType(2606, false),
    UDFDataError(2607, false),

    // Database error codes.
    UnknownDatabaseEngine(2701, false),
    UnknownTableEngine(2702, false),
    UnsupportedEngineParams(2703, false),

    // Share error codes.
    ShareAlreadyExists(2705, false),
    UnknownShare(2706, false),
    UnknownShareId(2707, false),
    ShareAccountsAlreadyExists(2708, false),
    UnknownShareAccounts(2709, false),
    WrongShareObject(2710, false),
    WrongShare(2711, false),
    ShareHasNoGrantedDatabase(2712, false),
    ShareHasNoGrantedPrivilege(2713, false),
    ShareEndpointAlreadyExists(2714, false),
    UnknownShareEndpoint(2715, false),
    UnknownShareEndpointId(2716, false),
    CannotAccessShareTable(2717, false),
    CannotShareDatabaseCreatedFromShare(2718, false),
    ShareStorageError(2719, false),

    // Index error codes.
    CreateIndexWithDropTime(2720, false),
    IndexAlreadyExists(2721, false),
    UnknownIndex(2722, false),
    DropIndexWithDropTime(2723, false),
    GetIndexWithDropTime(2724, false),
    DuplicatedIndexColumnId(2725, false),
    IndexColumnIdNotFound(2726, false),

    // Stream error codes.
    UnknownStream(2730, false),
    UnknownStreamId(2731, false),
    StreamAlreadyExists(2732, false),
    IllegalStream(2733, false),
    StreamVersionMismatched(2734, false),
    WithOptionInvalid(2735, false),

    // dynamic error codes.
    IllegalDynamicTable(2740, false),

    // Variable error codes.
    UnknownVariable(2801, false),
    OnlySupportAsciiChars(2802, false),
    WrongValueForVariable(2803, false),

    // Tenant quota error codes.
    IllegalTenantQuotaFormat(2901, false),
    TenantQuotaUnknown(2902, false),
    TenantQuotaExceeded(2903, false),

    // Script error codes.
    ScriptSemanticError(3001, false),
    ScriptExecutionError(3002, false),

    // sequence
    SequenceError(3101, false),

    // Share error codes(continue, false).
    ErrorShareEndpointCredential(3111, false),
    WrongSharePrivileges(3112, false),

    // dictionary
    DictionaryAlreadyExists(3113, false),
    UnknownDictionary(3114, false),
    DictionarySourceError(3115, false),
    // Procedure
    UnknownProcedure(3130, false),
    ProcedureAlreadyExists(3131, false),
    IllegalProcedureFormat(3132, false),
}

// Storage errors [3001, 4000].
build_exceptions! {
    StorageNotFound(3001, false),
    StoragePermissionDenied(3002, false),
    StorageUnavailable(3901, false),
    StorageUnsupported(3902, false),
    StorageInsecure(3903, false),
    DeprecatedIndexFormat(3904, false),
    InvalidOperation(3905, false),
    StorageOther(4000, false),
    UnresolvableConflict(4001, false),

    // transaction error codes
    CurrentTransactionIsAborted(4002, false),
    TransactionTimeout(4003, false),
    InvalidSessionState(4004, false),

    // recluster error codes
    NoNeedToRecluster(4011, false),
    NoNeedToCompact(4012, false),

    RefreshTableInfoFailure(4012, false),
}

// Service errors [5001,6000].
build_exceptions! {
    // A task that already stopped and can not stop twice.
    AlreadyStopped(5002, false),

    // auth related
    AuthenticateFailure(5100, false),
    // the flowing 4 code is used by clients
    SessionTokenExpired(5101, false),
    RefreshTokenExpired(5102, false),
    SessionTokenNotFound(5103, false),
    RefreshTokenNotFound(5104, false)
}
