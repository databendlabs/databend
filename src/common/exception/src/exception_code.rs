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
    ($($(#[$meta:meta])* $body:ident($code:expr)),*$(,)*) => {
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
                        None,
                        bt,
                    )
                }
            )*
        }
    }
}

// Core System Errors [0-1000]
build_exceptions! {
    /// Success code
    Ok(0),
    /// Internal system error
    Internal(1001),
    /// Feature not implemented
    Unimplemented(1002),
}

// Database and Table Access Errors [1003-1004, 1020, 1025-1026, 1058, 1119-1120, 2318-2320]
build_exceptions! {
    /// Database not found
    UnknownDatabase(1003),
    /// Database ID not found
    UnknownDatabaseId(1004),
    /// Table ID not found
    UnknownTableId(1020),
    /// Table not found
    UnknownTable(1025),
    /// View not found
    UnknownView(1026),
    /// Column not found
    UnknownColumn(1058),
    /// Unknown catalog
    UnknownCatalog(1119),
    /// Unknown catalog type
    UnknownCatalogType(1120),
    /// Catalog not supported
    CatalogNotSupported(2318),
    /// Catalog already exists
    CatalogAlreadyExists(2319),
    /// Catalog not found
    CatalogNotFound(2320),
}

// Syntax and Semantic Errors [1005-1010, 1027-1028, 1065]
build_exceptions! {
    /// Syntax error in query
    SyntaxException(1005),
    /// Invalid arguments
    BadArguments(1006),
    /// Illegal data type
    IllegalDataType(1007),
    /// Unknown function
    UnknownFunction(1008),
    /// Bad data value type
    BadDataValueType(1010),
    /// Unknown aggregate function
    UnknownAggregateFunction(1027),
    /// Number of arguments doesn't match
    NumberArgumentsNotMatch(1028),
    /// Semantic error
    SemanticError(1065),
}

// Data Structure Errors [1016-1018, 1030, 1114]
build_exceptions! {
    /// Empty data
    EmptyData(1016),
    /// Data structure mismatch
    DataStructMissMatch(1017),
    /// Bad data array length
    BadDataArrayLength(1018),
    /// Empty data from server
    EmptyDataFromServer(1030),
    /// Column data type mismatch
    UnmatchColumnDataType(1114),
}

// Network and Communication Errors [1036-1038]
build_exceptions! {
    /// Bad address format
    BadAddressFormat(1036),
    /// DNS parse error
    DnsParseError(1037),
    /// Cannot connect to node
    CannotConnectNode(1038),
}

// Session and Query Errors [1041-1044, 1053, 1127]
build_exceptions! {
    /// Too many connections for user
    TooManyUserConnections(1041),
    /// Session was aborted
    AbortedSession(1042),
    /// Query was aborted
    AbortedQuery(1043),
    /// Query was closed
    ClosedQuery(1044),
    /// Session not found
    UnknownSession(1053),
    /// Query not found
    UnknownQuery(1127),
}

// Internal System Errors [1000-1100, 1104, 1122-1123]
build_exceptions! {
    /// Failed to initialize Prometheus
    InitPrometheusFailure(1047),
    /// Numeric overflow
    Overflow(1049),
    /// Panic occurred
    PanicError(1104),
    /// Operation timed out
    Timeout(1122),
    /// Data is outdated
    Outdated(1123),
    /// Unknown exception
    UnknownException(1067),
    /// Tokio runtime error
    TokioError(1068),
}

// Permission and Security Errors [1052, 1061-1063, 1066, 2506]
build_exceptions! {
    /// TLS configuration failure
    TLSConfigurationFailure(1052),
    /// Illegal grant
    IllegalGrant(1061),
    /// Management mode permission denied
    ManagementModePermissionDenied(1062),
    /// Permission denied
    PermissionDenied(1063),
    /// Need to change password
    NeedChangePasswordDenied(1066),
    /// Stage permission denied
    StagePermissionDenied(2506),
}

// Data Format and Parsing Errors [1046, 1057, 1060, 1064, 1072, 1074-1081, 1090, 1201-1202, 2507-2509]
build_exceptions! {
    /// Bad bytes
    BadBytes(1046),
    /// SHA1 check failed
    SHA1CheckFailed(1057),
    /// String parsing error
    StrParseError(1060),
    /// Unmarshalling error
    UnmarshalError(1064),
    /// HTTP resource not found
    HttpNotFound(1072),
    /// Unknown format
    UnknownFormat(1074),
    /// Unknown compression type
    UnknownCompressionType(1075),
    /// Invalid compression data
    InvalidCompressionData(1076),
    /// Invalid timezone
    InvalidTimezone(1078),
    /// Invalid date
    InvalidDate(1079),
    /// Invalid timestamp
    InvalidTimestamp(1080),
    /// Invalid cluster keys
    InvalidClusterKeys(1081),
    /// Geometry error
    GeometryError(1090),
    /// Parquet file is invalid
    ParquetFileInvalid(1201),
    /// Invalid UTF-8 string
    InvalidUtf8String(1202),
    /// Unknown file format
    UnknownFileFormat(2507),
    /// Illegal file format
    IllegalFileFormat(2508),
    /// File format already exists
    FileFormatAlreadyExists(2509),
}

// Cluster and Resource Management Errors [1035, 1045, 1082, 1101, 2401-2410, 4013]
build_exceptions! {
    /// Cluster node not found
    NotFoundClusterNode(1035),
    /// Cannot listen on port
    CannotListenerPort(1045),
    /// Unknown fragment exchange
    UnknownFragmentExchange(1082),
    /// Tenant is empty
    TenantIsEmpty(1101),
    /// Cluster unknown node
    ClusterUnknownNode(2401),
    /// Cluster node already exists
    ClusterNodeAlreadyExists(2402),
    /// Invalid warehouse
    InvalidWarehouse(2403),
    /// No resources available
    NoResourcesAvailable(2404),
    /// Warehouse already exists
    WarehouseAlreadyExists(2405),
    /// Unknown warehouse
    UnknownWarehouse(2406),
    /// Warehouse operate conflict
    WarehouseOperateConflict(2407),
    /// Empty nodes for warehouse
    EmptyNodesForWarehouse(2408),
    /// Warehouse cluster already exists
    WarehouseClusterAlreadyExists(2409),
    /// Warehouse cluster not exists
    WarehouseClusterNotExists(2410),
    /// Unsupported cluster type
    UnsupportedClusterType(4013),
}

// Table Structure and Operation Errors [1102-1103, 1106-1111, 1113-1118, 1121-1122, 1130-1133]
build_exceptions! {
    /// Index out of bounds
    IndexOutOfBounds(1102),
    /// Layout error
    LayoutError(1103),
    /// Table info error
    TableInfoError(1106),
    /// Read table data error
    ReadTableDataError(1107),
    /// Add column exists error
    AddColumnExistError(1108),
    /// Drop column empty error
    DropColumnEmptyError(1109),
    /// Table with internal column name
    TableWithInternalColumnName(1110),
    /// License denied
    LicenceDenied(1112),
    /// Unknown datamask
    UnknownDatamask(1113),
    /// Virtual column error
    VirtualColumnError(1115),
    /// Virtual column already exists
    VirtualColumnAlreadyExists(1116),
    /// Column referenced by computed column
    ColumnReferencedByComputedColumn(1117),
    /// Unclustered table
    UnclusteredTable(1118),
    /// Unmatch mask policy return type
    UnmatchMaskPolicyReturnType(1121),
    /// Empty share endpoint config
    EmptyShareEndpointConfig(1130),
    /// Unknown row policy
    UnknownRowAccessPolicy(1131),
    /// Alter Table error
    AlterTableError(1132),
    /// Constraint error
    ConstraintError(1133),
    /// Unknown row policy
    UnknownMaskPolicy(1134),
}

// Sequence Errors [1124-1126, 3101-3102]
build_exceptions! {
    /// Out of sequence range
    OutofSequenceRange(1124),
    /// Wrong sequence count
    WrongSequenceCount(1125),
    /// Unknown sequence
    UnknownSequence(1126),
    /// Sequence error
    SequenceError(3101),
    /// AutoIncrement error
    AutoIncrementError(3102),
}

// Virtual Column Errors [1128-1129]
build_exceptions! {
    /// Virtual column ID out of bound
    VirtualColumnIdOutBound(1129),
    /// Too many virtual columns
    VirtualColumnTooMany(1128),
}

// Table Engine Errors [1301-1303, 2701-2703]
build_exceptions! {
    /// Table option is invalid
    TableOptionInvalid(1301),
    /// Table engine not supported
    TableEngineNotSupported(1302),
    /// Table schema mismatch
    TableSchemaMismatch(1303),
    /// Unknown database engine
    UnknownDatabaseEngine(2701),
    /// Unknown table engine
    UnknownTableEngine(2702),
    /// Unsupported engine params
    UnsupportedEngineParams(2703),
}

// License Errors [1401-1404]
build_exceptions! {
    /// License key parse error
    LicenseKeyParseError(1401),
    /// License key is invalid
    LicenseKeyInvalid(1402),
    /// Enterprise feature not enabled
    EnterpriseFeatureNotEnable(1403),
    /// License key expired
    LicenseKeyExpired(1404),
}

// Index Errors [1503, 1601-1603, 2720-2726]
build_exceptions! {
    /// Column referenced by inverted index
    ColumnReferencedByInvertedIndex(1111),
    /// Invalid row ID index
    InvalidRowIdIndex(1503),
    /// Unsupported index
    UnsupportedIndex(1601),
    /// Refresh index error
    RefreshIndexError(1602),
    /// Index option invalid
    IndexOptionInvalid(1603),
    /// Create index with drop time
    CreateIndexWithDropTime(2720),
    /// Index already exists
    IndexAlreadyExists(2721),
    /// Unknown index
    UnknownIndex(2722),
    /// Drop index with drop time
    DropIndexWithDropTime(2723),
    /// Get index with drop time
    GetIndexWithDropTime(2724),
    /// Duplicated index column ID
    DuplicatedIndexColumnId(2725),
    /// Index column ID not found
    IndexColumnIdNotFound(2726),
}

// Cloud and Integration Errors [1701-1703]
build_exceptions! {
    /// Cloud control connect error
    CloudControlConnectError(1701),
    /// Cloud control not enabled
    CloudControlNotEnabled(1702),
    /// Illegal cloud control message format
    IllegalCloudControlMessageFormat(1703),
}

// UDF and Extension Errors [1810, 2601-2607]
build_exceptions! {
    /// UDF runtime error
    UDFRuntimeError(1810),
    /// Illegal UDF format
    IllegalUDFFormat(2601),
    /// Unknown UDF
    UnknownUDF(2602),
    /// Udf already exists
    UdfAlreadyExists(2603),
    /// UDF server connect error
    UDFServerConnectError(2604),
    /// UDF schema mismatch
    UDFSchemaMismatch(2605),
    /// Unsupported data type
    UnsupportedDataType(2606),
    /// UDF data error
    UDFDataError(2607),
}

// Task Errors [2611-2614]
build_exceptions! {
    /// Unknown Task
    UnknownTask(2611),
    /// Task already exists
    TaskAlreadyExists(2612),
    /// Task timezone invalid
    TaskTimezoneInvalid(2613),
    /// Task cron invalid
    TaskCronInvalid(2614),
    /// Task schedule and after conflict
    TaskScheduleAndAfterConflict(2615),
    /// Task when condition not met
    TaskWhenConditionNotMet(2616),
}

// Search and External Service Errors [1901-1903, 1910]
build_exceptions! {
    /// Tantivy error
    TantivyError(1901),
    /// Tantivy open read error
    TantivyOpenReadError(1902),
    /// Tantivy query parser error
    TantivyQueryParserError(1903),
    /// Reqwest error
    ReqwestError(1910),
}

// Meta Service Core Errors [2001-2005, 2009, 2011-2016]
build_exceptions! {
    /// Meta service error
    MetaServiceError(2001),
    /// Invalid configuration
    InvalidConfig(2002),
    /// Meta storage error
    MetaStorageError(2003),
    /// Invalid argument
    InvalidArgument(2004),
    /// Invalid reply
    InvalidReply(2005),
    /// Table version mismatched
    TableVersionMismatched(2009),
    /// OCC retry failure
    OCCRetryFailure(2011),
    /// Table not writable
    TableNotWritable(2012),
    /// Table historical data not found
    TableHistoricalDataNotFound(2013),
    /// Duplicated upsert files
    DuplicatedUpsertFiles(2014),
    /// Table already locked
    TableAlreadyLocked(2015),
    /// Table lock expired
    TableLockExpired(2016),
}

// User and Role Management Errors [2201-2218]
build_exceptions! {
    /// Unknown user
    UnknownUser(2201),
    /// User already exists
    UserAlreadyExists(2202),
    /// Illegal user info format
    IllegalUserInfoFormat(2203),
    /// Unknown role
    UnknownRole(2204),
    /// Invalid role
    InvalidRole(2206),
    /// Unknown network policy
    UnknownNetworkPolicy(2207),
    /// Network policy already exists
    NetworkPolicyAlreadyExists(2208),
    /// Illegal network policy
    IllegalNetworkPolicy(2209),
    /// Network policy is used by user
    NetworkPolicyIsUsedByUser(2210),
    /// Unknown password policy
    UnknownPasswordPolicy(2211),
    /// Password policy already exists
    PasswordPolicyAlreadyExists(2212),
    /// Illegal password policy
    IllegalPasswordPolicy(2213),
    /// Password policy is used by user
    PasswordPolicyIsUsedByUser(2214),
    /// Invalid password
    InvalidPassword(2215),
    /// Role already exists
    RoleAlreadyExists(2216),
    /// Illegal role
    IllegalRole(2217),
    /// Illegal user
    IllegalUser(2218),
}

// Database and Catalog Management Errors [2301-2317, 2321-2323]
build_exceptions! {
    /// Database already exists
    DatabaseAlreadyExists(2301),
    /// Table already exists
    TableAlreadyExists(2302),
    /// View already exists
    ViewAlreadyExists(2306),
    /// Create table with drop time
    CreateTableWithDropTime(2307),
    /// Undrop table already exists
    UndropTableAlreadyExists(2308),
    /// Undrop table has no history
    UndropTableHasNoHistory(2309),
    /// Create database with drop time
    CreateDatabaseWithDropTime(2310),
    /// Undrop database has no history
    UndropDbHasNoHistory(2312),
    /// Undrop table with no drop time
    UndropTableWithNoDropTime(2313),
    /// Drop table with drop time
    DropTableWithDropTime(2314),
    /// Drop database with drop time
    DropDbWithDropTime(2315),
    /// Undrop database with no drop time
    UndropDbWithNoDropTime(2316),
    /// Transaction retry max times
    TxnRetryMaxTimes(2317),
    /// Datamask already exists
    DatamaskAlreadyExists(2321),
    /// Commit table meta error
    CommitTableMetaError(2322),
    /// Create as drop table without drop time
    CreateAsDropTableWithoutDropTime(2323),
    /// Row Policy already exists
    RowAccessPolicyAlreadyExists(2324),
    /// General failures met while garbage collecting database meta
    GeneralDbGcFailure(2325),
}

// Stage and Connection Errors [2501-2505, 2510-2512]
build_exceptions! {
    /// Unknown stage
    UnknownStage(2501),
    /// Stage already exists
    StageAlreadyExists(2502),
    /// Illegal user stage format
    IllegalUserStageFormat(2503),
    /// Stage file already exists
    StageFileAlreadyExists(2504),
    /// Illegal stage file format
    IllegalStageFileFormat(2505),
    /// Unknown connection
    UnknownConnection(2510),
    /// Illegal connection
    IllegalConnection(2511),
    /// Connection already exists
    ConnectionAlreadyExists(2512),
}

// Stream and Dynamic Table Errors [2730-2735, 2740]
build_exceptions! {
    /// Unknown stream
    UnknownStream(2730),
    /// Unknown stream ID
    UnknownStreamId(2731),
    /// Stream already exists
    StreamAlreadyExists(2732),
    /// Illegal stream
    IllegalStream(2733),
    /// Stream version mismatched
    StreamVersionMismatched(2734),
    /// With option invalid
    WithOptionInvalid(2735),
    /// Illegal dynamic table
    IllegalDynamicTable(2740),
}

// Sharing and Collaboration Errors [2705-2719, 3111-3112]
build_exceptions! {
    /// Share already exists
    ShareAlreadyExists(2705),
    /// Unknown share
    UnknownShare(2706),
    /// Unknown share ID
    UnknownShareId(2707),
    /// Share accounts already exists
    ShareAccountsAlreadyExists(2708),
    /// Unknown share accounts
    UnknownShareAccounts(2709),
    /// Wrong share object
    WrongShareObject(2710),
    /// Wrong share
    WrongShare(2711),
    /// Share has no granted database
    ShareHasNoGrantedDatabase(2712),
    /// Share has no granted privilege
    ShareHasNoGrantedPrivilege(2713),
    /// Share endpoint already exists
    ShareEndpointAlreadyExists(2714),
    /// Unknown share endpoint
    UnknownShareEndpoint(2715),
    /// Unknown share endpoint ID
    UnknownShareEndpointId(2716),
    /// Cannot access share table
    CannotAccessShareTable(2717),
    /// Cannot share database created from share
    CannotShareDatabaseCreatedFromShare(2718),
    /// Share storage error
    ShareStorageError(2719),
    /// Error share endpoint credential
    ErrorShareEndpointCredential(3111),
    /// Wrong share privileges
    WrongSharePrivileges(3112),
}

// Variable and Configuration Errors [2801-2803]
build_exceptions! {
    /// Unknown variable
    UnknownVariable(2801),
    /// Only support ASCII chars
    OnlySupportAsciiChars(2802),
    /// Wrong value for variable
    WrongValueForVariable(2803),
}

// Tenant and Quota Errors [2901-2903]
build_exceptions! {
    /// Illegal tenant quota format
    IllegalTenantQuotaFormat(2901),
    /// Tenant quota unknown
    TenantQuotaUnknown(2902),
    /// Tenant quota exceeded
    TenantQuotaExceeded(2903),
}

// Script and Procedure Errors [3128-3132]
build_exceptions! {
    /// Script semantic error
    ScriptSemanticError(3128),
    /// Script execution error
    ScriptExecutionError(3129),
    /// Unknown procedure
    UnknownProcedure(3130),
    /// Procedure already exists
    ProcedureAlreadyExists(3131),
    /// Illegal procedure format
    IllegalProcedureFormat(3132),
}

// Storage and I/O Errors [3001-3002, 3901-3905, 4000]
build_exceptions! {
    /// Storage not found
    StorageNotFound(3001),
    /// Storage permission denied
    StoragePermissionDenied(3002),
    /// Storage unavailable
    StorageUnavailable(3901),
    /// Storage unsupported
    StorageUnsupported(3902),
    /// Storage insecure
    StorageInsecure(3903),
    /// Deprecated index format
    DeprecatedIndexFormat(3904),
    /// Invalid operation
    InvalidOperation(3905),
    /// Storage other error
    StorageOther(4000),
}

// Dictionary Errors [3113-3115]
build_exceptions! {
    /// Dictionary already exists
    DictionaryAlreadyExists(3113),
    /// Unknown dictionary
    UnknownDictionary(3114),
    /// Dictionary source error
    DictionarySourceError(3115),
}

// Workload Management Errors [3140-3143]
build_exceptions! {
    /// Invalid workload
    InvalidWorkload(3140),
    /// Already exists workload
    AlreadyExistsWorkload(3141),
    /// Unknown workload
    UnknownWorkload(3142),
    /// Workload operate conflict
    WorkloadOperateConflict(3143),
    UnknownWorkloadQuotas(3144),
}

// Transaction and Processing Errors [4001-4004, 4012]
build_exceptions! {
    /// Unresolvable conflict
    UnresolvableConflict(4001),
    /// Current transaction is aborted
    CurrentTransactionIsAborted(4002),
    /// Transaction timeout
    TransactionTimeout(4003),
    /// No need to compact
    NoNeedToCompact(4012),
}

// Service Status Errors [5002]
build_exceptions! {
    /// Already stopped
    AlreadyStopped(5002),
}

// Service and Authentication Errors [5100-5104]
build_exceptions! {
    /// Authentication failure
    AuthenticateFailure(5100),
    /// Session token expired
    SessionTokenExpired(5101),
    /// Refresh token expired
    RefreshTokenExpired(5102),
    /// Session token not found
    SessionTokenNotFound(5103),
    /// Refresh token not found
    RefreshTokenNotFound(5104),
}

// Client Session Errors [5110-5115]
build_exceptions! {
    /// Session Idle too long, only used for worksheet for now
    SessionTimeout(5110),
    /// Server side state lost, mainly because server restarted
    SessionLost(5111),
    /// Unexpected session state, maybe bug of client or server
    InvalidSessionState(5112),
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    #[test]
    fn test_error_codes_unique() {
        let text = include_str!("exception_code.rs");

        let mut map: HashMap<u32, Vec<String>> = HashMap::new();

        for line in text.lines() {
            let trimmed = line.trim();
            if let Some(idx1) = trimmed.find('(') {
                if let Some(idx2) = trimmed[idx1..].find(')') {
                    let name = trimmed[..idx1].trim();
                    let code_str = &trimmed[idx1 + 1..idx1 + idx2].trim();
                    if !name.is_empty()
                        && !code_str.is_empty()
                        && code_str.chars().all(|c| c.is_ascii_digit())
                    {
                        let code: u32 = code_str.parse().unwrap();
                        map.entry(code).or_default().push(name.to_string());
                    }
                }
            }
        }

        let dups: Vec<_> = map.iter().filter(|(_, names)| names.len() > 1).collect();
        if !dups.is_empty() {
            for (code, names) in &dups {
                eprintln!("Duplicate error code {} used by: {:?}", code, names);
            }
            panic!("Found duplicate error codes!");
        }
    }
}
