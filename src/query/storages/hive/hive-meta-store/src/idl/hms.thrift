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

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#
# Thrift Service that the MetaStore is built on
#

namespace java org.apache.hadoop.hive.metastore.api
namespace php metastore
namespace cpp Apache.Hadoop.Hive

const string DDL_TIME = "transient_lastDdlTime"

struct Version {
  1: string version,
  2: string comments
}

struct FieldSchema {
  1: string name, // name of the field
  2: string type, // type of the field. primitive types defined above, specify list<TYPE_NAME>, map<TYPE_NAME, TYPE_NAME> for lists & maps
  3: string comment
}

struct SQLPrimaryKey {
  1: string table_db,    // table schema
  2: string table_name,  // table name
  3: string column_name, // column name
  4: i32 key_seq,        // sequence number within primary key
  5: string pk_name,     // primary key name
  6: bool enable_cstr,   // Enable/Disable
  7: bool validate_cstr, // Validate/No validate
  8: bool rely_cstr,     // Rely/No Rely
  9: optional string catName
}

struct SQLForeignKey {
  1: string pktable_db,    // primary key table schema
  2: string pktable_name,  // primary key table name
  3: string pkcolumn_name, // primary key column name
  4: string fktable_db,    // foreign key table schema
  5: string fktable_name,  // foreign key table name
  6: string fkcolumn_name, // foreign key column name
  7: i32 key_seq,          // sequence within foreign key
  8: i32 update_rule,      // what happens to foreign key when parent key is updated
  9: i32 delete_rule,      // what happens to foreign key when parent key is deleted
  10: string fk_name,      // foreign key name
  11: string pk_name,      // primary key name
  12: bool enable_cstr,    // Enable/Disable
  13: bool validate_cstr,  // Validate/No validate
  14: bool rely_cstr,      // Rely/No Rely
  15: optional string catName
}

struct SQLUniqueConstraint {
  1: string catName,     // table catalog
  2: string table_db,    // table schema
  3: string table_name,  // table name
  4: string column_name, // column name
  5: i32 key_seq,        // sequence number within unique constraint
  6: string uk_name,     // unique key name
  7: bool enable_cstr,   // Enable/Disable
  8: bool validate_cstr, // Validate/No validate
  9: bool rely_cstr,     // Rely/No Rely
}

struct SQLNotNullConstraint {
  1: string catName,     // table catalog
  2: string table_db,    // table schema
  3: string table_name,  // table name
  4: string column_name, // column name
  5: string nn_name,     // not null name
  6: bool enable_cstr,   // Enable/Disable
  7: bool validate_cstr, // Validate/No validate
  8: bool rely_cstr,     // Rely/No Rely
}

struct SQLDefaultConstraint {
  1: string catName,     // catalog name
  2: string table_db,    // table schema
  3: string table_name,  // table name
  4: string column_name, // column name
  5: string default_value,// default value
  6: string dc_name,     // default name
  7: bool enable_cstr,   // Enable/Disable
  8: bool validate_cstr, // Validate/No validate
  9: bool rely_cstr      // Rely/No Rely
}

struct SQLCheckConstraint {
  1: string catName,     // catalog name
  2: string table_db,    // table schema
  3: string table_name,  // table name
  4: string column_name, // column name
  5: string check_expression,// check expression
  6: string dc_name,     // default name
  7: bool enable_cstr,   // Enable/Disable
  8: bool validate_cstr, // Validate/No validate
  9: bool rely_cstr      // Rely/No Rely
}

struct Type {
  1: string          name,             // one of the types in PrimitiveTypes or CollectionTypes or User defined types
  2: optional string type1,            // object type if the name is 'list' (LIST_TYPE), key type if the name is 'map' (MAP_TYPE)
  3: optional string type2,            // val type if the name is 'map' (MAP_TYPE)
  4: optional list<FieldSchema> fields // if the name is one of the user defined types
}

enum HiveObjectType {
  GLOBAL = 1,
  DATABASE = 2,
  TABLE = 3,
  PARTITION = 4,
  COLUMN = 5,
}

enum PrincipalType {
  USER = 1,
  ROLE = 2,
  GROUP = 3,
}

const string HIVE_FILTER_FIELD_OWNER = "hive_filter_field_owner__"
const string HIVE_FILTER_FIELD_PARAMS = "hive_filter_field_params__"
const string HIVE_FILTER_FIELD_LAST_ACCESS = "hive_filter_field_last_access__"

enum PartitionEventType {
  LOAD_DONE = 1,
}

// Enums for transaction and lock management 
enum TxnState {
    COMMITTED = 1,
    ABORTED = 2,
    OPEN = 3,
}

enum LockLevel {
    DB = 1,
    TABLE = 2,
    PARTITION = 3,
}

enum LockState {
    ACQUIRED = 1,       // requester has the lock
    WAITING = 2,        // requester is waiting for the lock and should call checklock at a later point to see if the lock has been obtained.
    ABORT = 3,          // the lock has been aborted, most likely due to timeout
    NOT_ACQUIRED = 4,   // returned only with lockNoWait, indicates the lock was not available and was not acquired
}

enum LockType {
    SHARED_READ = 1,
    SHARED_WRITE = 2,
    EXCLUSIVE = 3,
}

enum CompactionType {
    MINOR = 1,
    MAJOR = 2,
}

enum GrantRevokeType {
    GRANT = 1,
    REVOKE = 2,
}

enum DataOperationType {
    SELECT = 1,
    INSERT = 2
    UPDATE = 3,
    DELETE = 4,
    UNSET = 5,//this is the default to distinguish from NULL from old clients
    NO_TXN = 6,//drop table, insert overwrite, etc - something non-transactional
}

// Types of events the client can request that the metastore fire.  For now just support DML operations, as the metastore knows
// about DDL operations and there's no reason for the client to request such an event.
enum EventRequestType {
    INSERT = 1,
    UPDATE = 2,
    DELETE = 3,
}

enum SerdeType {
  HIVE = 1,
  SCHEMA_REGISTRY = 2,
}

enum SchemaType {
  HIVE = 1,
  AVRO = 2,
}

enum SchemaCompatibility {
  NONE = 1,
  BACKWARD = 2,
  FORWARD = 3,
  BOTH = 4
}

enum SchemaValidation {
  LATEST = 1,
  ALL = 2
}

enum SchemaVersionState {
  INITIATED = 1,
  START_REVIEW = 2,
  CHANGES_REQUIRED = 3,
  REVIEWED = 4,
  ENABLED = 5,
  DISABLED = 6,
  ARCHIVED = 7,
  DELETED = 8
}

struct HiveObjectRef{
  1: HiveObjectType objectType,
  2: string dbName,
  3: string objectName,
  4: list<string> partValues,
  5: string columnName,
  6: optional string catName
}

struct PrivilegeGrantInfo {
  1: string privilege,
  2: i32 createTime,
  3: string grantor,
  4: PrincipalType grantorType,
  5: bool grantOption,
}

struct HiveObjectPrivilege {
  1: HiveObjectRef  hiveObject,
  2: string principalName,
  3: PrincipalType principalType,
  4: PrivilegeGrantInfo grantInfo,
  5: string authorizer,
}

struct PrivilegeBag {
  1: list<HiveObjectPrivilege> privileges,
}

struct PrincipalPrivilegeSet {
  1: map<string, list<PrivilegeGrantInfo>> userPrivileges, // user name -> privilege grant info
  2: map<string, list<PrivilegeGrantInfo>> groupPrivileges, // group name -> privilege grant info
  3: map<string, list<PrivilegeGrantInfo>> rolePrivileges, //role name -> privilege grant info
}

struct GrantRevokePrivilegeRequest {
  1: GrantRevokeType requestType;
  2: PrivilegeBag privileges;
  3: optional bool revokeGrantOption;  // Only for revoke request
}

struct GrantRevokePrivilegeResponse {
  1: optional bool success;
}

struct Role {
  1: string roleName,
  2: i32 createTime,
  3: string ownerName,
}

// Representation of a grant for a principal to a role
struct RolePrincipalGrant {
  1: string roleName,
  2: string principalName,
  3: PrincipalType principalType,
  4: bool grantOption,
  5: i32 grantTime,
  6: string grantorName,
  7: PrincipalType grantorPrincipalType
}

struct GetRoleGrantsForPrincipalRequest {
  1: required string principal_name,
  2: required PrincipalType principal_type
}

struct GetRoleGrantsForPrincipalResponse {
  1: required list<RolePrincipalGrant> principalGrants;
}

struct GetPrincipalsInRoleRequest {
  1: required string roleName;
}

struct GetPrincipalsInRoleResponse {
  1: required list<RolePrincipalGrant> principalGrants;
}

struct GrantRevokeRoleRequest {
  1: GrantRevokeType requestType;
  2: string roleName;
  3: string principalName;
  4: PrincipalType principalType;
  5: optional string grantor;            // Needed for grant
  6: optional PrincipalType grantorType; // Needed for grant
  7: optional bool grantOption;
}

struct GrantRevokeRoleResponse {
  1: optional bool success;
}

struct Catalog {
  1: string name,                    // Name of the catalog
  2: optional string description,    // description of the catalog
  3: string locationUri              // default storage location.  When databases are created in
                                     // this catalog, if they do not specify a location, they will
                                     // be placed in this location.
}

struct CreateCatalogRequest {
  1: Catalog catalog
}

struct AlterCatalogRequest {
  1: string name,
  2: Catalog newCat
}

struct GetCatalogRequest {
  1: string name
}

struct GetCatalogResponse {
  1: Catalog catalog
}

struct GetCatalogsResponse {
  1: list<string> names
}

struct DropCatalogRequest {
  1: string name
}

// namespace for tables
struct Database {
  1: string name,
  2: string description,
  3: string locationUri,
  4: map<string, string> parameters, // properties associated with the database
  5: optional PrincipalPrivilegeSet privileges,
  6: optional string ownerName,
  7: optional PrincipalType ownerType,
  8: optional string catalogName
}

// This object holds the information needed by SerDes
struct SerDeInfo {
  1: string name,                   // name of the serde, table name by default
  2: string serializationLib,       // usually the class that implements the extractor & loader
  3: map<string, string> parameters, // initialization parameters
  4: optional string description,
  5: optional string serializerClass,
  6: optional string deserializerClass,
  7: optional SerdeType serdeType
}

// sort order of a column (column name along with asc(1)/desc(0))
struct Order {
  1: string col,      // sort column name
  2: i32    order     // asc(1) or desc(0)
}

// this object holds all the information about skewed table
struct SkewedInfo {
  1: list<string> skewedColNames, // skewed column names
  2: list<list<string>> skewedColValues, //skewed values
  3: map<list<string>, string> skewedColValueLocationMaps, //skewed value to location mappings
}

// this object holds all the information about physical storage of the data belonging to a table
struct StorageDescriptor {
  1: list<FieldSchema> cols,  // required (refer to types defined above)
  2: string location,         // defaults to <warehouse loc>/<db loc>/tablename
  3: string inputFormat,      // SequenceFileInputFormat (binary) or TextInputFormat`  or custom format
  4: string outputFormat,     // SequenceFileOutputFormat (binary) or IgnoreKeyTextOutputFormat or custom format
  5: bool   compressed,       // compressed or not
  6: i32    numBuckets,       // this must be specified if there are any dimension columns
  7: SerDeInfo    serdeInfo,  // serialization and deserialization information
  8: list<string> bucketCols, // reducer grouping columns and clustering columns and bucketing columns`
  9: list<Order>  sortCols,   // sort order of the data in each bucket
  10: map<string, string> parameters, // any user supplied key value hash
  11: optional SkewedInfo skewedInfo, // skewed information
  12: optional bool   storedAsSubDirectories       // stored as subdirectories or not
}

// table information
struct Table {
  1: string tableName,                // name of the table
  2: string dbName,                   // database name ('default')
  3: string owner,                    // owner of this table
  4: i32    createTime,               // creation time of the table
  5: i32    lastAccessTime,           // last access time (usually this will be filled from HDFS and shouldn't be relied on)
  6: i32    retention,                // retention time
  7: StorageDescriptor sd,            // storage descriptor of the table
  8: list<FieldSchema> partitionKeys, // partition keys of the table. only primitive types are supported
  9: map<string, string> parameters,   // to store comments or any other user level parameters
  10: string viewOriginalText,         // original view text, null for non-view
  11: string viewExpandedText,         // expanded view text, null for non-view
  12: string tableType,                // table type enum, e.g. EXTERNAL_TABLE
  13: optional PrincipalPrivilegeSet privileges,
  14: optional bool temporary=false,
  15: optional bool rewriteEnabled,     // rewrite enabled or not
  16: optional CreationMetadata creationMetadata,   // only for MVs, it stores table names used and txn list at MV creation
  17: optional string catName,          // Name of the catalog the table is in
  18: optional PrincipalType ownerType = PrincipalType.USER // owner type of this table (default to USER for backward compatibility)
}

struct Partition {
  1: list<string> values // string value is converted to appropriate partition key type
  2: string       dbName,
  3: string       tableName,
  4: i32          createTime,
  5: i32          lastAccessTime,
  6: StorageDescriptor   sd,
  7: map<string, string> parameters,
  8: optional PrincipalPrivilegeSet privileges,
  9: optional string catName
}

struct PartitionWithoutSD {
  1: list<string> values // string value is converted to appropriate partition key type
  2: i32          createTime,
  3: i32          lastAccessTime,
  4: string       relativePath,
  5: map<string, string> parameters,
  6: optional PrincipalPrivilegeSet privileges
}

struct PartitionSpecWithSharedSD {
  1: list<PartitionWithoutSD> partitions,
  2: StorageDescriptor sd,
}

struct PartitionListComposingSpec {
  1: list<Partition> partitions
}

struct PartitionSpec {
  1: string dbName,
  2: string tableName,
  3: string rootPath,
  4: optional PartitionSpecWithSharedSD sharedSDPartitionSpec,
  5: optional PartitionListComposingSpec partitionList,
  6: optional string catName
}

// column statistics
struct BooleanColumnStatsData {
1: required i64 numTrues,
2: required i64 numFalses,
3: required i64 numNulls,
4: optional binary bitVectors
}

struct DoubleColumnStatsData {
1: optional double lowValue,
2: optional double highValue,
3: required i64 numNulls,
4: required i64 numDVs,
5: optional binary bitVectors
}

struct LongColumnStatsData {
1: optional i64 lowValue,
2: optional i64 highValue,
3: required i64 numNulls,
4: required i64 numDVs,
5: optional binary bitVectors
}

struct StringColumnStatsData {
1: required i64 maxColLen,
2: required double avgColLen,
3: required i64 numNulls,
4: required i64 numDVs,
5: optional binary bitVectors
}

struct BinaryColumnStatsData {
1: required i64 maxColLen,
2: required double avgColLen,
3: required i64 numNulls,
4: optional binary bitVectors
}


struct Decimal {
3: required i16 scale, // force using scale first in Decimal.compareTo
1: required binary unscaled
}

struct DecimalColumnStatsData {
1: optional Decimal lowValue,
2: optional Decimal highValue,
3: required i64 numNulls,
4: required i64 numDVs,
5: optional binary bitVectors
}

struct Date {
1: required i64 daysSinceEpoch
}

struct DateColumnStatsData {
1: optional Date lowValue,
2: optional Date highValue,
3: required i64 numNulls,
4: required i64 numDVs,
5: optional binary bitVectors
}

union ColumnStatisticsData {
1: BooleanColumnStatsData booleanStats,
2: LongColumnStatsData longStats,
3: DoubleColumnStatsData doubleStats,
4: StringColumnStatsData stringStats,
5: BinaryColumnStatsData binaryStats,
6: DecimalColumnStatsData decimalStats,
7: DateColumnStatsData dateStats
}

struct ColumnStatisticsObj {
1: required string colName,
2: required string colType,
3: required ColumnStatisticsData statsData
}

struct ColumnStatisticsDesc {
1: required bool isTblLevel,
2: required string dbName,
3: required string tableName,
4: optional string partName,
5: optional i64 lastAnalyzed,
6: optional string catName
}

struct ColumnStatistics {
1: required ColumnStatisticsDesc statsDesc,
2: required list<ColumnStatisticsObj> statsObj;
}

struct AggrStats {
1: required list<ColumnStatisticsObj> colStats,
2: required i64 partsFound // number of partitions for which stats were found
}

struct SetPartitionsStatsRequest {
1: required list<ColumnStatistics> colStats,
2: optional bool needMerge //stats need to be merged with the existing stats
}

// schema of the table/query results etc.
struct Schema {
 // column names, types, comments
 1: list<FieldSchema> fieldSchemas,  // delimiters etc
 2: map<string, string> properties
}

// Key-value store to be used with selected
// Metastore APIs (create, alter methods).
// The client can pass environment properties / configs that can be
// accessed in hooks.
struct EnvironmentContext {
  1: map<string, string> properties
}

struct PrimaryKeysRequest {
  1: required string db_name,
  2: required string tbl_name,
  3: optional string catName
}

struct PrimaryKeysResponse {
  1: required list<SQLPrimaryKey> primaryKeys
}

struct ForeignKeysRequest {
  1: string parent_db_name,
  2: string parent_tbl_name,
  3: string foreign_db_name,
  4: string foreign_tbl_name
  5: optional string catName          // No cross catalog constraints
}

struct ForeignKeysResponse {
  1: required list<SQLForeignKey> foreignKeys
}

struct UniqueConstraintsRequest {
  1: required string catName,
  2: required string db_name,
  3: required string tbl_name,
}

struct UniqueConstraintsResponse {
  1: required list<SQLUniqueConstraint> uniqueConstraints
}

struct NotNullConstraintsRequest {
  1: required string catName,
  2: required string db_name,
  3: required string tbl_name,
}

struct NotNullConstraintsResponse {
  1: required list<SQLNotNullConstraint> notNullConstraints
}

struct DefaultConstraintsRequest {
  1: required string catName,
  2: required string db_name,
  3: required string tbl_name
}

struct DefaultConstraintsResponse {
  1: required list<SQLDefaultConstraint> defaultConstraints
}

struct CheckConstraintsRequest {
  1: required string catName,
  2: required string db_name,
  3: required string tbl_name
}

struct CheckConstraintsResponse {
  1: required list<SQLCheckConstraint> checkConstraints
}


struct DropConstraintRequest {
  1: required string dbname, 
  2: required string tablename,
  3: required string constraintname,
  4: optional string catName
}

struct AddPrimaryKeyRequest {
  1: required list<SQLPrimaryKey> primaryKeyCols
}

struct AddForeignKeyRequest {
  1: required list<SQLForeignKey> foreignKeyCols
}

struct AddUniqueConstraintRequest {
  1: required list<SQLUniqueConstraint> uniqueConstraintCols
}

struct AddNotNullConstraintRequest {
  1: required list<SQLNotNullConstraint> notNullConstraintCols
}

struct AddDefaultConstraintRequest {
  1: required list<SQLDefaultConstraint> defaultConstraintCols
}

struct AddCheckConstraintRequest {
  1: required list<SQLCheckConstraint> checkConstraintCols
}

// Return type for get_partitions_by_expr
struct PartitionsByExprResult {
  1: required list<Partition> partitions,
  // Whether the results has any (currently, all) partitions which may or may not match
  2: required bool hasUnknownPartitions
}

struct PartitionsByExprRequest {
  1: required string dbName,
  2: required string tblName,
  3: required binary expr,
  4: optional string defaultPartitionName,
  5: optional i16 maxParts=-1
  6: optional string catName
}

struct TableStatsResult {
  1: required list<ColumnStatisticsObj> tableStats
}

struct PartitionsStatsResult {
  1: required map<string, list<ColumnStatisticsObj>> partStats
}

struct TableStatsRequest {
 1: required string dbName,
 2: required string tblName,
 3: required list<string> colNames
 4: optional string catName
}

struct PartitionsStatsRequest {
 1: required string dbName,
 2: required string tblName,
 3: required list<string> colNames,
 4: required list<string> partNames,
 5: optional string catName
}

// Return type for add_partitions_req
struct AddPartitionsResult {
  1: optional list<Partition> partitions,
}

// Request type for add_partitions_req
struct AddPartitionsRequest {
  1: required string dbName,
  2: required string tblName,
  3: required list<Partition> parts,
  4: required bool ifNotExists,
  5: optional bool needResult=true,
  6: optional string catName
}

// Return type for drop_partitions_req
struct DropPartitionsResult {
  1: optional list<Partition> partitions,
}

struct DropPartitionsExpr {
  1: required binary expr;
  2: optional i32 partArchiveLevel;
}

union RequestPartsSpec {
  1: list<string> names;
  2: list<DropPartitionsExpr> exprs;
}

// Request type for drop_partitions_req
// TODO: we might want to add "bestEffort" flag; where a subset can fail
struct DropPartitionsRequest {
  1: required string dbName,
  2: required string tblName,
  3: required RequestPartsSpec parts,
  4: optional bool deleteData,
  5: optional bool ifExists=true, // currently verified on client
  6: optional bool ignoreProtection,
  7: optional EnvironmentContext environmentContext,
  8: optional bool needResult=true,
  9: optional string catName
}

struct PartitionValuesRequest {
  1: required string dbName,
  2: required string tblName,
  3: required list<FieldSchema> partitionKeys;
  4: optional bool applyDistinct = true;
  5: optional string filter;
  6: optional list<FieldSchema> partitionOrder;
  7: optional bool ascending = true;
  8: optional i64 maxParts = -1;
  9: optional string catName
}

struct PartitionValuesRow {
  1: required list<string> row;
}

struct PartitionValuesResponse {
  1: required list<PartitionValuesRow> partitionValues;
}

enum FunctionType {
  JAVA = 1,
}

enum ResourceType {
  JAR     = 1,
  FILE    = 2,
  ARCHIVE = 3,
}

struct ResourceUri {
  1: ResourceType resourceType,
  2: string       uri,
}

// User-defined function
struct Function {
  1: string           functionName,
  2: string           dbName,
  3: string           className,
  4: string           ownerName,
  5: PrincipalType    ownerType,
  6: i32              createTime,
  7: FunctionType     functionType,
  8: list<ResourceUri> resourceUris,
  9: optional string  catName
}

// Structs for transaction and locks
struct TxnInfo {
    1: required i64 id,
    2: required TxnState state,
    3: required string user,        // used in 'show transactions' to help admins find who has open transactions
    4: required string hostname,    // used in 'show transactions' to help admins find who has open transactions
    5: optional string agentInfo = "Unknown",
    6: optional i32 heartbeatCount=0,
    7: optional string metaInfo,
    8: optional i64 startedTime,
    9: optional i64 lastHeartbeatTime,
}

struct GetOpenTxnsInfoResponse {
    1: required i64 txn_high_water_mark,
    2: required list<TxnInfo> open_txns,
}

struct GetOpenTxnsResponse {
    1: required i64 txn_high_water_mark,
    2: required list<i64> open_txns,  // set<i64> changed to list<i64> since 3.0
    3: optional i64 min_open_txn, //since 1.3,2.2
    4: required binary abortedBits,   // since 3.0
}

struct OpenTxnRequest {
    1: required i32 num_txns,
    2: required string user,
    3: required string hostname,
    4: optional string agentInfo = "Unknown",
    5: optional string replPolicy,
    6: optional list<i64> replSrcTxnIds,
}

struct OpenTxnsResponse {
    1: required list<i64> txn_ids,
}

struct AbortTxnRequest {
    1: required i64 txnid,
    2: optional string replPolicy,
}

struct AbortTxnsRequest {
    1: required list<i64> txn_ids,
}

struct CommitTxnRequest {
    1: required i64 txnid,
    2: optional string replPolicy,
}

struct ReplTblWriteIdStateRequest {
    1: required string validWriteIdlist,
    2: required string user,
    3: required string hostName,
    4: required string dbName,
    5: required string tableName,
    6: optional list<string> partNames,
}

// Request msg to get the valid write ids list for the given list of tables wrt to input validTxnList
struct GetValidWriteIdsRequest {
    1: required list<string> fullTableNames, // Full table names of format <db_name>.<table_name>
    2: required string validTxnList, // Valid txn list string wrt the current txn of the caller
}

// Valid Write ID list of one table wrt to current txn
struct TableValidWriteIds {
    1: required string fullTableName,  // Full table name of format <db_name>.<table_name>
    2: required i64 writeIdHighWaterMark, // The highest write id valid for this table wrt given txn
    3: required list<i64> invalidWriteIds, // List of open and aborted writes ids in the table
    4: optional i64 minOpenWriteId, // Minimum write id which maps to a opened txn
    5: required binary abortedBits, // Bit array to identify the aborted write ids in invalidWriteIds list
}

// Valid Write ID list for all the input tables wrt to current txn
struct GetValidWriteIdsResponse {
    1: required list<TableValidWriteIds> tblValidWriteIds,
}

// Request msg to allocate table write ids for the given list of txns
struct AllocateTableWriteIdsRequest {
    1: required string dbName,
    2: required string tableName,
    // Either txnIds or replPolicy+srcTxnToWriteIdList can exist in a call. txnIds is used by normal flow and
    // replPolicy+srcTxnToWriteIdList is used by replication task.
    3: optional list<i64> txnIds,
    4: optional string replPolicy,
    // The list is assumed to be sorted by both txnids and write ids. The write id list is assumed to be contiguous.
    5: optional list<TxnToWriteId> srcTxnToWriteIdList,
}

// Map for allocated write id against the txn for which it is allocated
struct TxnToWriteId {
    1: required i64 txnId,
    2: required i64 writeId,
}

struct AllocateTableWriteIdsResponse {
    1: required list<TxnToWriteId> txnToWriteIds,
}

struct LockComponent {
    1: required LockType type,
    2: required LockLevel level,
    3: required string dbname,
    4: optional string tablename,
    5: optional string partitionname,
    6: optional DataOperationType operationType = DataOperationType.UNSET,
    7: optional bool isTransactional = false,
    8: optional bool isDynamicPartitionWrite = false
}

struct LockRequest {
    1: required list<LockComponent> component,
    2: optional i64 txnid,
    3: required string user,     // used in 'show locks' to help admins find who has open locks
    4: required string hostname, // used in 'show locks' to help admins find who has open locks
    5: optional string agentInfo = "Unknown",
}

struct LockResponse {
    1: required i64 lockid,
    2: required LockState state,
}

struct CheckLockRequest {
    1: required i64 lockid,
    2: optional i64 txnid,
    3: optional i64 elapsed_ms,
}

struct UnlockRequest {
    1: required i64 lockid,
}

struct ShowLocksRequest {
    1: optional string dbname,
    2: optional string tablename,
    3: optional string partname,
    4: optional bool isExtended=false,
}

struct ShowLocksResponseElement {
    1: required i64 lockid,
    2: required string dbname,
    3: optional string tablename,
    4: optional string partname,
    5: required LockState state,
    6: required LockType type,
    7: optional i64 txnid,
    8: required i64 lastheartbeat,
    9: optional i64 acquiredat,
    10: required string user,
    11: required string hostname,
    12: optional i32 heartbeatCount = 0,
    13: optional string agentInfo,
    14: optional i64 blockedByExtId,
    15: optional i64 blockedByIntId,
    16: optional i64 lockIdInternal,
}

struct ShowLocksResponse {
    1: list<ShowLocksResponseElement> locks,
}

struct HeartbeatRequest {
    1: optional i64 lockid,
    2: optional i64 txnid
}

struct HeartbeatTxnRangeRequest {
    1: required i64 min,
    2: required i64 max
}

struct HeartbeatTxnRangeResponse {
    1: required set<i64> aborted,
    2: required set<i64> nosuch
}

struct CompactionRequest {
    1: required string dbname,
    2: required string tablename,
    3: optional string partitionname,
    4: required CompactionType type,
    5: optional string runas,
    6: optional map<string, string> properties
}

struct CompactionResponse {
    1: required i64 id,
    2: required string state,
    3: required bool accepted
}

struct ShowCompactRequest {
}

struct ShowCompactResponseElement {
    1: required string dbname,
    2: required string tablename,
    3: optional string partitionname,
    4: required CompactionType type,
    5: required string state,
    6: optional string workerid,
    7: optional i64 start,
    8: optional string runAs,
    9: optional i64 hightestTxnId, // Highest Txn ID handled by this compaction
    10: optional string metaInfo,
    11: optional i64 endTime,
    12: optional string hadoopJobId = "None",
    13: optional i64 id,
}

struct ShowCompactResponse {
    1: required list<ShowCompactResponseElement> compacts,
}

struct AddDynamicPartitions {
    1: required i64 txnid,
    2: required i64 writeid,
    3: required string dbname,
    4: required string tablename,
    5: required list<string> partitionnames,
    6: optional DataOperationType operationType = DataOperationType.UNSET
}

struct BasicTxnInfo {
    1: required bool isnull,
    2: optional i64 time,
    3: optional i64 txnid,
    4: optional string dbname,
    5: optional string tablename,
    6: optional string partitionname
}

struct CreationMetadata {
    1: required string catName
    2: required string dbName,
    3: required string tblName,
    4: required set<string> tablesUsed,
    5: optional string validTxnList,
    6: optional i64 materializationTime
}

struct NotificationEventRequest {
    1: required i64 lastEvent,
    2: optional i32 maxEvents,
}

struct NotificationEvent {
    1: required i64 eventId,
    2: required i32 eventTime,
    3: required string eventType,
    4: optional string dbName,
    5: optional string tableName,
    6: required string message,
    7: optional string messageFormat,
    8: optional string catName
}

struct NotificationEventResponse {
    1: required list<NotificationEvent> events,
}

struct CurrentNotificationEventId {
    1: required i64 eventId,
}

struct NotificationEventsCountRequest {
    1: required i64 fromEventId,
    2: required string dbName,
    3: optional string catName
}

struct NotificationEventsCountResponse {
    1: required i64 eventsCount,
}

struct InsertEventRequestData {
    1: optional bool replace,
    2: required list<string> filesAdded,
    // Checksum of files (hex string of checksum byte payload)
    3: optional list<string> filesAddedChecksum,
}

union FireEventRequestData {
    1: InsertEventRequestData insertData
}

struct FireEventRequest {
    1: required bool successful,
    2: required FireEventRequestData data
    // dbname, tablename, and partition vals are included as optional in the top level event rather than placed in each type of
    // subevent as I assume they'll be used across most event types.
    3: optional string dbName,
    4: optional string tableName,
    5: optional list<string> partitionVals,
    6: optional string catName,
}

struct FireEventResponse {
    // NOP for now, this is just a place holder for future responses
}
    
struct MetadataPpdResult {
  1: optional binary metadata,
  2: optional binary includeBitset
}

// Return type for get_file_metadata_by_expr
struct GetFileMetadataByExprResult {
  1: required map<i64, MetadataPpdResult> metadata,
  2: required bool isSupported
}

enum FileMetadataExprType {
  ORC_SARG = 1
}


// Request type for get_file_metadata_by_expr
struct GetFileMetadataByExprRequest {
  1: required list<i64> fileIds,
  2: required binary expr,
  3: optional bool doGetFooters,
  4: optional FileMetadataExprType type
}

// Return type for get_file_metadata
struct GetFileMetadataResult {
  1: required map<i64, binary> metadata,
  2: required bool isSupported
}

// Request type for get_file_metadata
struct GetFileMetadataRequest {
  1: required list<i64> fileIds
}

// Return type for put_file_metadata
struct PutFileMetadataResult {
}

// Request type for put_file_metadata
struct PutFileMetadataRequest {
  1: required list<i64> fileIds,
  2: required list<binary> metadata,
  3: optional FileMetadataExprType type
}

// Return type for clear_file_metadata
struct ClearFileMetadataResult {
}

// Request type for clear_file_metadata
struct ClearFileMetadataRequest {
  1: required list<i64> fileIds
}

// Return type for cache_file_metadata
struct CacheFileMetadataResult {
  1: required bool isSupported
}

// Request type for cache_file_metadata
struct CacheFileMetadataRequest {
  1: required string dbName,
  2: required string tblName,
  3: optional string partName,
  4: optional bool isAllParts
}

struct GetAllFunctionsResponse {
  1: optional list<Function> functions
}

enum ClientCapability {
  TEST_CAPABILITY = 1,
  INSERT_ONLY_TABLES = 2
}


struct ClientCapabilities {
  1: required list<ClientCapability> values
}

struct GetTableRequest {
  1: required string dbName,
  2: required string tblName,
  3: optional ClientCapabilities capabilities,
  4: optional string catName
}

struct GetTableResult {
  1: required Table table
}

struct GetTablesRequest {
  1: required string dbName,
  2: optional list<string> tblNames,
  3: optional ClientCapabilities capabilities,
  4: optional string catName
}

struct GetTablesResult {
  1: required list<Table> tables
}

// Request type for cm_recycle
struct CmRecycleRequest {
  1: required string dataPath,
  2: required bool purge
}

// Response type for cm_recycle
struct CmRecycleResponse {
}

struct TableMeta {
  1: required string dbName;
  2: required string tableName;
  3: required string tableType;
  4: optional string comments;
  5: optional string catName;
}

struct Materialization {
  1: required bool sourceTablesUpdateDeleteModified;
}

// Data types for workload management.

enum WMResourcePlanStatus {
  ACTIVE = 1,
  ENABLED = 2,
  DISABLED = 3
}

enum  WMPoolSchedulingPolicy {
  FAIR = 1,
  FIFO = 2
}

struct WMResourcePlan {
  1: required string name;
  2: optional WMResourcePlanStatus status;
  3: optional i32 queryParallelism;
  4: optional string defaultPoolPath;
}

struct WMNullableResourcePlan {
  1: optional string name;
  2: optional WMResourcePlanStatus status;
  4: optional i32 queryParallelism;
  5: optional bool isSetQueryParallelism;
  6: optional string defaultPoolPath;
  7: optional bool isSetDefaultPoolPath;
}

struct WMPool {
  1: required string resourcePlanName;
  2: required string poolPath;
  3: optional double allocFraction;
  4: optional i32 queryParallelism;
  5: optional string schedulingPolicy;
}


struct WMNullablePool {
  1: required string resourcePlanName;
  2: required string poolPath;
  3: optional double allocFraction;
  4: optional i32 queryParallelism;
  5: optional string schedulingPolicy;
  6: optional bool isSetSchedulingPolicy;
}

struct WMTrigger {
  1: required string resourcePlanName;
  2: required string triggerName;
  3: optional string triggerExpression;
  4: optional string actionExpression;
  5: optional bool isInUnmanaged;
}

struct WMMapping {
  1: required string resourcePlanName;
  2: required string entityType;
  3: required string entityName;
  4: optional string poolPath;
  5: optional i32 ordering;
}

struct WMPoolTrigger {
  1: required string pool;
  2: required string trigger;
}

struct WMFullResourcePlan {
  1: required WMResourcePlan plan;
  2: required list<WMPool> pools;
  3: optional list<WMMapping> mappings;
  4: optional list<WMTrigger> triggers;
  5: optional list<WMPoolTrigger> poolTriggers;
}

// Request response for workload management API's.

struct WMCreateResourcePlanRequest {
  1: optional WMResourcePlan resourcePlan;
  2: optional string copyFrom;
}

struct WMCreateResourcePlanResponse {
}

struct WMGetActiveResourcePlanRequest {
}

struct WMGetActiveResourcePlanResponse {
  1: optional WMFullResourcePlan resourcePlan;
}

struct WMGetResourcePlanRequest {
  1: optional string resourcePlanName;
}

struct WMGetResourcePlanResponse {
  1: optional WMFullResourcePlan resourcePlan;
}

struct WMGetAllResourcePlanRequest {
}

struct WMGetAllResourcePlanResponse {
  1: optional list<WMResourcePlan> resourcePlans;
}

struct WMAlterResourcePlanRequest {
  1: optional string resourcePlanName;
  2: optional WMNullableResourcePlan resourcePlan;
  3: optional bool isEnableAndActivate;
  4: optional bool isForceDeactivate;
  5: optional bool isReplace;
}

struct WMAlterResourcePlanResponse {
  1: optional WMFullResourcePlan fullResourcePlan;
}

struct WMValidateResourcePlanRequest {
  1: optional string resourcePlanName;
}

struct WMValidateResourcePlanResponse {
  1: optional list<string> errors;
  2: optional list<string> warnings;
}

struct WMDropResourcePlanRequest {
  1: optional string resourcePlanName;
}

struct WMDropResourcePlanResponse {
}

struct WMCreateTriggerRequest {
  1: optional WMTrigger trigger;
}

struct WMCreateTriggerResponse {
}

struct WMAlterTriggerRequest {
  1: optional WMTrigger trigger;
}

struct WMAlterTriggerResponse {
}

struct WMDropTriggerRequest {
  1: optional string resourcePlanName;
  2: optional string triggerName;
}

struct WMDropTriggerResponse {
}

struct WMGetTriggersForResourePlanRequest {
  1: optional string resourcePlanName;
}

struct WMGetTriggersForResourePlanResponse {
  1: optional list<WMTrigger> triggers;
}

struct WMCreatePoolRequest {
  1: optional WMPool pool;
}

struct WMCreatePoolResponse {
}

struct WMAlterPoolRequest {
  1: optional WMNullablePool pool;
  2: optional string poolPath;
}

struct WMAlterPoolResponse {
}

struct WMDropPoolRequest {
  1: optional string resourcePlanName;
  2: optional string poolPath;
}

struct WMDropPoolResponse {
}

struct WMCreateOrUpdateMappingRequest {
  1: optional WMMapping mapping;
  2: optional bool update;
}

struct WMCreateOrUpdateMappingResponse {
}

struct WMDropMappingRequest {
  1: optional WMMapping mapping;
}

struct WMDropMappingResponse {
}

struct WMCreateOrDropTriggerToPoolMappingRequest {
  1: optional string resourcePlanName;
  2: optional string triggerName;
  3: optional string poolPath;
  4: optional bool drop;
}

struct WMCreateOrDropTriggerToPoolMappingResponse {
}

// Schema objects
// Schema is already taken, so for the moment I'm calling it an ISchema for Independent Schema
struct ISchema {
  1: SchemaType schemaType,
  2: string name,
  3: string catName,
  4: string dbName,
  5: SchemaCompatibility compatibility,
  6: SchemaValidation validationLevel,
  7: bool canEvolve,
  8: optional string schemaGroup,
  9: optional string description
}

struct ISchemaName {
  1: string catName,
  2: string dbName,
  3: string schemaName
}

struct AlterISchemaRequest {
  1: ISchemaName name,
  3: ISchema newSchema
}

struct SchemaVersion {
  1:  ISchemaName schema,
  2:  i32 version,
  3:  i64 createdAt,
  4:  list<FieldSchema> cols,
  5:  optional SchemaVersionState state,
  6:  optional string description,
  7:  optional string schemaText,
  8:  optional string fingerprint,
  9:  optional string name,
  10: optional SerDeInfo serDe
}

struct SchemaVersionDescriptor {
  1: ISchemaName schema,
  2: i32 version
}

struct FindSchemasByColsRqst {
  1: optional string colName,
  2: optional string colNamespace,
  3: optional string type
}

struct FindSchemasByColsResp {
  1: list<SchemaVersionDescriptor> schemaVersions
}

struct MapSchemaVersionToSerdeRequest {
  1: SchemaVersionDescriptor schemaVersion,
  2: string serdeName
}

struct SetSchemaVersionStateRequest {
  1: SchemaVersionDescriptor schemaVersion,
  2: SchemaVersionState state
}

struct GetSerdeRequest {
  1: string serdeName
}

struct RuntimeStat {
  1: optional i32 createTime,
  2: required i32 weight,
  3: required binary payload
}

struct GetRuntimeStatsRequest {
  1: required i32 maxWeight,
  2: required i32 maxCreateTime
}

// Exceptions.

exception MetaException {
  1: string message
}

exception UnknownTableException {
  1: string message
}

exception UnknownDBException {
  1: string message
}

exception AlreadyExistsException {
  1: string message
}

exception InvalidPartitionException {
  1: string message
}

exception UnknownPartitionException {
  1: string message
}

exception InvalidObjectException {
  1: string message
}

exception NoSuchObjectException {
  1: string message
}

exception InvalidOperationException {
  1: string message
}

exception ConfigValSecurityException {
  1: string message
}

exception InvalidInputException {
  1: string message
}

// Transaction and lock exceptions
exception NoSuchTxnException {
    1: string message
}

exception TxnAbortedException {
    1: string message
}

exception TxnOpenException {
    1: string message
}

exception NoSuchLockException {
    1: string message
}

/**
* This interface is live.
*/
service ThriftHiveMetastore
{
  string getMetaConf(1:string key) throws(1:MetaException o1)
  void setMetaConf(1:string key, 2:string value) throws(1:MetaException o1)

  void create_catalog(1: CreateCatalogRequest catalog) throws (1:AlreadyExistsException o1, 2:InvalidObjectException o2, 3: MetaException o3)
  void alter_catalog(1: AlterCatalogRequest rqst) throws (1:NoSuchObjectException o1, 2:InvalidOperationException o2, 3:MetaException o3)
  GetCatalogResponse get_catalog(1: GetCatalogRequest catName) throws (1:NoSuchObjectException o1, 2:MetaException o2)
  GetCatalogsResponse get_catalogs() throws (1:MetaException o1)
  void drop_catalog(1: DropCatalogRequest catName) throws (1:NoSuchObjectException o1, 2:InvalidOperationException o2, 3:MetaException o3)

  void create_database(1:Database database) throws(1:AlreadyExistsException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  Database get_database(1:string name) throws(1:NoSuchObjectException o1, 2:MetaException o2)
  void drop_database(1:string name, 2:bool deleteData, 3:bool cascade) throws(1:NoSuchObjectException o1, 2:InvalidOperationException o2, 3:MetaException o3)
  list<string> get_databases(1:string pattern) throws(1:MetaException o1)
  list<string> get_all_databases() throws(1:MetaException o1)
  void alter_database(1:string dbname, 2:Database db) throws(1:MetaException o1, 2:NoSuchObjectException o2)

  // returns the type with given name (make seperate calls for the dependent types if needed)
  Type get_type(1:string name)  throws(1:MetaException o1, 2:NoSuchObjectException o2)
  bool create_type(1:Type type) throws(1:AlreadyExistsException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  bool drop_type(1:string type) throws(1:MetaException o1, 2:NoSuchObjectException o2)
  map<string, Type> get_type_all(1:string name)
                                throws(1:MetaException o2)

  // Gets a list of FieldSchemas describing the columns of a particular table
  list<FieldSchema> get_fields(1: string db_name, 2: string table_name) throws (1: MetaException o1, 2: UnknownTableException o2, 3: UnknownDBException o3),
  list<FieldSchema> get_fields_with_environment_context(1: string db_name, 2: string table_name, 3:EnvironmentContext environment_context) throws (1: MetaException o1, 2: UnknownTableException o2, 3: UnknownDBException o3)

  // Gets a list of FieldSchemas describing both the columns and the partition keys of a particular table
  list<FieldSchema> get_schema(1: string db_name, 2: string table_name) throws (1: MetaException o1, 2: UnknownTableException o2, 3: UnknownDBException o3)
  list<FieldSchema> get_schema_with_environment_context(1: string db_name, 2: string table_name, 3:EnvironmentContext environment_context) throws (1: MetaException o1, 2: UnknownTableException o2, 3: UnknownDBException o3)

  // create a Hive table. Following fields must be set
  // tableName
  // database        (only 'default' for now until Hive QL supports databases)
  // owner           (not needed, but good to have for tracking purposes)
  // sd.cols         (list of field schemas)
  // sd.inputFormat  (SequenceFileInputFormat (binary like falcon tables or u_full) or TextInputFormat)
  // sd.outputFormat (SequenceFileInputFormat (binary) or TextInputFormat)
  // sd.serdeInfo.serializationLib (SerDe class name eg org.apache.hadoop.hive.serde.simple_meta.MetadataTypedColumnsetSerDe
  // * See notes on DDL_TIME
  void create_table(1:Table tbl) throws(1:AlreadyExistsException o1, 2:InvalidObjectException o2, 3:MetaException o3, 4:NoSuchObjectException o4)
  void create_table_with_environment_context(1:Table tbl,
      2:EnvironmentContext environment_context)
      throws (1:AlreadyExistsException o1,
              2:InvalidObjectException o2, 3:MetaException o3,
              4:NoSuchObjectException o4)
  void create_table_with_constraints(1:Table tbl, 2: list<SQLPrimaryKey> primaryKeys, 3: list<SQLForeignKey> foreignKeys,
  4: list<SQLUniqueConstraint> uniqueConstraints, 5: list<SQLNotNullConstraint> notNullConstraints,
  6: list<SQLDefaultConstraint> defaultConstraints, 7: list<SQLCheckConstraint> checkConstraints)
      throws (1:AlreadyExistsException o1,
              2:InvalidObjectException o2, 3:MetaException o3,
              4:NoSuchObjectException o4)
  void drop_constraint(1:DropConstraintRequest req)
      throws(1:NoSuchObjectException o1, 2:MetaException o3)
  void add_primary_key(1:AddPrimaryKeyRequest req)
      throws(1:NoSuchObjectException o1, 2:MetaException o2)
  void add_foreign_key(1:AddForeignKeyRequest req)
      throws(1:NoSuchObjectException o1, 2:MetaException o2)  
  void add_unique_constraint(1:AddUniqueConstraintRequest req)
      throws(1:NoSuchObjectException o1, 2:MetaException o2)
  void add_not_null_constraint(1:AddNotNullConstraintRequest req)
      throws(1:NoSuchObjectException o1, 2:MetaException o2)
  void add_default_constraint(1:AddDefaultConstraintRequest req)
      throws(1:NoSuchObjectException o1, 2:MetaException o2)
  void add_check_constraint(1:AddCheckConstraintRequest req)
      throws(1:NoSuchObjectException o1, 2:MetaException o2)

  // drops the table and all the partitions associated with it if the table has partitions
  // delete data (including partitions) if deleteData is set to true
  void drop_table(1:string dbname, 2:string name, 3:bool deleteData)
                       throws(1:NoSuchObjectException o1, 2:MetaException o3)
  void drop_table_with_environment_context(1:string dbname, 2:string name, 3:bool deleteData,
      4:EnvironmentContext environment_context)
                       throws(1:NoSuchObjectException o1, 2:MetaException o3)
  void truncate_table(1:string dbName, 2:string tableName, 3:list<string> partNames)
                          throws(1:MetaException o1)
  list<string> get_tables(1: string db_name, 2: string pattern) throws (1: MetaException o1)
  list<string> get_tables_by_type(1: string db_name, 2: string pattern, 3: string tableType) throws (1: MetaException o1)
  list<string> get_materialized_views_for_rewriting(1: string db_name) throws (1: MetaException o1)
  list<TableMeta> get_table_meta(1: string db_patterns, 2: string tbl_patterns, 3: list<string> tbl_types)
                       throws (1: MetaException o1)
  list<string> get_all_tables(1: string db_name) throws (1: MetaException o1)

  Table get_table(1:string dbname, 2:string tbl_name)
                       throws (1:MetaException o1, 2:NoSuchObjectException o2)
  list<Table> get_table_objects_by_name(1:string dbname, 2:list<string> tbl_names)
  GetTableResult get_table_req(1:GetTableRequest req) throws (1:MetaException o1, 2:NoSuchObjectException o2)
  GetTablesResult get_table_objects_by_name_req(1:GetTablesRequest req)
				   throws (1:MetaException o1, 2:InvalidOperationException o2, 3:UnknownDBException o3)
  Materialization get_materialization_invalidation_info(1:CreationMetadata creation_metadata, 2:string validTxnList)
				   throws (1:MetaException o1, 2:InvalidOperationException o2, 3:UnknownDBException o3)
  void update_creation_metadata(1: string catName, 2:string dbname, 3:string tbl_name, 4:CreationMetadata creation_metadata)
                   throws (1:MetaException o1, 2:InvalidOperationException o2, 3:UnknownDBException o3)

  // Get a list of table names that match a filter.
  // The filter operators are LIKE, <, <=, >, >=, =, <>
  //
  // In the filter statement, values interpreted as strings must be enclosed in quotes,
  // while values interpreted as integers should not be.  Strings and integers are the only
  // supported value types.
  //
  // The currently supported key names in the filter are:
  // Constants.HIVE_FILTER_FIELD_OWNER, which filters on the tables' owner's name
  //   and supports all filter operators
  // Constants.HIVE_FILTER_FIELD_LAST_ACCESS, which filters on the last access times
  //   and supports all filter operators except LIKE
  // Constants.HIVE_FILTER_FIELD_PARAMS, which filters on the tables' parameter keys and values
  //   and only supports the filter operators = and <>.
  //   Append the parameter key name to HIVE_FILTER_FIELD_PARAMS in the filter statement.
  //   For example, to filter on parameter keys called "retention", the key name in the filter
  //   statement should be Constants.HIVE_FILTER_FIELD_PARAMS + "retention"
  //   Also, = and <> only work for keys that exist
  //   in the tables. E.g., if you are looking for tables where key1 <> value, it will only
  //   look at tables that have a value for the parameter key1.
  // Some example filter statements include:
  // filter = Constants.HIVE_FILTER_FIELD_OWNER + " like \".*test.*\" and " +
  //   Constants.HIVE_FILTER_FIELD_LAST_ACCESS + " = 0";
  // filter = Constants.HIVE_FILTER_FIELD_PARAMS + "retention = \"30\" or " +
  //   Constants.HIVE_FILTER_FIELD_PARAMS + "retention = \"90\""
  // @param dbName
  //          The name of the database from which you will retrieve the table names
  // @param filterType
  //          The type of filter
  // @param filter
  //          The filter string
  // @param max_tables
  //          The maximum number of tables returned
  // @return  A list of table names that match the desired filter
  list<string> get_table_names_by_filter(1:string dbname, 2:string filter, 3:i16 max_tables=-1)
                       throws (1:MetaException o1, 2:InvalidOperationException o2, 3:UnknownDBException o3)

  // alter table applies to only future partitions not for existing partitions
  // * See notes on DDL_TIME
  void alter_table(1:string dbname, 2:string tbl_name, 3:Table new_tbl)
                       throws (1:InvalidOperationException o1, 2:MetaException o2)
  void alter_table_with_environment_context(1:string dbname, 2:string tbl_name,
      3:Table new_tbl, 4:EnvironmentContext environment_context)
      throws (1:InvalidOperationException o1, 2:MetaException o2)
  // alter table not only applies to future partitions but also cascade to existing partitions
  void alter_table_with_cascade(1:string dbname, 2:string tbl_name, 3:Table new_tbl, 4:bool cascade)
                       throws (1:InvalidOperationException o1, 2:MetaException o2)
  // the following applies to only tables that have partitions
  // * See notes on DDL_TIME
  Partition add_partition(1:Partition new_part)
                       throws(1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
  Partition add_partition_with_environment_context(1:Partition new_part,
      2:EnvironmentContext environment_context)
      throws (1:InvalidObjectException o1, 2:AlreadyExistsException o2,
      3:MetaException o3)
  i32 add_partitions(1:list<Partition> new_parts)
                       throws(1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
  i32 add_partitions_pspec(1:list<PartitionSpec> new_parts)
                       throws(1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
  Partition append_partition(1:string db_name, 2:string tbl_name, 3:list<string> part_vals)
                       throws (1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
  AddPartitionsResult add_partitions_req(1:AddPartitionsRequest request)
                       throws(1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
  Partition append_partition_with_environment_context(1:string db_name, 2:string tbl_name,
      3:list<string> part_vals, 4:EnvironmentContext environment_context)
                       throws (1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
  Partition append_partition_by_name(1:string db_name, 2:string tbl_name, 3:string part_name)
                       throws (1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
  Partition append_partition_by_name_with_environment_context(1:string db_name, 2:string tbl_name,
      3:string part_name, 4:EnvironmentContext environment_context)
                       throws (1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
  bool drop_partition(1:string db_name, 2:string tbl_name, 3:list<string> part_vals, 4:bool deleteData)
                       throws(1:NoSuchObjectException o1, 2:MetaException o2)
  bool drop_partition_with_environment_context(1:string db_name, 2:string tbl_name,
      3:list<string> part_vals, 4:bool deleteData, 5:EnvironmentContext environment_context)
                       throws(1:NoSuchObjectException o1, 2:MetaException o2)
  bool drop_partition_by_name(1:string db_name, 2:string tbl_name, 3:string part_name, 4:bool deleteData)
                       throws(1:NoSuchObjectException o1, 2:MetaException o2)
  bool drop_partition_by_name_with_environment_context(1:string db_name, 2:string tbl_name,
      3:string part_name, 4:bool deleteData, 5:EnvironmentContext environment_context)
                       throws(1:NoSuchObjectException o1, 2:MetaException o2)
  DropPartitionsResult drop_partitions_req(1: DropPartitionsRequest req)
                       throws(1:NoSuchObjectException o1, 2:MetaException o2)

  Partition get_partition(1:string db_name, 2:string tbl_name, 3:list<string> part_vals)
                       throws(1:MetaException o1, 2:NoSuchObjectException o2)
  Partition exchange_partition(1:map<string, string> partitionSpecs, 2:string source_db,
      3:string source_table_name, 4:string dest_db, 5:string dest_table_name)
      throws(1:MetaException o1, 2:NoSuchObjectException o2, 3:InvalidObjectException o3,
      4:InvalidInputException o4)

  list<Partition> exchange_partitions(1:map<string, string> partitionSpecs, 2:string source_db,
      3:string source_table_name, 4:string dest_db, 5:string dest_table_name)
      throws(1:MetaException o1, 2:NoSuchObjectException o2, 3:InvalidObjectException o3,
      4:InvalidInputException o4)

  Partition get_partition_with_auth(1:string db_name, 2:string tbl_name, 3:list<string> part_vals,
      4: string user_name, 5: list<string> group_names) throws(1:MetaException o1, 2:NoSuchObjectException o2)

  Partition get_partition_by_name(1:string db_name 2:string tbl_name, 3:string part_name)
                       throws(1:MetaException o1, 2:NoSuchObjectException o2)

  // returns all the partitions for this table in reverse chronological order.
  // If max parts is given then it will return only that many.
  list<Partition> get_partitions(1:string db_name, 2:string tbl_name, 3:i16 max_parts=-1)
                       throws(1:NoSuchObjectException o1, 2:MetaException o2)
  list<Partition> get_partitions_with_auth(1:string db_name, 2:string tbl_name, 3:i16 max_parts=-1,
     4: string user_name, 5: list<string> group_names) throws(1:NoSuchObjectException o1, 2:MetaException o2)

  list<PartitionSpec> get_partitions_pspec(1:string db_name, 2:string tbl_name, 3:i32 max_parts=-1)
                       throws(1:NoSuchObjectException o1, 2:MetaException o2)

  list<string> get_partition_names(1:string db_name, 2:string tbl_name, 3:i16 max_parts=-1)
                       throws(1:NoSuchObjectException o1, 2:MetaException o2)

  PartitionValuesResponse get_partition_values(1:PartitionValuesRequest request)
    throws(1:MetaException o1, 2:NoSuchObjectException o2);

  // get_partition*_ps methods allow filtering by a partial partition specification,
  // as needed for dynamic partitions. The values that are not restricted should
  // be empty strings. Nulls were considered (instead of "") but caused errors in
  // generated Python code. The size of part_vals may be smaller than the
  // number of partition columns - the unspecified values are considered the same
  // as "".
  list<Partition> get_partitions_ps(1:string db_name 2:string tbl_name
  	3:list<string> part_vals, 4:i16 max_parts=-1)
                       throws(1:MetaException o1, 2:NoSuchObjectException o2)
  list<Partition> get_partitions_ps_with_auth(1:string db_name, 2:string tbl_name, 3:list<string> part_vals, 4:i16 max_parts=-1,
     5: string user_name, 6: list<string> group_names) throws(1:NoSuchObjectException o1, 2:MetaException o2)

  list<string> get_partition_names_ps(1:string db_name,
  	2:string tbl_name, 3:list<string> part_vals, 4:i16 max_parts=-1)
  	                   throws(1:MetaException o1, 2:NoSuchObjectException o2)

  // get the partitions matching the given partition filter
  list<Partition> get_partitions_by_filter(1:string db_name 2:string tbl_name
    3:string filter, 4:i16 max_parts=-1)
                       throws(1:MetaException o1, 2:NoSuchObjectException o2)

  // List partitions as PartitionSpec instances.
  list<PartitionSpec> get_part_specs_by_filter(1:string db_name 2:string tbl_name
    3:string filter, 4:i32 max_parts=-1)
                       throws(1:MetaException o1, 2:NoSuchObjectException o2)

  // get the partitions matching the given partition filter
  // unlike get_partitions_by_filter, takes serialized hive expression, and with that can work
  // with any filter (get_partitions_by_filter only works if the filter can be pushed down to JDOQL.
  PartitionsByExprResult get_partitions_by_expr(1:PartitionsByExprRequest req)
                       throws(1:MetaException o1, 2:NoSuchObjectException o2)

  // get the partitions matching the given partition filter
  i32 get_num_partitions_by_filter(1:string db_name 2:string tbl_name 3:string filter)
                       throws(1:MetaException o1, 2:NoSuchObjectException o2)

  // get partitions give a list of partition names
  list<Partition> get_partitions_by_names(1:string db_name 2:string tbl_name 3:list<string> names)
                       throws(1:MetaException o1, 2:NoSuchObjectException o2)

  // changes the partition to the new partition object. partition is identified from the part values
  // in the new_part
  // * See notes on DDL_TIME
  void alter_partition(1:string db_name, 2:string tbl_name, 3:Partition new_part)
                       throws (1:InvalidOperationException o1, 2:MetaException o2)

  // change a list of partitions. All partitions are altered atomically and all
  // prehooks are fired together followed by all post hooks
  void alter_partitions(1:string db_name, 2:string tbl_name, 3:list<Partition> new_parts)
                       throws (1:InvalidOperationException o1, 2:MetaException o2)
  void alter_partitions_with_environment_context(1:string db_name, 2:string tbl_name, 3:list<Partition> new_parts, 4:EnvironmentContext environment_context) throws (1:InvalidOperationException o1, 2:MetaException o2)

  void alter_partition_with_environment_context(1:string db_name,
      2:string tbl_name, 3:Partition new_part,
      4:EnvironmentContext environment_context)
      throws (1:InvalidOperationException o1, 2:MetaException o2)

  // rename the old partition to the new partition object by changing old part values to the part values
  // in the new_part. old partition is identified from part_vals.
  // partition keys in new_part should be the same as those in old partition.
  void rename_partition(1:string db_name, 2:string tbl_name, 3:list<string> part_vals, 4:Partition new_part)
                       throws (1:InvalidOperationException o1, 2:MetaException o2)

  // returns whether or not the partition name is valid based on the value of the config
  // hive.metastore.partition.name.whitelist.pattern
  bool partition_name_has_valid_characters(1:list<string> part_vals, 2:bool throw_exception)
 	throws(1: MetaException o1)

  // gets the value of the configuration key in the metastore server. returns
  // defaultValue if the key does not exist. if the configuration key does not
  // begin with "hive", "mapred", or "hdfs", a ConfigValSecurityException is
  // thrown.
  string get_config_value(1:string name, 2:string defaultValue)
                          throws(1:ConfigValSecurityException o1)

  // converts a partition name into a partition values array
  list<string> partition_name_to_vals(1: string part_name)
                          throws(1: MetaException o1)
  // converts a partition name into a partition specification (a mapping from
  // the partition cols to the values)
  map<string, string> partition_name_to_spec(1: string part_name)
                          throws(1: MetaException o1)

  void markPartitionForEvent(1:string db_name, 2:string tbl_name, 3:map<string,string> part_vals,
                  4:PartitionEventType eventType) throws (1: MetaException o1, 2: NoSuchObjectException o2,
                  3: UnknownDBException o3, 4: UnknownTableException o4, 5: UnknownPartitionException o5,
                  6: InvalidPartitionException o6)
  bool isPartitionMarkedForEvent(1:string db_name, 2:string tbl_name, 3:map<string,string> part_vals,
                  4: PartitionEventType eventType) throws (1: MetaException o1, 2:NoSuchObjectException o2,
                  3: UnknownDBException o3, 4: UnknownTableException o4, 5: UnknownPartitionException o5,
                  6: InvalidPartitionException o6)

  //primary keys and foreign keys
  PrimaryKeysResponse get_primary_keys(1:PrimaryKeysRequest request)
                       throws(1:MetaException o1, 2:NoSuchObjectException o2)
  ForeignKeysResponse get_foreign_keys(1:ForeignKeysRequest request)
                       throws(1:MetaException o1, 2:NoSuchObjectException o2)
  // other constraints
  UniqueConstraintsResponse get_unique_constraints(1:UniqueConstraintsRequest request)
                       throws(1:MetaException o1, 2:NoSuchObjectException o2)
  NotNullConstraintsResponse get_not_null_constraints(1:NotNullConstraintsRequest request)
                       throws(1:MetaException o1, 2:NoSuchObjectException o2)
  DefaultConstraintsResponse get_default_constraints(1:DefaultConstraintsRequest request)
                       throws(1:MetaException o1, 2:NoSuchObjectException o2)
  CheckConstraintsResponse get_check_constraints(1:CheckConstraintsRequest request)
                       throws(1:MetaException o1, 2:NoSuchObjectException o2)

  // column statistics interfaces

  // update APIs persist the column statistics object(s) that are passed in. If statistics already
  // exists for one or more columns, the existing statistics will be overwritten. The update APIs
  // validate that the dbName, tableName, partName, colName[] passed in as part of the ColumnStatistics
  // struct are valid, throws InvalidInputException/NoSuchObjectException if found to be invalid
  bool update_table_column_statistics(1:ColumnStatistics stats_obj) throws (1:NoSuchObjectException o1,
              2:InvalidObjectException o2, 3:MetaException o3, 4:InvalidInputException o4)
  bool update_partition_column_statistics(1:ColumnStatistics stats_obj) throws (1:NoSuchObjectException o1,
              2:InvalidObjectException o2, 3:MetaException o3, 4:InvalidInputException o4)

  // get APIs return the column statistics corresponding to db_name, tbl_name, [part_name], col_name if
  // such statistics exists. If the required statistics doesn't exist, get APIs throw NoSuchObjectException
  // For instance, if get_table_column_statistics is called on a partitioned table for which only
  // partition level column stats exist, get_table_column_statistics will throw NoSuchObjectException
  ColumnStatistics get_table_column_statistics(1:string db_name, 2:string tbl_name, 3:string col_name) throws
              (1:NoSuchObjectException o1, 2:MetaException o2, 3:InvalidInputException o3, 4:InvalidObjectException o4)
  ColumnStatistics get_partition_column_statistics(1:string db_name, 2:string tbl_name, 3:string part_name,
               4:string col_name) throws (1:NoSuchObjectException o1, 2:MetaException o2,
               3:InvalidInputException o3, 4:InvalidObjectException o4)
  TableStatsResult get_table_statistics_req(1:TableStatsRequest request) throws
              (1:NoSuchObjectException o1, 2:MetaException o2)
  PartitionsStatsResult get_partitions_statistics_req(1:PartitionsStatsRequest request) throws
              (1:NoSuchObjectException o1, 2:MetaException o2)
  AggrStats get_aggr_stats_for(1:PartitionsStatsRequest request) throws
              (1:NoSuchObjectException o1, 2:MetaException o2)
  bool set_aggr_stats_for(1:SetPartitionsStatsRequest request) throws
              (1:NoSuchObjectException o1, 2:InvalidObjectException o2, 3:MetaException o3, 4:InvalidInputException o4)


  // delete APIs attempt to delete column statistics, if found, associated with a given db_name, tbl_name, [part_name]
  // and col_name. If the delete API doesn't find the statistics record in the metastore, throws NoSuchObjectException
  // Delete API validates the input and if the input is invalid throws InvalidInputException/InvalidObjectException.
  bool delete_partition_column_statistics(1:string db_name, 2:string tbl_name, 3:string part_name, 4:string col_name) throws
              (1:NoSuchObjectException o1, 2:MetaException o2, 3:InvalidObjectException o3,
               4:InvalidInputException o4)
  bool delete_table_column_statistics(1:string db_name, 2:string tbl_name, 3:string col_name) throws
              (1:NoSuchObjectException o1, 2:MetaException o2, 3:InvalidObjectException o3,
               4:InvalidInputException o4)

  //
  // user-defined functions
  //

  void create_function(1:Function func)
      throws (1:AlreadyExistsException o1,
              2:InvalidObjectException o2,
              3:MetaException o3,
              4:NoSuchObjectException o4)

  void drop_function(1:string dbName, 2:string funcName)
      throws (1:NoSuchObjectException o1, 2:MetaException o3)

  void alter_function(1:string dbName, 2:string funcName, 3:Function newFunc)
      throws (1:InvalidOperationException o1, 2:MetaException o2)

  list<string> get_functions(1:string dbName, 2:string pattern)
      throws (1:MetaException o1)
  Function get_function(1:string dbName, 2:string funcName)
      throws (1:MetaException o1, 2:NoSuchObjectException o2)

  GetAllFunctionsResponse get_all_functions() throws (1:MetaException o1)

  //authorization privileges

  bool create_role(1:Role role) throws(1:MetaException o1)
  bool drop_role(1:string role_name) throws(1:MetaException o1)
  list<string> get_role_names() throws(1:MetaException o1)
  // Deprecated, use grant_revoke_role()
  bool grant_role(1:string role_name, 2:string principal_name, 3:PrincipalType principal_type,
    4:string grantor, 5:PrincipalType grantorType, 6:bool grant_option) throws(1:MetaException o1)
  // Deprecated, use grant_revoke_role()
  bool revoke_role(1:string role_name, 2:string principal_name, 3:PrincipalType principal_type)
                        throws(1:MetaException o1)
  list<Role> list_roles(1:string principal_name, 2:PrincipalType principal_type) throws(1:MetaException o1)
  GrantRevokeRoleResponse grant_revoke_role(1:GrantRevokeRoleRequest request) throws(1:MetaException o1)

  // get all role-grants for users/roles that have been granted the given role
  // Note that in the returned list of RolePrincipalGrants, the roleName is
  // redundant as it would match the role_name argument of this function
  GetPrincipalsInRoleResponse get_principals_in_role(1: GetPrincipalsInRoleRequest request) throws(1:MetaException o1)

  // get grant information of all roles granted to the given principal
  // Note that in the returned list of RolePrincipalGrants, the principal name,type is
  // redundant as it would match the principal name,type arguments of this function
  GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(1: GetRoleGrantsForPrincipalRequest request) throws(1:MetaException o1)

  PrincipalPrivilegeSet get_privilege_set(1:HiveObjectRef hiveObject, 2:string user_name,
    3: list<string> group_names) throws(1:MetaException o1)
  list<HiveObjectPrivilege> list_privileges(1:string principal_name, 2:PrincipalType principal_type,
    3: HiveObjectRef hiveObject) throws(1:MetaException o1)

  // Deprecated, use grant_revoke_privileges()
  bool grant_privileges(1:PrivilegeBag privileges) throws(1:MetaException o1)
  // Deprecated, use grant_revoke_privileges()
  bool revoke_privileges(1:PrivilegeBag privileges) throws(1:MetaException o1)
  GrantRevokePrivilegeResponse grant_revoke_privileges(1:GrantRevokePrivilegeRequest request) throws(1:MetaException o1);
  // Revokes all privileges for the object and adds the newly granted privileges for it.
  GrantRevokePrivilegeResponse refresh_privileges(1:HiveObjectRef objToRefresh, 2:string authorizer, 3:GrantRevokePrivilegeRequest grantRequest) throws(1:MetaException o1);

  // this is used by metastore client to send UGI information to metastore server immediately
  // after setting up a connection.
  list<string> set_ugi(1:string user_name, 2:list<string> group_names) throws (1:MetaException o1)

  //Authentication (delegation token) interfaces

  // get metastore server delegation token for use from the map/reduce tasks to authenticate
  // to metastore server
  string get_delegation_token(1:string token_owner, 2:string renewer_kerberos_principal_name)
    throws (1:MetaException o1)

  // method to renew delegation token obtained from metastore server
  i64 renew_delegation_token(1:string token_str_form) throws (1:MetaException o1)

  // method to cancel delegation token obtained from metastore server
  void cancel_delegation_token(1:string token_str_form) throws (1:MetaException o1)

  // add a delegation token
  bool add_token(1:string token_identifier, 2:string delegation_token)

  // remove a delegation token
  bool remove_token(1:string token_identifier)

  // get a delegation token by identifier
  string get_token(1:string token_identifier)

  // get all delegation token identifiers
  list<string> get_all_token_identifiers()

  // add master key
  i32 add_master_key(1:string key) throws (1:MetaException o1)

  // update master key
  void update_master_key(1:i32 seq_number, 2:string key) throws (1:NoSuchObjectException o1, 2:MetaException o2)

  // remove master key
  bool remove_master_key(1:i32 key_seq)

  // get master keys
  list<string> get_master_keys()

  // Transaction and lock management calls
  // Get just list of open transactions
  GetOpenTxnsResponse get_open_txns()
  // Get list of open transactions with state (open, aborted)
  GetOpenTxnsInfoResponse get_open_txns_info()
  OpenTxnsResponse open_txns(1:OpenTxnRequest rqst)
  void abort_txn(1:AbortTxnRequest rqst) throws (1:NoSuchTxnException o1)
  void abort_txns(1:AbortTxnsRequest rqst) throws (1:NoSuchTxnException o1)
  void commit_txn(1:CommitTxnRequest rqst) throws (1:NoSuchTxnException o1, 2:TxnAbortedException o2)
  void repl_tbl_writeid_state(1: ReplTblWriteIdStateRequest rqst)
  GetValidWriteIdsResponse get_valid_write_ids(1:GetValidWriteIdsRequest rqst)
      throws (1:NoSuchTxnException o1, 2:MetaException o2)
  AllocateTableWriteIdsResponse allocate_table_write_ids(1:AllocateTableWriteIdsRequest rqst)
    throws (1:NoSuchTxnException o1, 2:TxnAbortedException o2, 3:MetaException o3)
  LockResponse lock(1:LockRequest rqst) throws (1:NoSuchTxnException o1, 2:TxnAbortedException o2)
  LockResponse check_lock(1:CheckLockRequest rqst)
    throws (1:NoSuchTxnException o1, 2:TxnAbortedException o2, 3:NoSuchLockException o3)
  void unlock(1:UnlockRequest rqst) throws (1:NoSuchLockException o1, 2:TxnOpenException o2)
  ShowLocksResponse show_locks(1:ShowLocksRequest rqst)
  void heartbeat(1:HeartbeatRequest ids) throws (1:NoSuchLockException o1, 2:NoSuchTxnException o2, 3:TxnAbortedException o3)
  HeartbeatTxnRangeResponse heartbeat_txn_range(1:HeartbeatTxnRangeRequest txns)
  void compact(1:CompactionRequest rqst) 
  CompactionResponse compact2(1:CompactionRequest rqst) 
  ShowCompactResponse show_compact(1:ShowCompactRequest rqst)
  void add_dynamic_partitions(1:AddDynamicPartitions rqst) throws (1:NoSuchTxnException o1, 2:TxnAbortedException o2)

  // Notification logging calls
  NotificationEventResponse get_next_notification(1:NotificationEventRequest rqst) 
  CurrentNotificationEventId get_current_notificationEventId()
  NotificationEventsCountResponse get_notification_events_count(1:NotificationEventsCountRequest rqst)
  FireEventResponse fire_listener_event(1:FireEventRequest rqst)
  void flushCache()

  // Repl Change Management api
  CmRecycleResponse cm_recycle(1:CmRecycleRequest request) throws(1:MetaException o1)

  GetFileMetadataByExprResult get_file_metadata_by_expr(1:GetFileMetadataByExprRequest req)
  GetFileMetadataResult get_file_metadata(1:GetFileMetadataRequest req)
  PutFileMetadataResult put_file_metadata(1:PutFileMetadataRequest req)
  ClearFileMetadataResult clear_file_metadata(1:ClearFileMetadataRequest req)
  CacheFileMetadataResult cache_file_metadata(1:CacheFileMetadataRequest req)

  // Metastore DB properties
  string get_metastore_db_uuid() throws (1:MetaException o1)

  // Workload management API's
  WMCreateResourcePlanResponse create_resource_plan(1:WMCreateResourcePlanRequest request)
      throws(1:AlreadyExistsException o1, 2:InvalidObjectException o2, 3:MetaException o3)

  WMGetResourcePlanResponse get_resource_plan(1:WMGetResourcePlanRequest request)
      throws(1:NoSuchObjectException o1, 2:MetaException o2)

  WMGetActiveResourcePlanResponse get_active_resource_plan(1:WMGetActiveResourcePlanRequest request)
      throws(1:MetaException o2)

  WMGetAllResourcePlanResponse get_all_resource_plans(1:WMGetAllResourcePlanRequest request)
      throws(1:MetaException o1)

  WMAlterResourcePlanResponse alter_resource_plan(1:WMAlterResourcePlanRequest request)
      throws(1:NoSuchObjectException o1, 2:InvalidOperationException o2, 3:MetaException o3)

  WMValidateResourcePlanResponse validate_resource_plan(1:WMValidateResourcePlanRequest request)
      throws(1:NoSuchObjectException o1, 2:MetaException o2)

  WMDropResourcePlanResponse drop_resource_plan(1:WMDropResourcePlanRequest request)
      throws(1:NoSuchObjectException o1, 2:InvalidOperationException o2, 3:MetaException o3)

  WMCreateTriggerResponse create_wm_trigger(1:WMCreateTriggerRequest request)
      throws(1:AlreadyExistsException o1, 2:NoSuchObjectException o2, 3:InvalidObjectException o3, 4:MetaException o4)

  WMAlterTriggerResponse alter_wm_trigger(1:WMAlterTriggerRequest request)
      throws(1:NoSuchObjectException o1, 2:InvalidObjectException o2, 3:MetaException o3)

  WMDropTriggerResponse drop_wm_trigger(1:WMDropTriggerRequest request)
      throws(1:NoSuchObjectException o1, 2:InvalidOperationException o2, 3:MetaException o3)

  WMGetTriggersForResourePlanResponse get_triggers_for_resourceplan(1:WMGetTriggersForResourePlanRequest request)
      throws(1:NoSuchObjectException o1, 2:MetaException o2)

  WMCreatePoolResponse create_wm_pool(1:WMCreatePoolRequest request)
      throws(1:AlreadyExistsException o1, 2:NoSuchObjectException o2, 3:InvalidObjectException o3, 4:MetaException o4)

  WMAlterPoolResponse alter_wm_pool(1:WMAlterPoolRequest request)
      throws(1:AlreadyExistsException o1, 2:NoSuchObjectException o2, 3:InvalidObjectException o3, 4:MetaException o4)

  WMDropPoolResponse drop_wm_pool(1:WMDropPoolRequest request)
      throws(1:NoSuchObjectException o1, 2:InvalidOperationException o2, 3:MetaException o3)

  WMCreateOrUpdateMappingResponse create_or_update_wm_mapping(1:WMCreateOrUpdateMappingRequest request)
      throws(1:AlreadyExistsException o1, 2:NoSuchObjectException o2, 3:InvalidObjectException o3, 4:MetaException o4)

  WMDropMappingResponse drop_wm_mapping(1:WMDropMappingRequest request)
      throws(1:NoSuchObjectException o1, 2:InvalidOperationException o2, 3:MetaException o3)

  WMCreateOrDropTriggerToPoolMappingResponse create_or_drop_wm_trigger_to_pool_mapping(1:WMCreateOrDropTriggerToPoolMappingRequest request)
      throws(1:AlreadyExistsException o1, 2:NoSuchObjectException o2, 3:InvalidObjectException o3, 4:MetaException o4)

  // Schema calls
  void create_ischema(1:ISchema schema) throws(1:AlreadyExistsException o1,
        NoSuchObjectException o2, 3:MetaException o3)
  void alter_ischema(1:AlterISchemaRequest rqst)
        throws(1:NoSuchObjectException o1, 2:MetaException o2)
  ISchema get_ischema(1:ISchemaName name) throws (1:NoSuchObjectException o1, 2:MetaException o2)
  void drop_ischema(1:ISchemaName name)
        throws(1:NoSuchObjectException o1, 2:InvalidOperationException o2, 3:MetaException o3)

  void add_schema_version(1:SchemaVersion schemaVersion)
        throws(1:AlreadyExistsException o1, 2:NoSuchObjectException o2, 3:MetaException o3)
  SchemaVersion get_schema_version(1: SchemaVersionDescriptor schemaVersion)
        throws (1:NoSuchObjectException o1, 2:MetaException o2)
  SchemaVersion get_schema_latest_version(1: ISchemaName schemaName)
        throws (1:NoSuchObjectException o1, 2:MetaException o2)
  list<SchemaVersion> get_schema_all_versions(1: ISchemaName schemaName)
        throws (1:NoSuchObjectException o1, 2:MetaException o2)
  void drop_schema_version(1: SchemaVersionDescriptor schemaVersion)
        throws(1:NoSuchObjectException o1, 2:MetaException o2)
  FindSchemasByColsResp get_schemas_by_cols(1: FindSchemasByColsRqst rqst)
        throws(1:MetaException o1)
  // There is no blanket update of SchemaVersion since it is (mostly) immutable.  The only
  // updates are the specific ones to associate a version with a serde and to change its state
  void map_schema_version_to_serde(1: MapSchemaVersionToSerdeRequest rqst)
        throws(1:NoSuchObjectException o1, 2:MetaException o2)
  void set_schema_version_state(1: SetSchemaVersionStateRequest rqst)
        throws(1:NoSuchObjectException o1, 2:InvalidOperationException o2, 3:MetaException o3)

  void add_serde(1: SerDeInfo serde) throws(1:AlreadyExistsException o1, 2:MetaException o2)
  SerDeInfo get_serde(1: GetSerdeRequest rqst) throws(1:NoSuchObjectException o1, 2:MetaException o2)

  LockResponse get_lock_materialization_rebuild(1: string dbName, 2: string tableName, 3: i64 txnId)
  bool heartbeat_lock_materialization_rebuild(1: string dbName, 2: string tableName, 3: i64 txnId)
  
  void add_runtime_stats(1: RuntimeStat stat) throws(1:MetaException o1)
  list<RuntimeStat> get_runtime_stats(1: GetRuntimeStatsRequest rqst) throws(1:MetaException o1)
}

// * Note about the DDL_TIME: When creating or altering a table or a partition,
// if the DDL_TIME is not set, the current time will be used.

// For storing info about archived partitions in parameters

// Whether the partition is archived
const string IS_ARCHIVED = "is_archived",
// The original location of the partition, before archiving. After archiving,
// this directory will contain the archive. When the partition
// is dropped, this directory will be deleted
const string ORIGINAL_LOCATION = "original_location",

// Whether or not the table is considered immutable - immutable tables can only be
// overwritten or created if unpartitioned, or if partitioned, partitions inside them
// can only be overwritten or created. Immutability supports write-once and replace
// semantics, but not append.
const string IS_IMMUTABLE = "immutable",

// these should be needed only for backward compatibility with filestore
const string META_TABLE_COLUMNS   = "columns",
const string META_TABLE_COLUMN_TYPES   = "columns.types",
const string BUCKET_FIELD_NAME    = "bucket_field_name",
const string BUCKET_COUNT         = "bucket_count",
const string FIELD_TO_DIMENSION   = "field_to_dimension",
const string META_TABLE_NAME      = "name",
const string META_TABLE_DB        = "db",
const string META_TABLE_LOCATION  = "location",
const string META_TABLE_SERDE     = "serde",
const string META_TABLE_PARTITION_COLUMNS = "partition_columns",
const string META_TABLE_PARTITION_COLUMN_TYPES = "partition_columns.types",
const string FILE_INPUT_FORMAT    = "file.inputformat",
const string FILE_OUTPUT_FORMAT   = "file.outputformat",
const string META_TABLE_STORAGE   = "storage_handler",
const string TABLE_IS_TRANSACTIONAL = "transactional",
const string TABLE_NO_AUTO_COMPACT = "no_auto_compaction",
const string TABLE_TRANSACTIONAL_PROPERTIES = "transactional_properties",
const string TABLE_BUCKETING_VERSION = "bucketing_version",

