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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::Deref;
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyerror::func_name;
use chrono::DateTime;
use chrono::Utc;
use databend_common_ast::ast::SnapshotRefType as AstSnapshotRefType;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::TableSchema;
use databend_common_expression::VirtualDataSchema;
use databend_meta_types::MatchSeq;
use databend_meta_types::MetaId;
use maplit::hashmap;

use super::CatalogInfo;
use super::CreateOption;
use super::DatabaseId;
use super::MarkedDeletedIndexMeta;
use crate::schema::constraint::Constraint;
use crate::schema::database_name_ident::DatabaseNameIdent;
use crate::schema::table_niv::TableNIV;
use crate::storage::StorageParams;
use crate::tenant::Tenant;

mod ident;
mod ops;

pub use ident::*;

// serde is required by [`TableInfo`]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Default)]
pub enum DatabaseType {
    #[default]
    NormalDB,
}

impl Display for DatabaseType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            DatabaseType::NormalDB => {
                write!(f, "normal database")
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct TableInfo {
    /// For a temp table,
    /// `ident.seq` is always 0.
    /// `id.table_id` is set as value of `TempTblId`.
    pub ident: TableIdent,

    /// For a table it is `db_name.table_name`.
    /// For a table function, it is `table_name(args)`
    pub desc: String,

    /// `name` is meant to be used with table-function.
    /// Table-function is identified by `name`.
    /// A table in the contrast, can only be identified by table-id.
    pub name: String,

    /// The essential information about a table definition.
    ///
    /// It is about what a table actually is.
    /// `name`, `id` or `version` is not included in the table structure definition.
    pub meta: TableMeta,

    /// The corresponding catalog info of this table.
    pub catalog_info: Arc<CatalogInfo>,

    // table belong to which type of database.
    pub db_type: DatabaseType,
}

impl TableInfo {
    pub fn database_name(&self) -> Result<&str> {
        if self.engine() != "FUSE" {
            return Err(ErrorCode::Internal(format!(
                "Invalid engine: {}",
                self.engine()
            )));
        }
        // desc format is "'db_name'.'table_name'"
        let raw = self
            .desc
            .split('.')
            .next()
            .ok_or_else(|| ErrorCode::Internal(format!("empty desc in table {}", self.name)))?;
        raw.strip_prefix('\'')
            .and_then(|s| s.strip_suffix('\''))
            .ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "unexpected desc format: {} in table {}",
                    raw, self.name
                ))
            })
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct TableStatistics {
    /// Number of rows
    pub number_of_rows: u64,
    // Size of data in bytes
    pub data_bytes: u64,
    /// Size of data compressed in bytes
    pub compressed_data_bytes: u64,
    /// Size of all index data in bytes
    pub index_data_bytes: u64,
    /// Size of bloom index in bytes
    pub bloom_index_size: Option<u64>,
    /// Size of ngram index in bytes
    pub ngram_index_size: Option<u64>,
    /// Size of inverted index in bytes
    pub inverted_index_size: Option<u64>,
    /// Size of vector index in bytes
    pub vector_index_size: Option<u64>,
    /// Size of virtual column in bytes
    pub virtual_column_size: Option<u64>,
    /// number of segments
    pub number_of_segments: Option<u64>,

    /// number of blocks
    pub number_of_blocks: Option<u64>,
}

/// Iceberg table partition
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub enum TablePartition {
    Identity { columns: Vec<String> },
}

/// The essential state that defines what a table is.
///
/// It is what a meta store just needs to save.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct TableMeta {
    pub schema: Arc<TableSchema>,
    pub engine: String,
    pub engine_options: BTreeMap<String, String>,
    pub storage_params: Option<StorageParams>,
    pub part_prefix: String,
    pub options: BTreeMap<String, String>,
    /// Deprecated, will be removed later.
    /// Original cluster key as a string. Use `cluster_key_v2` instead.
    pub cluster_key: Option<String>,
    /// Cluster key for the main branch, including an id.
    /// The `u32` is the cluster key id of the main branch, uniquely identifying each version.
    pub cluster_key_v2: Option<(u32, String)>,
    /// Global monotonically increasing sequence for cluster key changes, to
    /// ensuring a unique identifier for each version of cluster key.
    ///
    /// This sequence is shared across the main branch and all branches, and is
    /// incremented whenever a cluster key is created or altered on any branch.
    /// It remains unchanged when the cluster key is dropped.
    pub cluster_key_seq: u32,
    pub created_on: DateTime<Utc>,
    pub updated_on: DateTime<Utc>,
    pub comment: String,
    pub field_comments: Vec<String>,
    pub virtual_schema: Option<VirtualDataSchema>,

    // if used in CreateTableReq, this field MUST set to None.
    pub drop_on: Option<DateTime<Utc>>,
    pub statistics: TableStatistics,
    // shared by share_id
    pub shared_by: BTreeSet<u64>,
    // should be discard
    pub column_mask_policy: Option<BTreeMap<String, String>>,
    // ColumnId always equals the first value in SecurityPolicyColumnMap::columns_ids
    // One table can have multiple masking policies
    pub column_mask_policy_columns_ids: BTreeMap<ColumnId, SecurityPolicyColumnMap>,
    // One table only has an unique row access policy
    // should be discard
    pub row_access_policy: Option<String>,
    // One table only has an unique row access policy
    // store policy id and which column apply row access policy
    pub row_access_policy_columns_ids: Option<SecurityPolicyColumnMap>,
    pub indexes: BTreeMap<String, TableIndex>,
    pub constraints: BTreeMap<String, Constraint>,

    pub refs: BTreeMap<String, SnapshotRef>,
}

// Inspired by iceberg(https://github.com/apache/iceberg-rust/blob/main/crates/iceberg/src/spec/snapshot.rs#L443-L449)
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct SnapshotRef {
    /// The unique id of the reference.
    /// It is allocated from a global sequence and is unique cluster-wide.
    pub id: u64,
    /// After this timestamp, the reference becomes inactive.
    pub expire_at: Option<DateTime<Utc>>,
    /// The type of the reference.
    pub typ: SnapshotRefType,
    /// The location of the snapshot that this reference points to.
    pub loc: String,
}

#[derive(
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    num_derive::FromPrimitive,
    Hash,
)]
pub enum SnapshotRefType {
    Branch = 0,
    Tag = 1,
}

impl From<&AstSnapshotRefType> for SnapshotRefType {
    fn from(v: &AstSnapshotRefType) -> Self {
        match v {
            AstSnapshotRefType::Branch => SnapshotRefType::Branch,
            AstSnapshotRefType::Tag => SnapshotRefType::Tag,
        }
    }
}

impl Display for SnapshotRefType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapshotRefType::Branch => write!(f, "BRANCH"),
            SnapshotRefType::Tag => write!(f, "TAG"),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct BranchInfo {
    pub name: String,
    pub info: SnapshotRef,
    // Branch schema is derived from its snapshot
    // and should not be persisted in table meta.
    pub schema: Arc<TableSchema>,
    pub cluster_key_meta: Option<(u32, String)>,
}

impl BranchInfo {
    pub fn branch_name(&self) -> &str {
        &self.name
    }

    pub fn branch_id(&self) -> u64 {
        self.info.id
    }

    pub fn branch_type(&self) -> SnapshotRefType {
        self.info.typ
    }
}

impl TableMeta {
    /// Returns the cluster key defined on the main branch, if any.
    pub fn cluster_key_meta(&self) -> Option<(u32, String)> {
        // - Prefer `cluster_key_v2` if present (branch-aware)
        // - Otherwise fallback to old `cluster_key` + global `cluster_key_seq`
        self.cluster_key_v2.clone().or_else(|| {
            self.cluster_key
                .as_ref()
                .map(|k| (self.cluster_key_seq, k.clone()))
        })
    }

    pub fn cluster_key_str(&self) -> Option<&str> {
        if let Some((_, ref key)) = self.cluster_key_v2 {
            Some(key.as_str())
        } else {
            self.cluster_key.as_deref()
        }
    }

    pub fn cluster_key_id(&self) -> Option<u32> {
        if let Some((id, _)) = &self.cluster_key_v2 {
            Some(*id)
        } else if self.cluster_key.is_some() {
            Some(self.cluster_key_seq)
        } else {
            None
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct SecurityPolicyColumnMap {
    pub policy_id: u64,
    pub columns_ids: Vec<ColumnId>,
}

impl SecurityPolicyColumnMap {
    pub fn new(policy_id: u64, field_indexes: Vec<ColumnId>) -> Self {
        Self {
            policy_id,
            columns_ids: field_indexes,
        }
    }
}

#[derive(
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Debug,
    Eq,
    PartialEq,
    num_derive::FromPrimitive,
    Hash,
)]
pub enum TableIndexType {
    Inverted = 0,
    Ngram = 1,
    Vector = 2,
    Spatial = 3,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct TableIndex {
    pub index_type: TableIndexType,
    pub name: String,
    pub column_ids: Vec<u32>,
    // if true, index will create after data written to databend,
    // no need execute refresh index manually.
    pub sync_creation: bool,
    // if the index columns or options change,
    // the index data needs to be regenerated,
    // version is used to identify each change.
    pub version: String,
    // index options specify the index configs, like tokenizer.
    pub options: BTreeMap<String, String>,
}

impl TableInfo {
    /// Create a TableInfo with only db, table, schema
    pub fn simple(db: &str, table: &str, schema: Arc<TableSchema>) -> TableInfo {
        TableInfo {
            desc: format!("'{}'.'{}'", db, table),
            name: table.to_string(),
            meta: TableMeta {
                schema,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Deprecated: use `new_full()`. This method sets default values for some fields.
    pub fn new(db_name: &str, table_name: &str, ident: TableIdent, meta: TableMeta) -> TableInfo {
        TableInfo {
            ident,
            desc: format!("'{}'.'{}'", db_name, table_name),
            name: table_name.to_string(),
            meta,
            ..Default::default()
        }
    }

    pub fn new_full(
        db_name: &str,
        table_name: &str,
        ident: TableIdent,
        meta: TableMeta,
        catalog_info: Arc<CatalogInfo>,
        db_type: DatabaseType,
    ) -> TableInfo {
        TableInfo {
            ident,
            desc: format!("'{}'.'{}'", db_name, table_name),
            name: table_name.to_string(),
            meta,
            catalog_info,
            db_type,
        }
    }

    pub fn schema(&self) -> Arc<TableSchema> {
        self.meta.schema.clone()
    }

    pub fn options(&self) -> &BTreeMap<String, String> {
        &self.meta.options
    }

    pub fn options_mut(&mut self) -> &mut BTreeMap<String, String> {
        &mut self.meta.options
    }

    pub fn catalog(&self) -> &str {
        &self.catalog_info.name_ident.catalog_name
    }

    pub fn engine(&self) -> &str {
        &self.meta.engine
    }

    pub fn engine_options(&self) -> &BTreeMap<String, String> {
        &self.meta.engine_options
    }

    pub fn field_comments(&self) -> &Vec<String> {
        &self.meta.field_comments
    }

    #[must_use]
    pub fn set_schema(mut self, schema: Arc<TableSchema>) -> TableInfo {
        self.meta.schema = schema;
        self
    }

    /// Returns the cluster key defined on the main branch, if any.
    pub fn cluster_key(&self) -> Option<(u32, String)> {
        self.meta.cluster_key_meta()
    }

    pub fn get_option<T: FromStr>(&self, opt_key: &str, default: T) -> T {
        self.options()
            .get(opt_key)
            .and_then(|s| s.parse::<T>().ok())
            .unwrap_or(default)
    }

    pub fn get_table_ref(&self, name: &str) -> Result<&SnapshotRef> {
        let Some(table_ref) = self.meta.refs.get(name) else {
            return Err(ErrorCode::UnknownReference(format!(
                "Unknown reference '{}' in table {}",
                name, self.desc
            )));
        };
        if table_ref.expire_at.is_some_and(|v| v < Utc::now()) {
            return Err(ErrorCode::ReferenceExpired(format!(
                "{} '{}' in table {} is expired",
                table_ref.typ, name, self.desc,
            )));
        }
        Ok(table_ref)
    }
}

impl Default for TablePartition {
    fn default() -> Self {
        TablePartition::Identity { columns: vec![] }
    }
}

impl Display for TablePartition {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            TablePartition::Identity { columns } => {
                write!(f, "Partition Transform Identity: {:?}", columns)
            }
        }
    }
}

impl Default for TableMeta {
    fn default() -> Self {
        TableMeta {
            schema: Arc::new(TableSchema::empty()),
            engine: "FUSE".to_string(),
            engine_options: BTreeMap::new(),
            storage_params: None,
            part_prefix: "".to_string(),
            options: BTreeMap::new(),
            cluster_key: None,
            cluster_key_v2: None,
            cluster_key_seq: 0,
            created_on: Utc::now(),
            updated_on: Utc::now(),
            comment: "".to_string(),
            field_comments: vec![],
            virtual_schema: Default::default(),
            drop_on: None,
            statistics: Default::default(),
            shared_by: BTreeSet::new(),
            column_mask_policy: None,
            column_mask_policy_columns_ids: BTreeMap::new(),
            row_access_policy: None,
            row_access_policy_columns_ids: None,
            indexes: BTreeMap::new(),
            constraints: BTreeMap::new(),
            refs: BTreeMap::new(),
        }
    }
}

impl Display for TableMeta {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Engine: {}={:?}, Schema: {:?}, Options: {:?}, FieldComments: {:?} Indexes: {:?} CreatedOn: {:?} DropOn: {:?}",
            self.engine,
            self.engine_options,
            self.schema,
            self.options,
            self.field_comments,
            self.indexes,
            self.created_on,
            self.drop_on,
        )
    }
}

impl Display for TableInfo {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "DB.Table: {}, Table: {}-{}, Engine: {}",
            self.desc, self.name, self.ident, self.meta.engine
        )
    }
}

impl Display for TableIndexType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            TableIndexType::Inverted => {
                write!(f, "INVERTED")
            }
            TableIndexType::Ngram => {
                write!(f, "NGRAM")
            }
            TableIndexType::Vector => {
                write!(f, "VECTOR")
            }
            TableIndexType::Spatial => {
                write!(f, "SPATIAL")
            }
        }
    }
}

/// Save table name id list history.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct TableIdList {
    pub id_list: Vec<u64>,
}

impl TableIdList {
    pub fn new() -> TableIdList {
        TableIdList::default()
    }

    pub fn new_with_ids(ids: impl IntoIterator<Item = u64>) -> TableIdList {
        TableIdList {
            id_list: ids.into_iter().collect(),
        }
    }

    pub fn len(&self) -> usize {
        self.id_list.len()
    }

    pub fn id_list(&self) -> &Vec<u64> {
        &self.id_list
    }

    pub fn append(&mut self, table_id: u64) {
        self.id_list.push(table_id);
    }

    pub fn is_empty(&self) -> bool {
        self.id_list.is_empty()
    }

    pub fn pop(&mut self) -> Option<u64> {
        self.id_list.pop()
    }

    pub fn last(&self) -> Option<&u64> {
        self.id_list.last()
    }
}

impl Display for TableIdList {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "DB.Table id list: {:?}", self.id_list)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTableReq {
    pub create_option: CreateOption,
    pub catalog_name: Option<String>,
    pub name_ident: TableNameIdent,
    pub table_meta: TableMeta,

    /// Set it to true if a dropped table needs to be created,
    ///
    /// since [CreateOption] is used by various scenarios, we use
    /// this dedicated flag to mark this behavior.
    ///
    /// currently used in atomic CTAS.
    pub as_dropped: bool,

    /// Iceberg table properties
    pub table_properties: Option<BTreeMap<String, String>>,
    /// Iceberg table partition
    pub table_partition: Option<TablePartition>,
}

impl CreateTableReq {
    pub fn tenant(&self) -> &Tenant {
        &self.name_ident.tenant
    }
    pub fn db_name(&self) -> &str {
        &self.name_ident.db_name
    }
    pub fn table_name(&self) -> &str {
        &self.name_ident.table_name
    }
}

impl Display for CreateTableReq {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self.create_option {
            CreateOption::Create => write!(
                f,
                "create_table:{}/{}-{}={}",
                self.tenant().tenant_name(),
                self.db_name(),
                self.table_name(),
                self.table_meta
            ),
            CreateOption::CreateIfNotExists => write!(
                f,
                "create_table_if_not_exists:{}/{}-{}={}",
                self.tenant().tenant_name(),
                self.db_name(),
                self.table_name(),
                self.table_meta
            ),
            CreateOption::CreateOrReplace => write!(
                f,
                "create_or_replace_table:{}/{}-{}={}",
                self.tenant().tenant_name(),
                self.db_name(),
                self.table_name(),
                self.table_meta
            ),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateTableReply {
    pub table_id: u64,
    pub table_id_seq: Option<u64>,
    pub db_id: u64,
    pub new_table: bool,
    // (db id, removed table id)
    pub spec_vec: Option<(u64, u64)>,
    pub prev_table_id: Option<u64>,
    pub orphan_table_name: Option<String>,
}

/// Drop table by id.
///
/// Dropping a table requires just `table_id`, but when dropping a table, it also needs to update
/// the count of tables belonging to a tenant, which require tenant information.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropTableByIdReq {
    pub if_exists: bool,

    pub tenant: Tenant,

    pub tb_id: MetaId,

    pub table_name: String,

    pub db_id: MetaId,

    pub db_name: String,

    pub engine: String,

    pub temp_prefix: String,
}

impl DropTableByIdReq {
    pub fn tb_id(&self) -> MetaId {
        self.tb_id
    }
}

impl Display for DropTableByIdReq {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "drop_table_by_id(if_exists={}):{}",
            self.if_exists,
            self.tb_id(),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct DropTableReply {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitTableMetaReq {
    pub name_ident: TableNameIdent,
    pub db_id: MetaId,
    pub table_id: MetaId,
    pub prev_table_id: Option<MetaId>,
    pub orphan_table_name: Option<String>,
}

impl CommitTableMetaReq {
    pub fn table_id(&self) -> MetaId {
        self.table_id
    }
}

impl Display for CommitTableMetaReq {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "commit_table_meta:{}", self.table_id(),)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitTableMetaReply {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UndropTableReq {
    pub name_ident: TableNameIdent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UndropTableByIdReq {
    pub name_ident: TableNameIdent,
    pub db_id: MetaId,
    pub table_id: MetaId,
    pub table_id_seq: u64,
    // Indicates whether to forcefully replace an existing table with the same name, if it exists.
    pub force_replace: bool,
}

impl UndropTableReq {
    pub fn tenant(&self) -> &Tenant {
        &self.name_ident.tenant
    }
    pub fn db_name(&self) -> &str {
        &self.name_ident.db_name
    }
    pub fn table_name(&self) -> &str {
        &self.name_ident.table_name
    }
}

impl Display for UndropTableReq {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "undrop_table:{}/{}-{}",
            self.tenant().tenant_name(),
            self.db_name(),
            self.table_name()
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenameTableReq {
    pub if_exists: bool,
    pub name_ident: TableNameIdent,
    pub new_db_name: String,
    pub new_table_name: String,
}

impl RenameTableReq {
    pub fn tenant(&self) -> &Tenant {
        &self.name_ident.tenant
    }
    pub fn db_name(&self) -> &str {
        &self.name_ident.db_name
    }
    pub fn table_name(&self) -> &str {
        &self.name_ident.table_name
    }
}

impl Display for SwapTableReq {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "swap_table:{}/{}-{}<==>{}/{}-{}",
            self.origin_table.tenant.tenant_name(),
            self.origin_table.db_name,
            self.origin_table.table_name,
            self.origin_table.tenant.tenant_name(),
            self.origin_table.db_name,
            self.target_table_name
        )
    }
}

impl Display for RenameTableReq {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "rename_table:{}/{}-{}=>{}-{}",
            self.tenant().tenant_name(),
            self.db_name(),
            self.table_name(),
            self.new_db_name,
            self.new_table_name
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenameTableReply {
    pub table_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SwapTableReq {
    pub if_exists: bool,
    pub origin_table: TableNameIdent,
    pub target_table_name: String,
}

impl SwapTableReq {
    pub fn tenant(&self) -> &Tenant {
        &self.origin_table.tenant
    }
}

// Keep this structure for future compatibility, even if currently empty.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SwapTableReply {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpsertTableOptionReq {
    pub table_id: u64,
    pub seq: MatchSeq,

    /// Add or remove options
    ///
    /// Some(String): add or update an option.
    /// None: delete an option.
    pub options: HashMap<String, Option<String>>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct UpdateStreamMetaReq {
    pub stream_id: u64,
    pub seq: MatchSeq,
    pub options: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateTableMetaReq {
    pub table_id: u64,
    pub seq: MatchSeq,
    pub new_table_meta: TableMeta,
    pub base_snapshot_location: Option<String>,
    /// Optional optimistic LVT check.
    pub lvt_check: Option<TableLvtCheck>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableLvtCheck {
    pub tenant: Tenant,
    pub time: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateTempTableReq {
    pub table_id: u64,
    pub desc: String,
    pub new_table_meta: TableMeta,
    pub copied_files: BTreeMap<String, TableCopiedFileInfo>,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct UpdateMultiTableMetaReq {
    pub update_table_metas: Vec<(UpdateTableMetaReq, TableInfo)>,
    pub copied_files: Vec<(u64, UpsertTableCopiedFileReq)>,
    pub update_stream_metas: Vec<UpdateStreamMetaReq>,
    pub deduplicated_labels: Vec<String>,
    pub update_temp_tables: Vec<UpdateTempTableReq>,
}

impl UpdateMultiTableMetaReq {
    pub fn is_empty(&self) -> bool {
        self.update_table_metas.is_empty()
            && self.copied_files.is_empty()
            && self.update_stream_metas.is_empty()
            && self.deduplicated_labels.is_empty()
            && self.update_temp_tables.is_empty()
    }
}

/// The result of updating multiple table meta
///
/// If update fails due to table version mismatch, the `Err` will contain the (table id, seq , table meta)s that fail to update.
pub type UpdateMultiTableMetaResult =
    std::result::Result<UpdateTableMetaReply, Vec<(u64, u64, TableMeta)>>;

impl UpsertTableOptionReq {
    pub fn new(
        table_ident: &TableIdent,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> UpsertTableOptionReq {
        UpsertTableOptionReq {
            table_id: table_ident.table_id,
            seq: MatchSeq::Exact(table_ident.seq),
            options: hashmap! {key.into() => Some(value.into())},
        }
    }
}

impl Display for UpsertTableOptionReq {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "upsert-table-options: table-id:{}({:?}) = {:?}",
            self.table_id, self.seq, self.options
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetTableColumnMaskPolicyReq {
    pub tenant: Tenant,
    pub table_id: u64,
    pub seq: MatchSeq,
    pub action: SetSecurityPolicyAction,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetTableColumnMaskPolicyReply {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SetSecurityPolicyAction {
    // new policy name
    Set(u64, Vec<ColumnId>),
    // old policy name
    Unset(u64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetTableRowAccessPolicyReq {
    pub tenant: Tenant,
    pub table_id: u64,
    pub action: SetSecurityPolicyAction,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetTableRowAccessPolicyReply {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpsertTableOptionReply {}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct UpdateTableMetaReply {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTableIndexReq {
    pub create_option: CreateOption,
    pub index_type: TableIndexType,
    pub tenant: Tenant,
    pub table_id: u64,
    pub name: String,
    pub column_ids: Vec<u32>,
    pub sync_creation: bool,
    pub options: BTreeMap<String, String>,
}

impl Display for CreateTableIndexReq {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let typ = match self.create_option {
            CreateOption::Create => "create_table_index",
            CreateOption::CreateIfNotExists => "create_table_index_if_not_exists",
            CreateOption::CreateOrReplace => "create_or_replace_table_index",
        };

        write!(
            f,
            "{}: {} ColumnIds: {:?}, SyncCreation: {:?}, Options: {:?}",
            typ, self.name, self.column_ids, self.sync_creation, self.options,
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropTableIndexReq {
    pub index_type: TableIndexType,
    pub tenant: Tenant,
    pub if_exists: bool,
    pub table_id: u64,
    pub name: String,
}

impl Display for DropTableIndexReq {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "drop_table_index(if_exists={}):{}/{}",
            self.if_exists, self.table_id, self.name,
        )
    }
}

/// Maps table_id to a vector of (index_name, index_version, marked_deleted_index_meta) pairs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetMarkedDeletedTableIndexesReply {
    pub table_indexes: HashMap<u64, Vec<(IndexName, IndexVersion, MarkedDeletedIndexMeta)>>,
}

pub type IndexName = String;
pub type IndexVersion = String;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetTableReq {
    pub inner: TableNameIdent,
}

impl Deref for GetTableReq {
    type Target = TableNameIdent;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl From<(&str, &str, &str)> for GetTableReq {
    /// For testing only
    fn from(db_table: (&str, &str, &str)) -> Self {
        let tenant = Tenant::new_or_err(db_table.0, func_name!()).unwrap();
        Self::new(&tenant, db_table.1, db_table.2)
    }
}

impl GetTableReq {
    pub fn new(tenant: &Tenant, db_name: impl ToString, table_name: impl ToString) -> GetTableReq {
        GetTableReq {
            inner: TableNameIdent::new(tenant.clone(), db_name, table_name),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListTableReq {
    pub tenant: Tenant,
    pub database_id: DatabaseId,
}

impl ListTableReq {
    pub fn new(tenant: &Tenant, database_id: DatabaseId) -> ListTableReq {
        ListTableReq {
            tenant: tenant.clone(),
            database_id,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListDroppedTableReq {
    pub tenant: Tenant,

    /// If `database_name` is None, choose all tables in all databases.
    /// Otherwise, choose only tables in this database.
    pub database_name: Option<String>,

    /// The time range in which the database/table will be returned.
    /// choose only tables/databases dropped before this boundary time.
    /// It can include non-dropped tables/databases with `None..Some()`
    pub drop_time_range: Range<Option<DateTime<Utc>>>,

    pub limit: Option<usize>,
}

impl ListDroppedTableReq {
    pub fn new(tenant: &Tenant) -> ListDroppedTableReq {
        let rng_start = Some(DateTime::<Utc>::MIN_UTC);
        let rng_end = Some(DateTime::<Utc>::MAX_UTC);
        ListDroppedTableReq {
            tenant: tenant.clone(),
            database_name: None,
            drop_time_range: rng_start..rng_end,
            limit: None,
        }
    }

    pub fn with_db(self, db_name: impl ToString) -> Self {
        Self {
            database_name: Some(db_name.to_string()),
            ..self
        }
    }

    pub fn with_retention_boundary(self, d: DateTime<Utc>) -> Self {
        let rng_start = Some(DateTime::<Utc>::MIN_UTC);
        let rng_end = Some(d);
        Self {
            drop_time_range: rng_start..rng_end,
            ..self
        }
    }

    pub fn with_limit(self, limit: usize) -> Self {
        Self {
            limit: Some(limit),
            ..self
        }
    }

    pub fn new4(
        tenant: &Tenant,
        database_name: Option<impl ToString>,
        retention_boundary: Option<DateTime<Utc>>,
        limit: Option<usize>,
    ) -> ListDroppedTableReq {
        let rng_start = Some(DateTime::<Utc>::MIN_UTC);
        let rng_end = if let Some(b) = retention_boundary {
            Some(b)
        } else {
            Some(DateTime::<Utc>::MAX_UTC)
        };
        ListDroppedTableReq {
            tenant: tenant.clone(),
            database_name: database_name.map(|s| s.to_string()),
            drop_time_range: rng_start..rng_end,
            limit,
        }
    }

    pub fn database_name(&self) -> Option<&str> {
        self.database_name.as_deref()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DroppedId {
    Db { db_id: u64, db_name: String },
    Table { name: DBIdTableName, id: TableId },
}

impl From<TableNIV> for DroppedId {
    fn from(value: TableNIV) -> Self {
        let (name, id, _) = value.unpack();
        Self::Table { name, id }
    }
}

impl DroppedId {
    pub fn new_table_name_id(name: DBIdTableName, id: TableId) -> DroppedId {
        DroppedId::Table { name, id }
    }

    pub fn new_table(db_id: u64, table_id: u64, table_name: impl ToString) -> DroppedId {
        DroppedId::Table {
            name: DBIdTableName::new(db_id, table_name),
            id: TableId::new(table_id),
        }
    }

    /// Build a string contains essential information for comparison.
    ///
    /// Only used for testing.
    pub fn cmp_key(&self) -> String {
        match self {
            DroppedId::Db { db_id, db_name, .. } => format!("db:{}-{}", db_id, db_name),
            DroppedId::Table { name, id } => format!("table:{:?}-{:?}", name, id),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListDroppedTableResp {
    /// The **database_name, (name, id, value)** of a table to vacuum.
    pub vacuum_tables: Vec<(DatabaseNameIdent, TableNIV)>,
    pub drop_ids: Vec<DroppedId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcDroppedTableReq {
    pub tenant: Tenant,
    pub catalog: String,
    pub drop_ids: Vec<DroppedId>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TableIdToName {
    pub table_id: u64,
}

impl Display for TableIdToName {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "TableIdToName{{{}}}", self.table_id)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct TableCopiedFileNameIdent {
    pub table_id: u64,
    pub file: String,
}

impl fmt::Display for TableCopiedFileNameIdent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "TableCopiedFileNameIdent{{table_id:{}, file:{}}}",
            self.table_id, self.file
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct TableCopiedFileInfo {
    pub etag: Option<String>,
    pub content_length: u64,
    pub last_modified: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetTableCopiedFileReq {
    pub table_id: u64,
    pub files: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetTableCopiedFileReply {
    pub file_info: BTreeMap<String, TableCopiedFileInfo>,
}

pub type ListTableCopiedFileReply = GetTableCopiedFileReply;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpsertTableCopiedFileReq {
    pub file_info: BTreeMap<String, TableCopiedFileInfo>,
    /// If not None, specifies the time-to-live for the keys.
    pub ttl: Option<Duration>,
    /// If there is already existing key, ignore inserting
    pub insert_if_not_exists: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpsertTableCopiedFileReply {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TruncateTableReq {
    pub table_id: u64,
    /// Specify the max number copied file to delete in every sub-transaction.
    ///
    /// By default it use `DEFAULT_MGET_SIZE=256`
    pub batch_size: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TruncateTableReply {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmptyProto {}

mod kvapi_key_impl {
    use databend_meta_kvapi::kvapi;
    use databend_meta_kvapi::kvapi::Key;
    use databend_meta_kvapi::kvapi::KeyBuilder;
    use databend_meta_kvapi::kvapi::KeyError;
    use databend_meta_kvapi::kvapi::KeyParser;

    use crate::schema::DBIdTableName;
    use crate::schema::DatabaseId;
    use crate::schema::TableCopiedFileInfo;
    use crate::schema::TableCopiedFileNameIdent;
    use crate::schema::TableId;
    use crate::schema::TableIdHistoryIdent;
    use crate::schema::TableIdList;
    use crate::schema::TableIdToName;
    use crate::schema::TableMeta;

    impl kvapi::KeyCodec for DBIdTableName {
        fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
            b.push_u64(self.db_id).push_str(&self.table_name)
        }

        fn decode_key(p: &mut KeyParser) -> Result<Self, KeyError> {
            let db_id = p.next_u64()?;
            let table_name = p.next_str()?;
            Ok(Self { db_id, table_name })
        }
    }

    /// "__fd_table/<db_id>/<tb_name>"
    impl kvapi::Key for DBIdTableName {
        const PREFIX: &'static str = "__fd_table";

        type ValueType = TableId;

        fn parent(&self) -> Option<String> {
            Some(DatabaseId::new(self.db_id).to_string_key())
        }
    }

    impl kvapi::KeyCodec for TableIdToName {
        fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
            b.push_u64(self.table_id)
        }

        fn decode_key(p: &mut KeyParser) -> Result<Self, KeyError> {
            let table_id = p.next_u64()?;
            Ok(Self { table_id })
        }
    }

    /// "__fd_table_id_to_name/<table_id> -> DBIdTableName"
    impl kvapi::Key for TableIdToName {
        const PREFIX: &'static str = "__fd_table_id_to_name";

        type ValueType = DBIdTableName;

        fn parent(&self) -> Option<String> {
            Some(TableId::new(self.table_id).to_string_key())
        }
    }

    impl kvapi::KeyCodec for TableId {
        fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
            b.push_u64(self.table_id)
        }

        fn decode_key(p: &mut KeyParser) -> Result<Self, KeyError> {
            let table_id = p.next_u64()?;
            Ok(Self { table_id })
        }
    }

    /// "__fd_table_by_id/<tb_id> -> TableMeta"
    impl kvapi::Key for TableId {
        const PREFIX: &'static str = "__fd_table_by_id";

        type ValueType = TableMeta;

        fn parent(&self) -> Option<String> {
            None
        }
    }

    impl kvapi::KeyCodec for TableIdHistoryIdent {
        fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
            b.push_u64(self.database_id).push_str(&self.table_name)
        }

        fn decode_key(b: &mut KeyParser) -> Result<Self, kvapi::KeyError> {
            let db_id = b.next_u64()?;
            let table_name = b.next_str()?;
            Ok(Self {
                database_id: db_id,
                table_name,
            })
        }
    }

    /// "_fd_table_id_list/<db_id>/<tb_name> -> id_list"
    impl kvapi::Key for TableIdHistoryIdent {
        const PREFIX: &'static str = "__fd_table_id_list";

        type ValueType = TableIdList;

        fn parent(&self) -> Option<String> {
            Some(DatabaseId::new(self.database_id).to_string_key())
        }
    }

    impl kvapi::KeyCodec for TableCopiedFileNameIdent {
        fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
            // TODO: file is not escaped!!!
            //       There already are non escaped data stored on disk.
            //       We can not change it anymore.
            b.push_u64(self.table_id).push_raw(&self.file)
        }

        fn decode_key(p: &mut KeyParser) -> Result<Self, kvapi::KeyError> {
            let table_id = p.next_u64()?;
            let file = p.tail_raw()?.to_string();
            Ok(Self { table_id, file })
        }
    }

    // __fd_table_copied_files/table_id/file_name -> TableCopiedFileInfo
    impl kvapi::Key for TableCopiedFileNameIdent {
        const PREFIX: &'static str = "__fd_table_copied_files";

        type ValueType = TableCopiedFileInfo;

        fn parent(&self) -> Option<String> {
            Some(TableId::new(self.table_id).to_string_key())
        }
    }

    impl kvapi::Value for TableId {
        type KeyType = DBIdTableName;
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            [self.to_string_key()]
        }
    }

    impl kvapi::Value for DBIdTableName {
        type KeyType = TableIdToName;
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }

    impl kvapi::Value for TableMeta {
        type KeyType = TableId;
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }

    impl kvapi::Value for TableIdList {
        type KeyType = TableIdHistoryIdent;
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            self.id_list
                .iter()
                .map(|id| TableId::new(*id).to_string_key())
        }
    }

    impl kvapi::Value for TableCopiedFileInfo {
        type KeyType = TableCopiedFileNameIdent;
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_kvapi::kvapi;
    use databend_meta_kvapi::kvapi::Key;

    use crate::schema::TableCopiedFileNameIdent;
    use crate::schema::TableMeta;

    #[test]
    fn test_table_copied_file_name_ident_conversion() -> Result<(), kvapi::KeyError> {
        // test with a key has a file has multi path
        {
            let name = TableCopiedFileNameIdent {
                table_id: 2,
                file: "/path/to/file".to_owned(),
            };

            let key = name.to_string_key();
            assert_eq!(
                key,
                format!(
                    "{}/{}/{}",
                    TableCopiedFileNameIdent::PREFIX,
                    name.table_id,
                    name.file,
                )
            );
            let from = TableCopiedFileNameIdent::from_str_key(&key)?;
            assert_eq!(from, name);
        }

        // test with a key has only 2 sub-path
        {
            let key = format!("{}/{}", TableCopiedFileNameIdent::PREFIX, 2,);
            let res = TableCopiedFileNameIdent::from_str_key(&key);
            assert!(res.is_err());
            let err = res.unwrap_err();
            assert_eq!(err, kvapi::KeyError::WrongNumberOfSegments {
                expect: 3,
                got: key,
            });
        }

        // test with a key has 5 sub-path but an empty file string
        {
            let key = format!("{}/{}/{}", TableCopiedFileNameIdent::PREFIX, 2, "");
            let res = TableCopiedFileNameIdent::from_str_key(&key)?;
            assert_eq!(res, TableCopiedFileNameIdent {
                table_id: 2,
                file: "".to_string(),
            });
        }
        Ok(())
    }

    #[test]
    fn test_cluster_key_meta() -> databend_common_exception::Result<()> {
        {
            let table_meta = TableMeta {
                cluster_key: None,
                cluster_key_v2: None,
                cluster_key_seq: 2,
                ..Default::default()
            };

            assert_eq!(table_meta.cluster_key_meta(), None);
            assert_eq!(table_meta.cluster_key_id(), None);
            assert_eq!(table_meta.cluster_key_str(), None);
        }

        // only cluster_key
        {
            let table_meta = TableMeta {
                cluster_key: Some("(a)".to_string()),
                cluster_key_v2: None,
                cluster_key_seq: 2,
                ..Default::default()
            };
            assert_eq!(table_meta.cluster_key_meta(), Some((2, "(a)".to_string())));
            assert_eq!(table_meta.cluster_key_id(), Some(2));
            assert_eq!(table_meta.cluster_key_str(), Some("(a)"));
        }

        // cluster_key_v2
        {
            let table_meta = TableMeta {
                cluster_key: None,
                cluster_key_v2: Some((1, "(a)".to_string())),
                cluster_key_seq: 2,
                ..Default::default()
            };
            assert_eq!(table_meta.cluster_key_meta(), Some((1, "(a)".to_string())));
            assert_eq!(table_meta.cluster_key_id(), Some(1));
            assert_eq!(table_meta.cluster_key_str(), Some("(a)"));
        }

        // both cluster_key and cluster_key_v2
        {
            let table_meta = TableMeta {
                cluster_key: Some("(a)".to_string()),
                cluster_key_v2: Some((1, "(a)".to_string())),
                cluster_key_seq: 2,
                ..Default::default()
            };
            assert_eq!(table_meta.cluster_key_meta(), Some((1, "(a)".to_string())));
            assert_eq!(table_meta.cluster_key_id(), Some(1));
            assert_eq!(table_meta.cluster_key_str(), Some("(a)"));
        }
        Ok(())
    }
}
