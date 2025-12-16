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
use std::sync::Arc;
use std::time::Duration;

use databend_common_ast::ast::Engine;
use databend_common_ast::ast::Identifier;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_meta_app::schema::Constraint;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::schema::UndropTableReq;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::tenant::Tenant;
use databend_common_pipeline::core::SharedLockGuard;

use crate::plans::Plan;

pub type TableOptions = BTreeMap<String, String>;

#[derive(Clone, Debug)]
pub struct CreateTablePlan {
    pub create_option: CreateOption,
    pub tenant: Tenant,
    pub catalog: String,
    pub database: String,
    pub table: String,

    pub schema: TableSchemaRef,
    pub engine: Engine,
    pub engine_options: TableOptions,
    pub storage_params: Option<StorageParams>,
    pub options: TableOptions,
    pub table_properties: Option<TableOptions>,
    pub table_partition: Option<Vec<String>>,
    pub field_comments: Vec<String>,
    pub cluster_key: Option<String>,
    pub as_select: Option<Box<Plan>>,
    pub table_indexes: Option<BTreeMap<String, TableIndex>>,
    pub table_constraints: Option<BTreeMap<String, Constraint>>,

    pub attached_columns: Option<Vec<Identifier>>,
}

impl CreateTablePlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }
}

/// Desc.
#[derive(Clone, Debug)]
pub struct DescribeTablePlan {
    pub catalog: String,
    pub database: String,
    /// The table name.
    pub table: String,
    /// The schema description of the output.
    pub schema: DataSchemaRef,
}

impl DescribeTablePlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}

/// Drop.
#[derive(Clone, Debug)]
pub struct DropTablePlan {
    pub if_exists: bool,
    pub tenant: Tenant,
    pub catalog: String,
    pub database: String,
    /// The table name
    pub table: String,
    pub all: bool,
}

impl DropTablePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

/// Vacuum
#[derive(Clone, Debug)]
pub struct VacuumTablePlan {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub option: VacuumTableOption,
}

impl VacuumTablePlan {
    pub fn schema(&self) -> DataSchemaRef {
        if let Some(summary) = self.option.dry_run {
            if summary {
                Arc::new(DataSchema::new(vec![
                    DataField::new("total_files", DataType::Number(NumberDataType::UInt64)),
                    DataField::new("total_size", DataType::Number(NumberDataType::UInt64)),
                ]))
            } else {
                Arc::new(DataSchema::new(vec![
                    DataField::new("file", DataType::String),
                    DataField::new("file_size", DataType::Number(NumberDataType::UInt64)),
                ]))
            }
        } else {
            Arc::new(DataSchema::new(vec![
                DataField::new("snapshot_files", DataType::Number(NumberDataType::UInt64)),
                DataField::new("snapshot_size", DataType::Number(NumberDataType::UInt64)),
                DataField::new("segments_files", DataType::Number(NumberDataType::UInt64)),
                DataField::new("segments_size", DataType::Number(NumberDataType::UInt64)),
                DataField::new("block_files", DataType::Number(NumberDataType::UInt64)),
                DataField::new("block_size", DataType::Number(NumberDataType::UInt64)),
                DataField::new("index_files", DataType::Number(NumberDataType::UInt64)),
                DataField::new("index_size", DataType::Number(NumberDataType::UInt64)),
                DataField::new("total_files", DataType::Number(NumberDataType::UInt64)),
                DataField::new("total_size", DataType::Number(NumberDataType::UInt64)),
            ]))
        }
    }
}

/// Vacuum drop table
#[derive(Clone, Debug)]
pub struct VacuumDropTablePlan {
    pub catalog: String,
    pub database: String,
    pub option: VacuumDropTableOption,
}

impl VacuumDropTablePlan {
    pub fn schema(&self) -> DataSchemaRef {
        if let Some(summary) = self.option.dry_run {
            if summary {
                Arc::new(DataSchema::new(vec![
                    DataField::new("table", DataType::String),
                    DataField::new("total_files", DataType::Number(NumberDataType::UInt64)),
                    DataField::new("total_size", DataType::Number(NumberDataType::UInt64)),
                ]))
            } else {
                Arc::new(DataSchema::new(vec![
                    DataField::new("table", DataType::String),
                    DataField::new("file", DataType::String),
                    DataField::new("file_size", DataType::Number(NumberDataType::UInt64)),
                ]))
            }
        } else {
            Arc::new(DataSchema::new(vec![
                DataField::new(
                    "success_tables_count",
                    DataType::Number(NumberDataType::UInt64),
                ),
                DataField::new(
                    "failed_tables_count",
                    DataType::Number(NumberDataType::UInt64),
                ),
            ]))
        }
    }
}

#[derive(Clone, Debug)]
pub struct VacuumTemporaryFilesPlan {
    pub limit: Option<u64>,
    pub retain: Option<Duration>,
}

impl crate::plans::VacuumTemporaryFilesPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::new(vec![
            DataField::new("spill_files", DataType::Number(NumberDataType::UInt64)),
            DataField::new(
                "temp_table_sessions",
                DataType::Number(NumberDataType::UInt64),
            ),
        ]))
    }
}

#[derive(Debug, Clone)]
pub struct VacuumDropTableOption {
    // Some(true) means dry run with summary option
    pub dry_run: Option<bool>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct VacuumTableOption {
    pub dry_run: Option<bool>,
}

#[derive(Clone, Debug)]
pub struct AnalyzeTablePlan {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub no_scan: bool,
}

impl AnalyzeTablePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

/// Rename.
#[derive(Clone, Debug)]
pub struct RenameTablePlan {
    pub tenant: Tenant,
    pub if_exists: bool,
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub new_database: String,
    pub new_table: String,
}

impl RenameTablePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

/// Swap table names.
#[derive(Clone, Debug)]
pub struct SwapTablePlan {
    pub tenant: Tenant,
    pub if_exists: bool,
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub target_table: String,
}

impl SwapTablePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

/// Modify table comment.
#[derive(Clone, Debug)]
pub struct ModifyTableCommentPlan {
    pub if_exists: bool,
    pub new_comment: String,
    pub catalog: String,
    pub database: String,
    pub table: String,
}

impl ModifyTableCommentPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

/// Modify table connection.
#[derive(Clone, Debug)]
pub struct ModifyTableConnectionPlan {
    pub new_connection: BTreeMap<String, String>,
    pub catalog: String,
    pub database: String,
    pub table: String,
}

impl ModifyTableConnectionPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

/// SetOptions
#[derive(Clone, Debug)]
pub struct SetOptionsPlan {
    pub set_options: TableOptions,
    pub catalog: String,
    pub database: String,
    pub table: String,
}

impl SetOptionsPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

#[derive(Clone, Debug)]
pub struct UnsetOptionsPlan {
    pub options: Vec<String>,
    pub catalog: String,
    pub database: String,
    pub table: String,
}

impl UnsetOptionsPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

// Table add column
#[derive(Clone, Debug)]
pub struct AddTableColumnPlan {
    pub tenant: Tenant,
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub field: TableField,
    pub comment: String,
    pub option: AddColumnOption,
    pub is_deterministic: bool,
    pub is_nextval: bool,
    pub is_autoincrement: bool,
}

impl AddTableColumnPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

#[derive(Clone, Debug)]
pub enum AddColumnOption {
    First,
    After(String),
    End,
}

// Table rename column
#[derive(Clone, Debug)]
pub struct RenameTableColumnPlan {
    pub tenant: Tenant,
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub schema: TableSchema,
    pub old_column: String,
    pub new_column: String,
}

impl RenameTableColumnPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

// Table drop column
#[derive(Clone, Debug)]
pub struct DropTableColumnPlan {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub column: String,
}

impl DropTableColumnPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

// ModifyColumnAction after name resolved, used in ModifyTableColumnPlan
#[derive(Debug, Clone)]
pub enum ModifyColumnAction {
    // (masking policy name, using columns)
    SetMaskingPolicy(String, Vec<String>),
    // column name
    UnsetMaskingPolicy(String),
    // modify column table field, field comments
    SetDataType(Vec<(TableField, String)>),
    // column name
    ConvertStoredComputedColumn(String),
    // modify column table field, field comments
    Comment(Vec<(TableField, String)>),
}

// Table modify column
#[derive(Clone)]
pub struct ModifyTableColumnPlan {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub action: ModifyColumnAction,
    pub lock_guard: Option<SharedLockGuard>,
}

impl ModifyTableColumnPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

impl std::fmt::Debug for ModifyTableColumnPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("ModifyTableColumn")
            .field("catalog", &self.catalog)
            .field("database", &self.database)
            .field("table", &self.table)
            .field("action", &self.action)
            .finish()
    }
}

/// Show.
#[derive(Clone, Debug)]
pub struct ShowCreateTablePlan {
    /// The catalog name
    pub catalog: String,
    /// The database name
    pub database: String,
    /// The table name
    pub table: String,
    /// The table schema
    pub schema: DataSchemaRef,
    pub with_quoted_ident: bool,
}

impl ShowCreateTablePlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}

/// Truncate.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct TruncateTablePlan {
    pub catalog: String,
    pub database: String,
    /// The table name
    pub table: String,
}

impl TruncateTablePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum TruncateMode {
    // Truncate and keep the historical data.
    Normal,
    // Delete the data, used for delete operation.
    Delete,
    // Truncate and purge the historical data.
    DropAll,
}

/// Undrop.
#[derive(Clone, Debug)]
pub struct UndropTablePlan {
    pub tenant: Tenant,
    pub catalog: String,
    pub database: String,
    pub table: String,
}

impl UndropTablePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

/// The table name
impl From<UndropTablePlan> for UndropTableReq {
    fn from(p: UndropTablePlan) -> Self {
        UndropTableReq {
            name_ident: TableNameIdent {
                tenant: p.tenant,
                db_name: p.database,
                table_name: p.table,
            },
        }
    }
}

/// Exists table.
#[derive(Clone, Debug)]
pub struct ExistsTablePlan {
    pub catalog: String,
    pub database: String,
    pub table: String,
}

impl ExistsTablePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::new(vec![DataField::new(
            "result",
            DataType::Number(NumberDataType::UInt8),
        )]))
    }
}

/// Cluster key.
#[derive(Clone, Debug)]
pub struct AlterTableClusterKeyPlan {
    pub tenant: Tenant,
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub cluster_keys: Vec<String>,
    pub cluster_type: String,
}

impl AlterTableClusterKeyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

#[derive(Clone, Debug)]
pub struct DropTableClusterKeyPlan {
    pub tenant: Tenant,
    pub catalog: String,
    pub database: String,
    pub table: String,
}

impl DropTableClusterKeyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

#[derive(Clone, Debug)]
pub struct RefreshTableCachePlan {
    pub tenant: Tenant,
    pub catalog: String,
    pub database: String,
    pub table: String,
}

impl RefreshTableCachePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

#[derive(Clone, Debug)]
pub struct AddTableRowAccessPolicyPlan {
    pub tenant: Tenant,
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub columns: Vec<String>,
    pub policy: String,
}

#[derive(Clone, Debug)]
pub struct DropTableRowAccessPolicyPlan {
    pub tenant: Tenant,
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub policy: String,
}

#[derive(Clone, Debug)]
pub struct DropAllTableRowAccessPoliciesPlan {
    pub tenant: Tenant,
    pub catalog: String,
    pub database: String,
    pub table: String,
}
