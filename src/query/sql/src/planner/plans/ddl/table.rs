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

use common_ast::ast::Engine;
use common_catalog::table::NavigationPoint;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::DataField;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;
use common_expression::TableSchemaRef;
use common_meta_app::schema::TableNameIdent;
use common_meta_app::schema::UndropTableReq;
use common_meta_app::storage::StorageParams;

use crate::plans::Plan;

pub type TableOptions = BTreeMap<String, String>;

#[derive(Clone, Debug)]
pub struct CreateTablePlan {
    pub if_not_exists: bool,
    pub tenant: String,
    pub catalog: String,
    pub database: String,
    pub table: String,

    pub schema: TableSchemaRef,
    pub engine: Engine,
    pub storage_params: Option<StorageParams>,
    pub part_prefix: String,
    pub options: TableOptions,
    pub field_default_exprs: Vec<Option<String>>,
    pub field_comments: Vec<String>,
    pub cluster_key: Option<String>,
    pub as_select: Option<Box<Plan>>,
}

impl CreateTablePlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }
}

/// Desc.
#[derive(Clone, Debug, PartialEq, Eq)]
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropTablePlan {
    pub if_exists: bool,
    pub tenant: String,
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VacuumTablePlan {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub option: VacuumTableOption,
}

impl VacuumTablePlan {
    pub fn schema(&self) -> DataSchemaRef {
        if self.option.dry_run.is_some() {
            Arc::new(DataSchema::new(vec![DataField::new(
                "Files",
                DataType::String,
            )]))
        } else {
            Arc::new(DataSchema::empty())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VacuumTableOption {
    pub retain_hours: Option<usize>,
    pub dry_run: Option<()>,
}

/// Optimize.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OptimizeTablePlan {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub action: OptimizeTableAction,
}

impl OptimizeTablePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OptimizeTableAction {
    All,
    Purge(Option<NavigationPoint>),
    CompactBlocks(Option<usize>),
    CompactSegments(Option<usize>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AnalyzeTablePlan {
    pub catalog: String,
    pub database: String,
    pub table: String,
}

impl AnalyzeTablePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

/// Rename.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenameTablePlan {
    pub tenant: String,
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

// Table add column
#[derive(Clone, Debug, PartialEq)]
pub struct AddTableColumnPlan {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub schema: TableSchemaRef,
    pub field_default_exprs: Vec<Option<String>>,
    pub field_comments: Vec<String>,
}

impl AddTableColumnPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

// Table drop column
#[derive(Clone, Debug, PartialEq)]
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

/// Show.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShowCreateTablePlan {
    /// The catalog name
    pub catalog: String,
    /// The database name
    pub database: String,
    /// The table name
    pub table: String,
    /// The table schema
    pub schema: DataSchemaRef,
}

impl ShowCreateTablePlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}

/// Truncate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TruncateTablePlan {
    pub catalog: String,
    pub database: String,
    /// The table name
    pub table: String,
    pub purge: bool,
}

impl TruncateTablePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

/// Undrop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UndropTablePlan {
    pub tenant: String,
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
#[derive(Clone, Debug, PartialEq, Eq)]
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterTableClusterKeyPlan {
    pub tenant: String,
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub cluster_keys: Vec<String>,
}

impl AlterTableClusterKeyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropTableClusterKeyPlan {
    pub tenant: String,
    pub catalog: String,
    pub database: String,
    pub table: String,
}

impl DropTableClusterKeyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}
