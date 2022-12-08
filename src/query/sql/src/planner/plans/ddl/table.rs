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

use std::collections::BTreeMap;
use std::sync::Arc;

use common_ast::ast::Engine;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::ToDataType;
use common_meta_app::schema::DropTableReq;
use common_meta_app::schema::TableNameIdent;
use common_meta_app::schema::UndropTableReq;
use common_storage::StorageParams;

use crate::plans::Plan;
use crate::plans::Scalar;

pub type TableOptions = BTreeMap<String, String>;

#[derive(Clone, Debug)]
pub struct CreateTablePlanV2 {
    pub if_not_exists: bool,
    pub tenant: String,
    pub catalog: String,
    pub database: String,
    pub table: String,

    pub schema: DataSchemaRef,
    pub engine: Engine,
    pub storage_params: Option<StorageParams>,
    pub options: TableOptions,
    pub field_default_exprs: Vec<Option<Scalar>>,
    pub field_comments: Vec<String>,
    pub cluster_key: Option<String>,
    pub as_select: Option<Box<Plan>>,
}

impl CreateTablePlanV2 {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
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

impl From<DropTablePlan> for DropTableReq {
    fn from(p: DropTablePlan) -> Self {
        DropTableReq {
            if_exists: p.if_exists,
            name_ident: TableNameIdent {
                tenant: p.tenant,
                db_name: p.database,
                table_name: p.table,
            },
        }
    }
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

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum OptimizeTableAction {
    All,
    Purge,
    Statistic,
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
    pub entities: Vec<RenameTableEntity>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenameTableEntity {
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
            u8::to_data_type(),
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
