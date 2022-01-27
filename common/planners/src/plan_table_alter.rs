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

use std::sync::Arc;

use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub enum AlterTableOperation {
    RenameColumn {
        old_column_name: String,
        new_column_name: String,
    },

    DropColumn {
        column_name: String,
    },

    // sqlparser support adding only one column at a time
    AddColumn {
        column_name: String,
        data_type: DataType,
    },
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct AlterTablePlan {
    pub database_name: String,
    pub table_name: String,
    pub operation: AlterTableOperation,
}

impl AlterTablePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}
