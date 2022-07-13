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

use std::collections::BTreeMap;

use common_datavalues::DataSchemaRef;
use common_meta_app::schema::CreateTableReq;
use common_meta_app::schema::TableMeta;
use common_meta_app::schema::TableNameIdent;

use crate::sql::plans::Plan;

pub type TableOptions = BTreeMap<String, String>;

// Replace `PlanNode` with `Plan`
#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct CreateTablePlanV2 {
    pub if_not_exists: bool,
    pub tenant: String,
    /// The catalog name
    pub catalog: String,
    pub database: String,
    /// The table name
    pub table: String,

    pub table_meta: TableMeta,

    pub cluster_keys: Vec<String>,
    #[serde(skip)]
    pub as_select: Option<Box<Plan>>,
}

impl From<CreateTablePlanV2> for CreateTableReq {
    fn from(p: CreateTablePlanV2) -> Self {
        CreateTableReq {
            if_not_exists: p.if_not_exists,
            name_ident: TableNameIdent {
                tenant: p.tenant,
                db_name: p.database,
                table_name: p.table,
            },
            table_meta: p.table_meta,
        }
    }
}

impl CreateTablePlanV2 {
    pub fn schema(&self) -> DataSchemaRef {
        self.table_meta.schema.clone()
    }

    pub fn options(&self) -> &BTreeMap<String, String> {
        &self.table_meta.options
    }

    pub fn engine(&self) -> &str {
        &self.table_meta.engine
    }

    pub fn as_select(&self) -> &Option<Box<Plan>> {
        &self.as_select
    }
}

impl std::fmt::Debug for CreateTablePlanV2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreateTablePlanV2")
            .field("if_not_exists", &self.if_not_exists)
            .field("tenant", &self.tenant)
            .field("catalog", &self.catalog)
            .field("database", &self.database)
            .field("table", &self.table)
            .field("cluster_keys", &self.cluster_keys)
            .finish()
    }
}
