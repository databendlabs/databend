// Copyright 2020 Datafuse Labs.
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

use common_datavalues::DataSchemaRef;
use common_meta_types::MetaId;

use crate::PlanNode;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct InsertIntoPlan {
    pub db_name: String,
    pub tbl_name: String,
    pub tbl_id: MetaId,
    pub schema: DataSchemaRef,

    pub select_plan: Option<Box<PlanNode>>,
    pub values_opt: Option<String>,
}

impl PartialEq for InsertIntoPlan {
    fn eq(&self, other: &Self) -> bool {
        self.db_name == other.db_name
            && self.tbl_name == other.tbl_name
            && self.schema == other.schema
    }
}

impl InsertIntoPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }

    pub fn insert_select(
        db: String,
        table: String,
        table_meta_id: MetaId,
        schema: DataSchemaRef,
        select_plan: PlanNode,
    ) -> InsertIntoPlan {
        InsertIntoPlan {
            db_name: db,
            tbl_name: table,
            tbl_id: table_meta_id,
            schema,
            select_plan: Some(Box::new(select_plan)),
            values_opt: None,
        }
    }

    pub fn insert_values(
        db: String,
        table: String,
        table_meta_id: MetaId,
        schema: DataSchemaRef,
        values: String,
    ) -> InsertIntoPlan {
        InsertIntoPlan {
            db_name: db,
            tbl_name: table,
            tbl_id: table_meta_id,
            schema,
            select_plan: None,
            values_opt: Some(values),
        }
    }
}
