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

use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;

use super::Plan;
use crate::MetadataRef;
use crate::ScalarExpr;

#[derive(Clone, Debug)]
pub struct InsertMultiTable {
    pub overwrite: bool,
    pub is_first: bool,
    pub input_source: Plan,
    pub whens: Vec<When>,
    pub opt_else: Option<Else>,
    pub intos: Vec<Into>,
    pub target_tables: Vec<(u64, (String, String))>, /* (table_id, (database, table)), statement returns result set in this order */
    pub meta_data: MetadataRef,
}

#[derive(Clone, Debug)]
pub struct When {
    pub condition: ScalarExpr,
    pub intos: Vec<Into>,
}

#[derive(Clone, Debug)]
pub struct Into {
    pub catalog: String,
    pub database: String,
    pub table: String,
    // eval scalar and project subquery's output with VALUES ( source_col_name [ , ... ] ) (if exists)
    pub source_scalar_exprs: Option<Vec<ScalarExpr>>,
    //  cast to ( target_col_name [ , ... ] )'s schema (if exists) or target table's schema
    pub casted_schema: DataSchemaRef,
}

#[derive(Clone, Debug)]
pub struct Else {
    pub intos: Vec<Into>,
}

impl InsertMultiTable {
    pub fn schema(&self) -> DataSchemaRef {
        let mut fields = vec![];
        for (_, (db, tbl)) in self.target_tables.iter() {
            let field_name = format!("number of rows inserted into {}.{}", db, tbl);
            fields.push(DataField::new(
                &field_name,
                DataType::Number(NumberDataType::UInt64),
            ));
        }
        DataSchemaRefExt::create(fields)
    }
}
