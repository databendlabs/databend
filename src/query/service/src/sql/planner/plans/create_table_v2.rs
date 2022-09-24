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

use common_ast::ast::Engine;
use common_datavalues::DataSchemaRef;

use crate::sql::plans::Plan;
use crate::sql::plans::Scalar;

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
