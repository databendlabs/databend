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

use std::collections::HashMap;
use std::sync::Arc;

use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_expression::FieldIndex;

use crate::plans::ScalarExpr;
use crate::BindContext;

#[derive(Clone, Debug)]
pub struct UpdatePlan {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub update_list: HashMap<FieldIndex, ScalarExpr>,
    pub selection: Option<ScalarExpr>,
    pub bind_context: Box<BindContext>,
}

impl UpdatePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}
