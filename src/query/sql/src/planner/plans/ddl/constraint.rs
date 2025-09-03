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

use std::sync::Arc;

use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_meta_app::schema::Constraint;
use databend_common_meta_app::tenant::Tenant;

// Table add constraint
#[derive(Clone, Debug)]
pub struct AddTableConstraintPlan {
    pub tenant: Tenant,
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub constraint_name: String,
    pub constraint: Constraint,
}

impl AddTableConstraintPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

// Table drop constraint
#[derive(Clone, Debug)]
pub struct DropTableConstraintPlan {
    pub tenant: Tenant,
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub constraint_name: String,
}

impl DropTableConstraintPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}
