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

use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_meta_app::principal::UserDefinedFunction;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateUDFPlan {
    pub if_not_exists: bool,
    pub udf: UserDefinedFunction,
}

impl CreateUDFPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterUDFPlan {
    pub udf: UserDefinedFunction,
}

impl AlterUDFPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropUDFPlan {
    pub if_exists: bool,
    pub name: String,
}

impl DropUDFPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}
