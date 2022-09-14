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

use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq)]
pub struct EmptyPlan {
    pub schema: DataSchemaRef,
    pub is_cluster: bool,
}

impl EmptyPlan {
    pub fn create() -> Self {
        EmptyPlan {
            schema: DataSchemaRef::new(DataSchema::empty()),
            is_cluster: false,
        }
    }

    pub fn cluster() -> Self {
        EmptyPlan {
            schema: DataSchemaRef::new(DataSchema::empty()),
            is_cluster: true,
        }
    }

    pub fn create_with_schema(schema: DataSchemaRef) -> Self {
        EmptyPlan {
            schema,
            is_cluster: false,
        }
    }
}

impl EmptyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}
