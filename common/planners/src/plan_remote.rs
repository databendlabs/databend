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
use common_datavalues::{DataSchema, DataSchemaRef};

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct V1RemotePlan {
    pub schema: DataSchemaRef,
    pub query_id: String,
    pub stage_id: String,
    pub stream_id: String,
    pub fetch_nodes: Vec<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct V2RemotePlan {
    schema: DataSchemaRef,
    pub receive_query_id: String,
    pub receive_fragment_id: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub enum RemotePlan {
    V1(V1RemotePlan),
    V2(V2RemotePlan),
}

impl RemotePlan {
    pub fn create_v1(
        schema: DataSchemaRef,
        query_id: String,
        stage_id: String,
        stream_id: String,
        fetch_nodes: Vec<String>,
    ) -> RemotePlan {
        RemotePlan::V1(V1RemotePlan { schema, query_id, stage_id, stream_id, fetch_nodes })
    }

    pub fn create_v2(schema: DataSchemaRef, query_id: String, fragment: String) -> RemotePlan {
        RemotePlan::V2(V2RemotePlan {
            schema,
            receive_query_id: query_id,
            receive_fragment_id: fragment,
        })
    }

    pub fn schema(&self) -> DataSchemaRef {
        match self {
            Self::V1(plan) => plan.schema.clone(),
            Self::V2(plan) => plan.schema.clone(),
        }
    }
}