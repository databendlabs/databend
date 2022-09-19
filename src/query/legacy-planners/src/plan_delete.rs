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
use common_meta_app::schema::TableIdent;

use crate::Projection;

/// # TODO
///
/// From @xuanwo
///
/// Ideally, we need to use `Scalar` in DeletePlan.selection. But we met a
/// cycle deps here. So we have to change `selection` in String first, and
/// change into `Scalar` when our `Planner` has been moved out.
///
/// At this stage, DeletePlan's selection expr will be parsed twice:
///
/// - Parsed during `bind` to get column index and projection index.
/// - Parsed during `execution` to get the correct columns
///
/// It's an ugly but necessary price to pay. Without this, we would sink in
/// hell forever.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DeletePlan {
    pub catalog_name: String,
    pub database_name: String,
    pub table_name: String,
    pub table_id: TableIdent,
    pub selection: Option<String>,
    pub projection: Projection,
}

impl DeletePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}
