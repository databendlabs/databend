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

use databend_common_exception::Result;
use databend_common_expression::Expr;
use databend_common_expression::RemoteExpr;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::principal::NullAs;
use databend_storages_common_stage::project_columnar;

use crate::hashable_schema::HashableSchema;

#[derive(Clone)]
pub struct ProjectionFactory {
    pub output_schema: TableSchemaRef,
    default_values: Option<Vec<RemoteExpr>>,
    null_as: NullAs,

    projections: Arc<dashmap::DashMap<HashableSchema, Vec<Expr>>>,
}

impl ProjectionFactory {
    pub fn try_create(
        output_schema: TableSchemaRef,
        default_values: Option<Vec<RemoteExpr>>,
        null_as: NullAs,
    ) -> Result<Self> {
        Ok(Self {
            output_schema,
            default_values,
            null_as,
            projections: Default::default(),
        })
    }
    pub fn get(&self, schema: &HashableSchema, location: &str) -> Result<Vec<Expr>> {
        if let Some(v) = self.projections.get(schema) {
            Ok(v.clone())
        } else {
            let v = project_columnar(
                &schema.table_schema,
                &self.output_schema,
                &self.null_as,
                &self.default_values,
                location,
            )?
            .0;
            self.projections.insert(schema.clone(), v.clone());
            Ok(v)
        }
    }
}
