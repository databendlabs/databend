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

use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;

use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::plans::CacheSource;
use crate::ColumnSet;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CacheScan {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub cache_source: CacheSource,
    pub output_schema: DataSchemaRef,
}

impl CacheScan {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.output_schema.clone())
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_cache_scan(
        &mut self,
        scan: &crate::plans::CacheScan,
        required: ColumnSet,
    ) -> Result<PhysicalPlan> {
        // 1. Prune unused Columns.
        let used: ColumnSet = required.intersection(&scan.columns).cloned().collect();
        let (cache_source, fields) = if used == scan.columns {
            (scan.cache_source.clone(), scan.schema.fields().clone())
        } else {
            let new_scan = scan.prune_columns(used);
            (
                new_scan.cache_source.clone(),
                new_scan.schema.fields().clone(),
            )
        };
        // 2. Build physical plan.
        Ok(PhysicalPlan::CacheScan(CacheScan {
            plan_id: 0,
            cache_source,
            output_schema: DataSchemaRefExt::create(fields),
        }))
    }
}
