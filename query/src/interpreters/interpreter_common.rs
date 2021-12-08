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

use common_exception::Result;
use common_meta_types::GrantObject;
use common_planners::PlanNode;

use crate::catalogs::Catalog;
use crate::optimizers::Optimizers;
use crate::sessions::QueryContext;

pub fn apply_plan_rewrite(mut optimizer: Optimizers, plan: &PlanNode) -> Result<PlanNode> {
    optimizer.optimize(plan)
}

pub async fn grant_object_exists_or_err(
    ctx: &Arc<QueryContext>,
    object: &GrantObject,
) -> Result<()> {
    let catalog = ctx.get_catalog();

    match &object {
        GrantObject::Table(database_name, table_name) => {
            if !catalog.exists_table(database_name, table_name).await? {
                return Err(common_exception::ErrorCode::UnknownTable(format!(
                    "table {}.{} not exists",
                    database_name, table_name,
                )));
            }
        }
        GrantObject::Database(database_name) => {
            if !catalog.exists_database(database_name).await? {
                return Err(common_exception::ErrorCode::UnknownDatabase(format!(
                    "database {} not exists",
                    database_name,
                )));
            }
        }
        GrantObject::Global => (),
    }

    Ok(())
}
