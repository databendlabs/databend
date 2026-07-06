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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::Planner;
use databend_common_sql::plans::Plan;
use databend_storages_common_table_meta::table::OPT_KEY_MATERIALIZED_VIEW;
use databend_storages_common_table_meta::table::OPT_KEY_MATERIALIZED_VIEW_QUERY;
use databend_storages_common_table_meta::table::OPT_KEY_MATERIALIZED_VIEW_QUERY_CATALOG;
use databend_storages_common_table_meta::table::OPT_KEY_MATERIALIZED_VIEW_QUERY_DATABASE;

use crate::sessions::QueryContext;
use crate::sessions::TableContextTableAccess;

pub async fn plan_sql_in_mv_context(
    ctx: &Arc<QueryContext>,
    query_catalog: String,
    query_database: String,
    query_text: &str,
) -> Result<Plan> {
    let original_catalog = ctx.get_current_catalog();
    let original_database = ctx.get_current_database();

    let plan_result = async {
        ctx.set_current_catalog(query_catalog).await?;
        ctx.set_current_database(query_database).await?;

        let mut planner = Planner::new(ctx.clone());
        planner.plan_sql(query_text).await
    }
    .await;

    let restore_catalog_result = ctx.set_current_catalog(original_catalog).await;
    let restore_database_result = ctx.set_current_database(original_database).await;
    restore_catalog_result?;
    restore_database_result?;

    let (plan, _) = plan_result?;
    Ok(plan)
}

pub async fn plan_materialized_view_query(
    ctx: &Arc<QueryContext>,
    catalog_name: &str,
    db_name: &str,
    view_name: &str,
) -> Result<Plan> {
    let table = ctx.get_table(catalog_name, db_name, view_name).await?;
    let options = table.options();
    if !options.contains_key(OPT_KEY_MATERIALIZED_VIEW) {
        return Err(ErrorCode::TableEngineNotSupported(format!(
            "`{}`.`{}` is not a MATERIALIZED VIEW",
            db_name, view_name
        )));
    }

    let query_text = options
        .get(OPT_KEY_MATERIALIZED_VIEW_QUERY)
        .ok_or_else(|| {
            ErrorCode::Internal(format!(
                "materialized view `{}`.`{}` is missing refresh query definition",
                db_name, view_name
            ))
        })?
        .clone();
    let query_catalog = options
        .get(OPT_KEY_MATERIALIZED_VIEW_QUERY_CATALOG)
        .cloned()
        .unwrap_or_else(|| catalog_name.to_string());
    let query_database = options
        .get(OPT_KEY_MATERIALIZED_VIEW_QUERY_DATABASE)
        .cloned()
        .unwrap_or_else(|| db_name.to_string());

    plan_sql_in_mv_context(ctx, query_catalog, query_database, &query_text).await
}
