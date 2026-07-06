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
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataSchemaRef;
use databend_common_management::RoleApi;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::tenant::Tenant;
use databend_common_sql::parse_computed_expr;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;

use crate::sessions::TableContext;

pub async fn revoke_table_ownership(
    tenant: &Tenant,
    catalog_name: &str,
    db_id: u64,
    table_id: u64,
) -> Result<()> {
    let role_api = UserApiProvider::instance().role_api(tenant);
    let owner_object = OwnershipObject::Table {
        catalog_name: catalog_name.to_string(),
        db_id,
        table_id,
    };
    role_api.revoke_ownership(&owner_object).await?;
    RoleCacheManager::instance().invalidate_cache(tenant);
    Ok(())
}

pub fn check_referenced_computed_columns(
    ctx: Arc<dyn TableContext>,
    schema: DataSchemaRef,
    column: &str,
) -> Result<()> {
    for f in schema.fields() {
        if let Some(computed_expr) = f.computed_expr() {
            let expr = match computed_expr {
                ComputedExpr::Stored(expr) => expr,
                ComputedExpr::Virtual(expr) => expr,
            };
            match parse_computed_expr(ctx.clone(), schema.clone(), expr) {
                Ok(expr) => {
                    if expr.data_type() != f.data_type() {
                        return Err(ErrorCode::ColumnReferencedByComputedColumn(format!(
                            "expected computed column expression have type {}, but got type {}, may caused by modify column `{}`.",
                            f.data_type(),
                            expr.data_type(),
                            column,
                        )));
                    }
                }
                Err(_) => {
                    return Err(ErrorCode::ColumnReferencedByComputedColumn(format!(
                        "column `{}` is referenced by computed column `{}`",
                        column,
                        &f.name()
                    )));
                }
            }
        }
    }
    Ok(())
}
