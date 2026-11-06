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

use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_api::MaterializedViewApi;
use databend_common_meta_app::schema::is_materialized_view_engine;
use databend_common_sql::plans::MaintenanceTarget;
use databend_common_users::UserApiProvider;

use crate::meta_service_error;
use crate::sessions::QueryContext;
use crate::sessions::TableContextLicense;
use crate::sessions::TableContextTableAccess;

pub fn check_materialized_view_license(ctx: &QueryContext, engine: &str) -> Result<()> {
    if is_materialized_view_engine(engine) {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(ctx.get_license_key(), Feature::MaterializedView)?;
    }
    Ok(())
}

/// Reject attaching a row access or masking policy to a materialized-view source.
///
/// MV storage is computed without reader policies, so a source with any dependent MV, including
/// an invalid one, cannot get a policy. Drop the MVs first.
pub async fn check_policy_target_without_materialized_view(
    ctx: &QueryContext,
    table: &dyn Table,
) -> Result<()> {
    if UserApiProvider::instance()
        .get_meta_store_client()
        .has_mvs_by_source_table_id(&ctx.get_tenant(), table.get_id())
        .await
        .map_err(meta_service_error)?
    {
        return Err(ErrorCode::AlterTableError(format!(
            "Cannot attach a row access or masking policy to table '{}' because one or more materialized views depend on it; drop the materialized views first",
            table.name()
        )));
    }
    Ok(())
}

pub fn check_maintenance_target(table: &dyn Table, target: &MaintenanceTarget) -> Result<()> {
    match target {
        MaintenanceTarget::Table => table.check_mutable(),
        MaintenanceTarget::MaterializedView { table_id } => {
            if table.get_id() != *table_id || !is_materialized_view_engine(table.engine()) {
                return Err(ErrorCode::InvalidOperation(format!(
                    "Materialized view maintenance target changed: expected table id {}, found '{}'(id {}, engine {})",
                    table_id,
                    table.name(),
                    table.get_id(),
                    table.engine()
                )));
            }
            Ok(())
        }
    }
}
