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
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::UserPrivilegeType;

/// Table function arguments are not query table sources, so their shared tables
/// need the same SELECT and ownership checks as a direct table read.
pub async fn check_shared_table_select(
    ctx: &dyn TableContext,
    catalog_name: &str,
    database_name: &str,
    table: &dyn Table,
) -> Result<()> {
    if !table.get_table_info().is_shared() {
        return Ok(());
    }

    let named_object = GrantObject::Table(
        catalog_name.to_string(),
        database_name.to_string(),
        table.name().to_string(),
    );
    match ctx
        .validate_privilege(&named_object, UserPrivilegeType::Select, false)
        .await
    {
        Ok(()) => return Ok(()),
        Err(err) if err.code() == ErrorCode::PERMISSION_DENIED => (),
        Err(err) => return Err(err),
    }

    // Shared table metadata carries the provider's database ID. Grants and
    // ownership belong to the consumer's shared database instead.
    let database_id = ctx
        .get_catalog(catalog_name)
        .await?
        .get_database(&ctx.get_tenant(), database_name)
        .await?
        .get_db_info()
        .database_id
        .db_id;
    let id_object = GrantObject::TableById(catalog_name.to_string(), database_id, table.get_id());
    match ctx
        .validate_privilege(&id_object, UserPrivilegeType::Select, false)
        .await
    {
        Ok(()) => return Ok(()),
        Err(err) if err.code() == ErrorCode::PERMISSION_DENIED => (),
        Err(err) => return Err(err),
    }

    for object in [
        OwnershipObject::Table {
            catalog_name: catalog_name.to_string(),
            db_id: database_id,
            table_id: table.get_id(),
        },
        OwnershipObject::Database {
            catalog_name: catalog_name.to_string(),
            db_id: database_id,
        },
    ] {
        if ctx.has_ownership(&object, false).await? {
            return Ok(());
        }
    }

    Err(ErrorCode::PermissionDenied(format!(
        "SELECT privilege is required on shared table {catalog_name}.{database_name}.{}",
        table.name()
    )))
}
