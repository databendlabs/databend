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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_users::UserApiProvider;

use crate::sessions::QueryContext;

#[async_backtrace::framed]
pub async fn validate_grant_object_exists(
    ctx: &Arc<QueryContext>,
    object: &GrantObject,
) -> Result<()> {
    let tenant = ctx.get_tenant();

    match &object {
        GrantObject::Table(catalog_name, database_name, table_name) => {
            let catalog = ctx.get_catalog(catalog_name).await?;
            if catalog.exists_table_function(table_name) {
                return Ok(());
            }

            if !catalog
                .exists_table(tenant.as_str(), database_name, table_name)
                .await?
            {
                return Err(databend_common_exception::ErrorCode::UnknownTable(format!(
                    "table `{}`.`{}` not exists in catalog '{}'",
                    database_name, table_name, catalog_name,
                )));
            }
        }
        GrantObject::Database(catalog_name, database_name) => {
            let catalog = ctx.get_catalog(catalog_name).await?;
            if !catalog
                .exists_database(tenant.as_str(), database_name)
                .await?
            {
                return Err(databend_common_exception::ErrorCode::UnknownDatabase(
                    format!("database {} not exists", database_name,),
                ));
            }
        }
        GrantObject::DatabaseById(catalog_name, db_id) => {
            let catalog = ctx.get_catalog(catalog_name).await?;
            if catalog.get_db_name_by_id(*db_id).await.is_err() {
                return Err(databend_common_exception::ErrorCode::UnknownDatabaseId(
                    format!(
                        "database id {} not exists in catalog {}",
                        db_id, catalog_name
                    ),
                ));
            }
        }
        GrantObject::TableById(catalog_name, db_id, table_id) => {
            let catalog = ctx.get_catalog(catalog_name).await?;

            if catalog.get_table_name_by_id(*table_id).await.is_err() {
                return Err(databend_common_exception::ErrorCode::UnknownTableId(
                    format!(
                        "table id `{}`.`{}` not exists in catalog '{}'",
                        db_id, table_id, catalog_name,
                    ),
                ));
            }
        }
        GrantObject::UDF(udf) => {
            if !UserApiProvider::instance()
                .exists_udf(tenant.as_str(), udf)
                .await?
            {
                return Err(databend_common_exception::ErrorCode::UnknownStage(format!(
                    "udf {udf} not exists"
                )));
            }
        }
        GrantObject::Stage(stage) => {
            if !UserApiProvider::instance()
                .exists_stage(ctx.get_tenant().as_str(), stage)
                .await?
            {
                return Err(databend_common_exception::ErrorCode::UnknownStage(format!(
                    "stage {stage} not exists"
                )));
            }
        }
        GrantObject::Global => (),
    }

    Ok(())
}

#[async_backtrace::framed]
pub async fn convert_to_ownerobject(
    ctx: &Arc<QueryContext>,
    tenant: &str,
    object: &GrantObject,
    catalog_name: Option<String>,
) -> Result<OwnershipObject> {
    match object {
        GrantObject::Database(_, db_name) => {
            let catalog_name = catalog_name.unwrap();
            let catalog = ctx.get_catalog(&catalog_name).await?;
            let db_id = catalog
                .get_database(tenant, db_name)
                .await?
                .get_db_info()
                .ident
                .db_id;
            Ok(OwnershipObject::Database {
                catalog_name,
                db_id,
            })
        }
        GrantObject::Table(_, db_name, table_name) => {
            let catalog_name = catalog_name.unwrap();
            let catalog = ctx.get_catalog(&catalog_name).await?;
            let db_id = catalog
                .get_database(tenant, db_name)
                .await?
                .get_db_info()
                .ident
                .db_id;
            let table_id = catalog
                .get_table(tenant, db_name.as_str(), table_name)
                .await?
                .get_id();
            Ok(OwnershipObject::Table {
                catalog_name,
                db_id,
                table_id,
            })
        }
        GrantObject::TableById(_, db_id, table_id) => Ok(OwnershipObject::Table {
            catalog_name: catalog_name.unwrap(),
            db_id: *db_id,
            table_id: *table_id,
        }),
        GrantObject::DatabaseById(_, db_id) => Ok(OwnershipObject::Database {
            catalog_name: catalog_name.unwrap(),
            db_id: *db_id,
        }),
        GrantObject::Stage(name) => Ok(OwnershipObject::Stage {
            name: name.to_string(),
        }),
        GrantObject::UDF(name) => Ok(OwnershipObject::UDF {
            name: name.to_string(),
        }),
        GrantObject::Global => Err(ErrorCode::IllegalGrant(
            "Illegal GRANT/REVOKE command; please consult the manual to see which privileges can be used",
        )),
    }
}
