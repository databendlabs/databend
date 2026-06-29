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

use std::collections::BTreeMap;
use std::sync::Arc;

use databend_common_catalog::catalog::CATALOG_DEFAULT;
use databend_common_catalog::session_type::SessionType;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::plans::SetOptionsPlan;
use databend_common_sql::plans::UnsetOptionsPlan;
use futures_util::StreamExt;
use poem::IntoResponse;
use poem::web::Json;
use poem::web::Path;

use crate::interpreters::Interpreter;
use crate::interpreters::SetOptionsInterpreter;
use crate::interpreters::UnsetOptionsInterpreter;
use crate::sessions::QueryContext;
use crate::sessions::SessionManager;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct TableOptionsResponse {
    pub success: bool,
}

async fn create_query_context(tenant: &str) -> Result<Arc<QueryContext>> {
    if tenant.is_empty() {
        return Err(ErrorCode::TenantIsEmpty(
            "Tenant can not be empty(while altering table options)",
        ));
    }

    let session = SessionManager::instance()
        .create_session(SessionType::Dummy)
        .await?;
    let session = SessionManager::instance().register_session(session)?;
    session
        .create_query_context(&databend_common_version::BUILD_INFO)
        .await
}

async fn set_options_impl(
    tenant: &str,
    database: &str,
    table: &str,
    set_options: BTreeMap<String, String>,
) -> Result<TableOptionsResponse> {
    if set_options.is_empty() {
        return Err(ErrorCode::BadArguments(
            "No options provided(while setting table options)",
        ));
    }

    let ctx = create_query_context(tenant).await?;
    let plan = SetOptionsPlan {
        set_options,
        catalog: CATALOG_DEFAULT.to_string(),
        database: database.to_string(),
        table: table.to_string(),
    };
    let interpreter = SetOptionsInterpreter::try_create(ctx.clone(), plan)?;
    let mut stream = interpreter.execute(ctx).await?;
    while let Some(block) = stream.next().await {
        block?;
    }

    Ok(TableOptionsResponse { success: true })
}

async fn unset_options_impl(
    tenant: &str,
    database: &str,
    table: &str,
    options: Vec<String>,
) -> Result<TableOptionsResponse> {
    if options.is_empty() {
        return Err(ErrorCode::BadArguments(
            "No options provided(while unsetting table options)",
        ));
    }

    let ctx = create_query_context(tenant).await?;
    let plan = UnsetOptionsPlan {
        options,
        catalog: CATALOG_DEFAULT.to_string(),
        database: database.to_string(),
        table: table.to_string(),
    };
    let interpreter = UnsetOptionsInterpreter::try_create(ctx.clone(), plan)?;
    let mut stream = interpreter.execute(ctx).await?;
    while let Some(block) = stream.next().await {
        block?;
    }

    Ok(TableOptionsResponse { success: true })
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn set_table_options(
    Path((tenant, database, table)): Path<(String, String, String)>,
    options: Json<BTreeMap<String, String>>,
) -> poem::Result<impl IntoResponse> {
    Ok(Json(
        set_options_impl(&tenant, &database, &table, options.0)
            .await
            .map_err(poem::error::InternalServerError)?,
    ))
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn unset_table_options(
    Path((tenant, database, table)): Path<(String, String, String)>,
    options: Json<Vec<String>>,
) -> poem::Result<impl IntoResponse> {
    Ok(Json(
        unset_options_impl(&tenant, &database, &table, options.0)
            .await
            .map_err(poem::error::InternalServerError)?,
    ))
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn set_table_options_local(
    Path((database, table)): Path<(String, String)>,
    options: Json<BTreeMap<String, String>>,
) -> poem::Result<impl IntoResponse> {
    let config = GlobalConfig::instance();
    let tenant = config.query.tenant_id.tenant_name();
    Ok(Json(
        set_options_impl(tenant, &database, &table, options.0)
            .await
            .map_err(poem::error::InternalServerError)?,
    ))
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn unset_table_options_local(
    Path((database, table)): Path<(String, String)>,
    options: Json<Vec<String>>,
) -> poem::Result<impl IntoResponse> {
    let config = GlobalConfig::instance();
    let tenant = config.query.tenant_id.tenant_name();
    Ok(Json(
        unset_options_impl(tenant, &database, &table, options.0)
            .await
            .map_err(poem::error::InternalServerError)?,
    ))
}
