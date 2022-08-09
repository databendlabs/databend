// Copyright 2022 Datafuse Labs.
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

use common_catalog::catalog::CATALOG_DEFAULT;
use common_exception::Result;
use common_meta_app::schema::CountTablesReq;
use poem::web::Data;
use poem::web::Json;
use poem::web::Path;
use poem::IntoResponse;
use serde::Deserialize;
use serde::Serialize;

use crate::sessions::SessionManager;

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Default)]
pub struct TenantStatus {
    tables_count: u64,
    // TODO(liyazhou): add users_count: u64,
    // TODO(liyazhou): add tables
}

struct TenantStatusLoader {
    session_mgr: Arc<SessionManager>,
    tenant: String,
}

impl TenantStatusLoader {
    async fn load_tables_count(&self) -> Result<u64> {
        let catalog = self
            .session_mgr
            .get_catalog_manager()
            .get_catalog(CATALOG_DEFAULT)?;
        catalog
            .count_tables(CountTablesReq {
                tenant: self.tenant.clone(),
            })
            .await
            .map(|r| r.count)
    }
}

// This handler returns the statistics about the metadata of a tenant, includes tables count, users
// count, etc. It's only enabled in the management mode.
#[poem::handler]
pub async fn tenant_status_handler(
    Path(tenant): Path<String>,
    session_mgr: Data<&Arc<SessionManager>>,
) -> poem::Result<impl IntoResponse> {
    let conf = session_mgr.get_conf();
    if !conf.query.management_mode {
        return Err(poem::error::NotFoundError.into());
    }

    let loader = TenantStatusLoader {
        session_mgr: session_mgr.clone(),
        tenant,
    };
    let tables_count = loader
        .load_tables_count()
        .await
        .map_err(poem::error::InternalServerError)?;
    Ok(Json(TenantStatus { tables_count }))
}
