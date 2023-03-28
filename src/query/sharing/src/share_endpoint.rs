// Copyright 2023 Datafuse Labs.
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

use chrono::Utc;
use common_config::InnerConfig;
use common_exception::Result;
use common_meta_api::ShareApi;
use common_meta_app::share::DropShareEndpointReq;
use common_meta_app::share::ShareEndpointIdent;
use common_meta_app::share::UpsertShareEndpointReq;
use common_users::UserApiProvider;

pub async fn init_share_endpoint_config(config: &InnerConfig) -> Result<()> {
    let share_endpoint_address = &config.query.share_endpoint_address;
    let endpoint_name = "_query_share_endpoint_config";
    let tenant = config.query.tenant_id.clone();
    let meta_api = UserApiProvider::instance().get_meta_store_client();
    if share_endpoint_address.is_empty() {
        let req = DropShareEndpointReq {
            if_exists: true,
            endpoint: ShareEndpointIdent {
                tenant: tenant.clone(),
                endpoint: endpoint_name.to_string(),
            },
        };

        meta_api.drop_share_endpoint(req).await?;
    } else {
        let create_on = Utc::now();
        let mut args = BTreeMap::new();
        args.insert(
            "token_file".to_string(),
            config.query.share_endpoint_auth_token_file.clone(),
        );
        let upsert_req = UpsertShareEndpointReq {
            endpoint: ShareEndpointIdent {
                tenant: tenant.clone(),
                endpoint: endpoint_name.to_string(),
            },
            url: share_endpoint_address.to_string(),
            tenant: tenant.clone(),
            create_on,
            args,
        };

        meta_api.upsert_share_endpoint(upsert_req).await?;
    }
    Ok(())
}
