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

use databend_common_base::base::tokio;
use databend_common_config::DATABEND_COMMIT_VERSION;
use databend_sharing_endpoint::configs::Config;
use databend_sharing_endpoint::handlers::share_spec;
use databend_sharing_endpoint::handlers::share_table_meta;
use databend_sharing_endpoint::handlers::share_table_presign_files;
use databend_sharing_endpoint::middlewares::SharingAuth;
use databend_sharing_endpoint::services::SharingServices;
use poem::listener::TcpListener;
use poem::EndpointExt;
use poem::Route;
use poem::Server;

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let config = Config::load().await.expect("cfgs");
    // Print information to users.
    println!("Databend open-sharing");
    println!();
    println!("Version: {}", *DATABEND_COMMIT_VERSION);
    println!();
    println!("open open-sharing config: {:?}", config);

    SharingServices::init(config.clone())
        .await
        .expect("failed to init sharing service");
    let app = Route::new()
        // handler for share table presign
        .at(
            "/tenant/:tenant_id/:share_name/table/:table_name/presign",
            poem::post(share_table_presign_files),
        )
        // handler for accessing share table meta
        .at(
            "/tenant/:tenant_id/:share_name/meta",
            poem::post(share_table_meta),
        )
        // handler for accessing share spec
        .at("/tenant/:tenant_id/share_spec", poem::post(share_spec))
        .with(SharingAuth);

    Server::new(TcpListener::bind(config.share_endpoint_address))
        .run(app)
        .await
}
