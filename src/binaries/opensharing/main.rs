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

use common_base::base::tokio;
use poem::listener::TcpListener;
use poem::EndpointExt;
use poem::Route;
use poem::Server;
use sharing_endpoint::configs::Config;
use sharing_endpoint::handlers::presign_files;
use sharing_endpoint::middlewares::SharingAuth;
use sharing_endpoint::services::SharingServices;
#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let config = Config::load().expect("cfgs");
    println!("config: {:?}", config);
    SharingServices::init(config)
        .await
        .expect("failed to init sharing service");
    let app = Route::new()
        .at(
            "/tenant/:tenant_id/:share_name/table/:table_name/presign",
            poem::post(presign_files),
        )
        .with(SharingAuth);

    // TODO(zhihanz): remove the hard coded port into a config
    Server::new(TcpListener::bind("127.0.0.1:33003"))
        .run(app)
        .await
}
