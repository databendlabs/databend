// Copyright 2021 Datafuse Labs.
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

use common_base::base::Stoppable;
use databend_meta::api::GrpcServer;
use databend_meta::configs;
use databend_meta::meta_service::MetaNode;

/// MetaSrvTestContext is a context for meta service integration tests.
pub struct MetaSrvContext {
    pub grpc_srv: Option<Box<GrpcServer>>,
}

impl MetaSrvContext {
    pub async fn start() -> Self {
        let config = configs::Config::default();
        let mn = MetaNode::start(&config).await.unwrap();
        let _ = mn
            .join_cluster(&config.raft_config, config.grpc_api_advertise_address())
            .await
            .unwrap();

        let mut srv = GrpcServer::create(config.clone(), mn);
        srv.start().await.unwrap();

        let mc = MetaSrvContext {
            grpc_srv: Some(Box::new(srv)),
        };

        mc
    }
}
