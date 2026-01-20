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

use databend_common_exception::Result;
use tonic::Request;
use tonic::transport::Channel;

use crate::pb::ApplyUdfResourceRequest;
use crate::pb::ApplyUdfResourceResponse;
use crate::pb::udf_service_client::UdfServiceClient;

pub(crate) const UDF_CLIENT_VERSION: &str = "v1";
pub(crate) const UDF_CLIENT_VERSION_NAME: &str = "UDF_CLIENT_VERSION";

pub struct UdfClient {
    pub udf_client: UdfServiceClient<Channel>,
}

impl UdfClient {
    pub async fn new(channel: Channel) -> Result<Arc<UdfClient>> {
        let udf_client = UdfServiceClient::new(channel);
        Ok(Arc::new(UdfClient { udf_client }))
    }

    pub async fn apply_udf_resource(
        &self,
        req: Request<ApplyUdfResourceRequest>,
    ) -> Result<ApplyUdfResourceResponse> {
        let mut client = self.udf_client.clone();
        let resp = client.apply_udf_resource(req).await?;
        Ok(resp.into_inner())
    }
}
