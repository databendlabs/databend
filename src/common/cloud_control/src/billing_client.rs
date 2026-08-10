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

use crate::pb::GetBillingUsageDailyRequest;
use crate::pb::GetBillingUsageDailyResponse;
use crate::pb::billing_service_client::BillingServiceClient;
use crate::task_client::MAX_DECODING_SIZE;
use crate::task_client::MAX_ENCODING_SIZE;

pub struct BillingClient {
    pub client: BillingServiceClient<Channel>,
}

impl BillingClient {
    pub async fn new(channel: Channel) -> Result<Arc<BillingClient>> {
        let client = BillingServiceClient::new(channel)
            .max_decoding_message_size(MAX_DECODING_SIZE)
            .max_encoding_message_size(MAX_ENCODING_SIZE);
        Ok(Arc::new(BillingClient { client }))
    }

    pub async fn get_billing_usage_daily(
        &self,
        req: Request<GetBillingUsageDailyRequest>,
    ) -> Result<GetBillingUsageDailyResponse> {
        let mut client = self.client.clone();
        let resp = client.get_billing_usage_daily(req).await?;
        Ok(resp.into_inner())
    }
}
