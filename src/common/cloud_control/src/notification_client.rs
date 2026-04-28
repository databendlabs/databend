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

use tonic::transport::Channel;
use tonic::Request;

use crate::pb::notification_service_client::NotificationServiceClient;
use crate::pb::CreateNotificationRequest;
use crate::pb::CreateNotificationResponse;
use crate::pb::DropNotificationRequest;
use crate::pb::DropNotificationResponse;
use crate::pb::GetNotificationRequest;
use crate::pb::GetNotificationResponse;
use crate::pb::ListNotificationRequest;
use crate::pb::ListNotificationResponse;

pub(crate) const NOTIFICATION_CLIENT_VERSION: &str = "v1";
pub(crate) const NOTIFICATION_CLIENT_VERSION_NAME: &str = "NOTIFICATION_CLIENT_VERSION";
pub struct NotificationClient {
    pub client: NotificationServiceClient<Channel>,
}

impl NotificationClient {
    // TODO: add auth interceptor
    pub async fn new(
        channel: Channel,
    ) -> databend_common_exception::Result<Arc<NotificationClient>> {
        let client = NotificationServiceClient::new(channel);
        Ok(Arc::new(NotificationClient { client }))
    }

    // TODO: richer error handling on Task Error
    pub async fn create_notification(
        &self,
        req: Request<CreateNotificationRequest>,
    ) -> databend_common_exception::Result<CreateNotificationResponse> {
        let mut client = self.client.clone();
        let resp = client.create_notification(req).await?;
        Ok(resp.into_inner())
    }

    pub async fn drop_notification(
        &self,
        req: Request<DropNotificationRequest>,
    ) -> databend_common_exception::Result<DropNotificationResponse> {
        let mut client = self.client.clone();
        let resp = client.drop_notification(req).await?;
        Ok(resp.into_inner())
    }

    pub async fn desc_notification(
        &self,
        req: Request<GetNotificationRequest>,
    ) -> databend_common_exception::Result<GetNotificationResponse> {
        let mut client = self.client.clone();
        let resp = client.get_notification(req).await?;
        Ok(resp.into_inner())
    }

    pub async fn alter_notification(
        &self,
        req: Request<crate::pb::AlterNotificationRequest>,
    ) -> databend_common_exception::Result<crate::pb::AlterNotificationResponse> {
        let mut client = self.client.clone();
        let resp = client.alter_notification(req).await?;
        Ok(resp.into_inner())
    }

    pub async fn list_notifications(
        &self,
        req: Request<ListNotificationRequest>,
    ) -> databend_common_exception::Result<ListNotificationResponse> {
        let mut client = self.client.clone();
        let resp = client.list_notification(req).await?;
        Ok(resp.into_inner())
    }

    pub async fn list_notification_histories(
        &self,
        req: Request<crate::pb::ListNotificationHistoryRequest>,
    ) -> databend_common_exception::Result<crate::pb::ListNotificationHistoryResponse> {
        let mut client = self.client.clone();
        let resp = client.list_notification_history(req).await?;
        Ok(resp.into_inner())
    }
}
