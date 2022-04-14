//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use common_base::tokio::sync::mpsc::error::SendError;
use common_meta_types::protobuf::watch_request::FilterType;
use common_meta_types::protobuf::WatchResponse;
use tonic::Status;

use super::WatcherId;
use super::WatcherStreamSender;

pub struct WatcherStream {
    pub id: WatcherId,

    pub filter_type: FilterType,

    tx: WatcherStreamSender,

    pub key: String,

    pub key_end: String,
}

impl WatcherStream {
    pub fn new(
        id: WatcherId,
        filter_type: FilterType,
        tx: WatcherStreamSender,
        key: String,
        key_end: String,
    ) -> Self {
        WatcherStream {
            id,
            filter_type,
            tx,
            key,
            key_end,
        }
    }

    pub async fn send(
        &self,
        resp: WatchResponse,
    ) -> Result<(), SendError<Result<WatchResponse, Status>>> {
        self.tx.send(Ok(resp)).await
    }
}
