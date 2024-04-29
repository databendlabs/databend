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

use databend_common_arrow::arrow_format::flight::service::flight_service_client::FlightServiceClient;
use databend_common_config::InnerConfig;
use databend_common_exception::Result;
use databend_common_grpc::ConnectionFactory;

use crate::servers::flight::FlightClient;

#[async_trait::async_trait]
pub trait Packet: Send + Sync {
    async fn commit(&self, config: &InnerConfig, timeout: u64) -> Result<()>;
}

#[async_trait::async_trait]
impl<T: Packet> Packet for Vec<T> {
    #[async_backtrace::framed]
    async fn commit(&self, config: &InnerConfig, timeout: u64) -> Result<()> {
        for packet in self.iter() {
            packet.commit(config, timeout).await?;
        }

        Ok(())
    }
}

#[async_backtrace::framed]
pub async fn create_client(config: &InnerConfig, address: &str) -> Result<FlightClient> {
    match config.tls_query_cli_enabled() {
        true => Ok(FlightClient::new(FlightServiceClient::new(
            ConnectionFactory::create_rpc_channel(
                address.to_owned(),
                None,
                Some(config.query.to_rpc_client_tls_config()),
            )
            .await?,
        ))),
        false => Ok(FlightClient::new(FlightServiceClient::new(
            ConnectionFactory::create_rpc_channel(address.to_owned(), None, None).await?,
        ))),
    }
}
