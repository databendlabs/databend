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

use common_arrow::arrow_format::flight::service::flight_service_client::FlightServiceClient;
use common_exception::Result;
use common_grpc::ConnectionFactory;

use crate::Config;
use crate::FlightClient;

#[async_trait::async_trait]
pub trait Packet: Send + Sync {
    async fn commit(&self, config: &Config, timeout: u64) -> Result<()>;
}

#[async_trait::async_trait]
impl<T: Packet> Packet for Vec<T> {
    async fn commit(&self, config: &Config, timeout: u64) -> Result<()> {
        for packet in self.iter() {
            packet.commit(config, timeout).await?;
        }

        Ok(())
    }
}

pub async fn create_client(config: &Config, address: &str) -> Result<FlightClient> {
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
