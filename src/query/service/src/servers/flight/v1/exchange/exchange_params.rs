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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::RemoteExpr;

use crate::servers::flight::FlightReceiver;
use crate::servers::flight::FlightSender;
use crate::servers::flight::v1::exchange::ExchangeInjector;
use crate::servers::flight::v1::scatter::FlightScatter;

#[derive(Clone)]
pub struct ShuffleExchangeParams {
    pub query_id: String,
    pub executor_id: String,
    pub fragment_id: usize,
    pub schema: DataSchemaRef,
    pub destination_ids: Vec<String>,
    pub destination_channels: Vec<(String, Vec<String>)>,
    pub shuffle_scatter: Arc<Box<dyn FlightScatter>>,
    pub exchange_injector: Arc<dyn ExchangeInjector>,
    pub allow_adjust_parallelism: bool,
}

#[derive(Clone)]
pub struct MergeExchangeParams {
    pub query_id: String,
    pub fragment_id: usize,
    pub destination_id: String,
    pub channel_id: String,
    pub schema: DataSchemaRef,
    pub ignore_exchange: bool,
    pub allow_adjust_parallelism: bool,
    pub exchange_injector: Arc<dyn ExchangeInjector>,
}

#[derive(Clone)]
pub struct BroadcastExchangeParams {
    pub query_id: String,
    pub executor_id: String,
    pub schema: DataSchemaRef,
    pub destination_channels: Vec<(String, Vec<String>)>,
}

#[derive(Clone)]
pub struct GlobalExchangeParams {
    pub query_id: String,
    pub executor_id: String,
    pub schema: DataSchemaRef,
    pub shuffle_keys: Vec<RemoteExpr>,
    pub destination_channels: Vec<(String, Vec<String>)>,
}

#[allow(clippy::enum_variant_names)]
pub enum ExchangeParams {
    MergeExchange(MergeExchangeParams),
    BroadcastExchange(BroadcastExchangeParams),
    NodeShuffleExchange(ShuffleExchangeParams),
    GlobalShuffleExchange(GlobalExchangeParams),
}

impl ExchangeParams {
    pub fn get_schema(&self) -> DataSchemaRef {
        match self {
            ExchangeParams::NodeShuffleExchange(exchange) => exchange.schema.clone(),
            ExchangeParams::MergeExchange(exchange) => exchange.schema.clone(),
            ExchangeParams::BroadcastExchange(exchange) => exchange.schema.clone(),
            ExchangeParams::GlobalShuffleExchange(exchange) => exchange.schema.clone(),
        }
    }

    pub fn get_query_id(&self) -> String {
        match self {
            ExchangeParams::NodeShuffleExchange(exchange) => exchange.query_id.clone(),
            ExchangeParams::MergeExchange(exchange) => exchange.query_id.clone(),
            ExchangeParams::BroadcastExchange(exchange) => exchange.query_id.clone(),
            ExchangeParams::GlobalShuffleExchange(exchange) => exchange.query_id.clone(),
        }
    }

    pub fn take_flight_sender(
        &self,
        senders: &mut HashMap<String, Vec<FlightSender>>,
    ) -> databend_common_exception::Result<Vec<(String, FlightSender)>> {
        match self {
            ExchangeParams::MergeExchange(params) => params.take_flight_sender(senders),
            ExchangeParams::BroadcastExchange(params) => params.take_flight_sender(senders),
            ExchangeParams::NodeShuffleExchange(params) => params.take_flight_sender(senders),
            ExchangeParams::GlobalShuffleExchange(_params) => unimplemented!(),
        }
    }

    pub fn take_flight_receiver(
        &self,
        receivers: &mut HashMap<String, Vec<FlightReceiver>>,
    ) -> Result<Vec<FlightReceiver>> {
        match self {
            ExchangeParams::MergeExchange(params) => params.take_flight_receiver(receivers),
            ExchangeParams::BroadcastExchange(params) => params.take_flight_receiver(receivers),
            ExchangeParams::NodeShuffleExchange(params) => params.take_flight_receiver(receivers),
            ExchangeParams::GlobalShuffleExchange(_params) => unimplemented!(),
        }
    }
}

impl MergeExchangeParams {
    fn take_flight_sender(
        &self,
        senders: &mut HashMap<String, Vec<FlightSender>>,
    ) -> Result<Vec<(String, FlightSender)>> {
        let Some(sender) = senders.remove(&self.channel_id) else {
            return Err(ErrorCode::UnknownFragmentExchange(format!(
                "Unknown fragment exchange channel, {}, {}",
                self.destination_id, self.fragment_id
            )));
        };

        Ok(sender
            .into_iter()
            .map(|x| (self.destination_id.clone(), x))
            .collect())
    }

    fn take_flight_receiver(
        &self,
        receivers: &mut HashMap<String, Vec<FlightReceiver>>,
    ) -> Result<Vec<FlightReceiver>> {
        let Some(receivers) = receivers.remove(&self.channel_id) else {
            return Err(ErrorCode::UnknownFragmentExchange(format!(
                "Unknown fragment flight receiver, {}, {}",
                self.destination_id, self.fragment_id
            )));
        };

        Ok(receivers)
    }
}

impl BroadcastExchangeParams {
    fn take_flight_sender(
        &self,
        senders: &mut HashMap<String, Vec<FlightSender>>,
    ) -> Result<Vec<(String, FlightSender)>> {
        let mut exchanges = Vec::with_capacity(self.destination_channels.len());

        for (destination, channels) in &self.destination_channels {
            for channel in channels {
                if destination == &self.executor_id {
                    exchanges.push((
                        destination.clone(),
                        FlightSender::create(async_channel::bounded(1).0),
                    ));

                    continue;
                }

                let Some(senders) = senders.remove(channel) else {
                    return Err(ErrorCode::UnknownFragmentExchange(format!(
                        "Unknown fragment broadcast exchange channel, {}",
                        destination
                    )));
                };

                exchanges.extend(senders.into_iter().map(|x| (destination.clone(), x)));
            }
        }

        Ok(exchanges)
    }

    fn take_flight_receiver(
        &self,
        receivers: &mut HashMap<String, Vec<FlightReceiver>>,
    ) -> Result<Vec<FlightReceiver>> {
        let mut exchanges = Vec::with_capacity(self.destination_channels.len());

        for (destination, channels) in &self.destination_channels {
            if destination == &self.executor_id {
                for channel in channels {
                    let Some(receivers) = receivers.remove(channel) else {
                        return Err(ErrorCode::UnknownFragmentExchange(format!(
                            "Unknown fragment broadcast flight receiver, {}",
                            self.executor_id
                        )));
                    };
                    exchanges.extend(receivers);
                }
            }
        }

        Ok(exchanges)
    }
}

impl ShuffleExchangeParams {
    fn take_flight_sender(
        &self,
        senders: &mut HashMap<String, Vec<FlightSender>>,
    ) -> Result<Vec<(String, FlightSender)>> {
        let mut exchanges = Vec::with_capacity(self.destination_ids.len());

        for (destination, channels) in &self.destination_channels {
            for channel in channels {
                if destination == &self.executor_id {
                    exchanges.push((
                        destination.clone(),
                        FlightSender::create(async_channel::bounded(1).0),
                    ));

                    continue;
                }

                let Some(senders) = senders.remove(channel) else {
                    return Err(ErrorCode::UnknownFragmentExchange(format!(
                        "Unknown fragment exchange channel, {}, {}",
                        destination, self.fragment_id
                    )));
                };

                exchanges.extend(senders.into_iter().map(|x| (destination.clone(), x)));
            }
        }

        Ok(exchanges)
    }

    fn take_flight_receiver(
        &self,
        receivers: &mut HashMap<String, Vec<FlightReceiver>>,
    ) -> Result<Vec<FlightReceiver>> {
        let mut exchanges = Vec::with_capacity(self.destination_channels.len());

        for (destination, channels) in &self.destination_channels {
            if destination == &self.executor_id {
                for channel in channels {
                    let Some(receivers) = receivers.remove(channel) else {
                        return Err(ErrorCode::UnknownFragmentExchange(format!(
                            "Unknown fragment flight receiver, {}, {}",
                            self.executor_id, self.fragment_id
                        )));
                    };
                    exchanges.extend(receivers.into_iter());
                }
            }
        }

        Ok(exchanges)
    }
}
