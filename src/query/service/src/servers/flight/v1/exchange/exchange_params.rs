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
use std::collections::HashSet;
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
    pub exchange_id: String,
    pub destination_ids: Vec<String>,
    pub destination_channels: Vec<(String, Vec<String>)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct BroadcastDestination {
    pub index: usize,
    pub num_threads: usize,
}

/// Validate the destination metadata shared by broadcast senders and
/// remote-only receivers, and return the local destination's position.
pub(super) fn validate_broadcast_destinations(
    exchange_id: &str,
    executor_id: &str,
    destination_ids: &[String],
    destination_channels: &[(String, Vec<String>)],
) -> Result<BroadcastDestination> {
    if destination_ids.is_empty() || destination_ids.len() != destination_channels.len() {
        return Err(ErrorCode::Internal(format!(
            "Broadcast exchange {} has {} destination IDs and {} channel groups",
            exchange_id,
            destination_ids.len(),
            destination_channels.len()
        )));
    }

    let mut seen_destinations = HashSet::with_capacity(destination_ids.len());
    let mut seen_channels = HashSet::new();
    let mut expected_threads = None;
    let mut local_destination = None;

    for (index, (destination_id, (channel_destination, channels))) in
        destination_ids.iter().zip(destination_channels).enumerate()
    {
        if destination_id != channel_destination {
            return Err(ErrorCode::Internal(format!(
                "Broadcast exchange {} destination {} does not match channel destination {}",
                exchange_id, destination_id, channel_destination
            )));
        }
        if !seen_destinations.insert(destination_id) {
            return Err(ErrorCode::Internal(format!(
                "Broadcast exchange {} contains duplicate destination {}",
                exchange_id, destination_id
            )));
        }
        if channels.is_empty() {
            return Err(ErrorCode::Internal(format!(
                "Broadcast exchange {} destination {} has no channels",
                exchange_id, destination_id
            )));
        }
        if expected_threads
            .replace(channels.len())
            .is_some_and(|expected| expected != channels.len())
        {
            return Err(ErrorCode::Internal(format!(
                "Broadcast exchange {} has inconsistent parallelism for destination {}",
                exchange_id, destination_id
            )));
        }
        for channel_id in channels {
            if !seen_channels.insert(channel_id) {
                return Err(ErrorCode::Internal(format!(
                    "Broadcast exchange {} contains duplicate channel {}",
                    exchange_id, channel_id
                )));
            }
        }
        if destination_id == executor_id {
            local_destination = Some(BroadcastDestination {
                index,
                num_threads: channels.len(),
            });
        }
    }

    local_destination.ok_or_else(|| {
        ErrorCode::Internal(format!(
            "Executor {} is not a destination of broadcast exchange {}",
            executor_id, exchange_id
        ))
    })
}

#[derive(Clone)]
pub struct GlobalExchangeParams {
    pub query_id: String,
    pub executor_id: String,
    pub schema: DataSchemaRef,
    pub exchange_id: String,
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
    ) -> Result<Vec<(String, FlightSender)>> {
        match self {
            ExchangeParams::MergeExchange(params) => params.take_flight_sender(senders),
            ExchangeParams::BroadcastExchange(params) => params.take_flight_sender(senders),
            ExchangeParams::NodeShuffleExchange(params) => params.take_flight_sender(senders),
            ExchangeParams::GlobalShuffleExchange(_params) => Ok(vec![]),
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
            ExchangeParams::GlobalShuffleExchange(_params) => Ok(vec![]),
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

#[cfg(test)]
mod tests {
    use databend_common_exception::Result;

    use super::BroadcastDestination;
    use super::validate_broadcast_destinations;

    fn destinations() -> (Vec<String>, Vec<(String, Vec<String>)>) {
        (vec!["node-a".to_string(), "node-b".to_string()], vec![
            ("node-a".to_string(), vec![
                "a-0".to_string(),
                "a-1".to_string(),
            ]),
            ("node-b".to_string(), vec![
                "b-0".to_string(),
                "b-1".to_string(),
            ]),
        ])
    }

    #[test]
    fn test_broadcast_destinations_validate_metadata() -> Result<()> {
        let (destination_ids, destination_channels) = destinations();
        assert_eq!(
            validate_broadcast_destinations(
                "exchange",
                "node-b",
                &destination_ids,
                &destination_channels,
            )?,
            BroadcastDestination {
                index: 1,
                num_threads: 2,
            }
        );
        Ok(())
    }

    #[test]
    fn test_broadcast_destinations_reject_invalid_metadata() {
        let (destination_ids, destination_channels) = destinations();
        let error = validate_broadcast_destinations(
            "exchange",
            "node-c",
            &destination_ids,
            &destination_channels,
        )
        .expect_err("the local executor must be a broadcast destination");
        assert!(error.to_string().contains("node-c is not a destination"));

        let error = validate_broadcast_destinations(
            "exchange",
            "node-a",
            &destination_ids[..1],
            &destination_channels,
        )
        .expect_err("destination IDs and channels must have the same length");
        assert!(
            error
                .to_string()
                .contains("1 destination IDs and 2 channel groups")
        );

        let mut mismatched_channels = destination_channels.clone();
        mismatched_channels[1].0 = "node-c".to_string();
        let error = validate_broadcast_destinations(
            "exchange",
            "node-a",
            &destination_ids,
            &mismatched_channels,
        )
        .expect_err("destination IDs must match channel destinations");
        assert!(
            error
                .to_string()
                .contains("does not match channel destination")
        );

        let duplicate_ids = vec!["node-a".to_string(), "node-a".to_string()];
        let duplicate_channels = vec![
            ("node-a".to_string(), vec!["a-0".to_string()]),
            ("node-a".to_string(), vec!["a-1".to_string()]),
        ];
        let error = validate_broadcast_destinations(
            "exchange",
            "node-a",
            &duplicate_ids,
            &duplicate_channels,
        )
        .expect_err("duplicate destinations must be rejected");
        assert!(error.to_string().contains("duplicate destination node-a"));

        let mut inconsistent_channels = destination_channels.clone();
        inconsistent_channels[1].1.pop();
        let error = validate_broadcast_destinations(
            "exchange",
            "node-a",
            &destination_ids,
            &inconsistent_channels,
        )
        .expect_err("all destinations must use the same parallelism");
        assert!(error.to_string().contains("inconsistent parallelism"));

        let mut empty_channels = destination_channels;
        empty_channels[0].1.clear();
        let error = validate_broadcast_destinations(
            "exchange",
            "node-a",
            &destination_ids,
            &empty_channels,
        )
        .expect_err("every destination must have at least one channel");
        assert!(
            error
                .to_string()
                .contains("destination node-a has no channels")
        );
    }
}
