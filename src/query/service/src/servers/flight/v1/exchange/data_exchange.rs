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

use databend_common_base::base::GlobalUniqName;
use databend_common_expression::RemoteExpr;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum DataExchange {
    Merge(MergeExchange),
    Broadcast(BroadcastExchange),
    NodeToNodeExchange(NodeToNodeExchange),
}

impl DataExchange {
    pub fn get_destinations(&self) -> Vec<String> {
        match self {
            DataExchange::Merge(exchange) => vec![exchange.destination_id.clone()],
            DataExchange::Broadcast(exchange) => exchange.destination_ids.clone(),
            DataExchange::NodeToNodeExchange(exchange) => exchange.destination_ids.clone(),
        }
    }

    pub fn get_channels(&self, destination: &str) -> Vec<String> {
        match self {
            DataExchange::Merge(exchange) => vec![exchange.channel_id.clone()],
            DataExchange::Broadcast(exchange) => {
                for (to, channels) in &exchange.destination_channels {
                    if to == destination {
                        return channels.clone();
                    }
                }

                vec![]
            }
            DataExchange::NodeToNodeExchange(exchange) => {
                for (to, channels) in &exchange.destination_channels {
                    if to == destination {
                        return channels.clone();
                    }
                }

                vec![]
            }
        }
    }

    pub fn get_parallel(&self) -> usize {
        1
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NodeToNodeExchange {
    pub destination_ids: Vec<String>,
    pub shuffle_keys: Vec<RemoteExpr>,
    pub destination_channels: Vec<(String, Vec<String>)>,
    pub allow_adjust_parallelism: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MergeExchange {
    pub destination_id: String,
    pub ignore_exchange: bool,
    pub channel_id: String,
    pub allow_adjust_parallelism: bool,
}

impl MergeExchange {
    pub fn create(
        destination_id: String,
        ignore_exchange: bool,
        allow_adjust_parallelism: bool,
    ) -> DataExchange {
        DataExchange::Merge(MergeExchange {
            destination_id,
            ignore_exchange,
            allow_adjust_parallelism,
            channel_id: GlobalUniqName::unique(),
        })
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BroadcastExchange {
    pub destination_ids: Vec<String>,
    pub destination_channels: Vec<(String, Vec<String>)>,
}

impl BroadcastExchange {
    pub fn create(destination_ids: Vec<String>) -> DataExchange {
        let mut destination_channels = Vec::with_capacity(destination_ids.len());

        for destination in &destination_ids {
            destination_channels.push((destination.clone(), vec![GlobalUniqName::unique()]));
        }

        DataExchange::Broadcast(BroadcastExchange {
            destination_ids,
            destination_channels,
        })
    }
}
