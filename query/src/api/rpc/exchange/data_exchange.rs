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

use common_planners::Expression;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum DataExchange {
    // None,
    Merge(MergeExchange),
    ShuffleDataExchange(ShuffleDataExchange),
}

impl DataExchange {
    pub fn get_destinations(&self) -> Vec<String> {
        match self {
            // DataExchange::None => vec![],
            DataExchange::Merge(exchange) => vec![exchange.destination_id.clone()],
            DataExchange::ShuffleDataExchange(exchange) => exchange.destination_ids.clone(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ShuffleDataExchange {
    pub destination_ids: Vec<String>,
    pub exchange_expression: Expression,
}

impl ShuffleDataExchange {
    pub fn create(destination_ids: Vec<String>, exchange_expression: Expression) -> DataExchange {
        DataExchange::ShuffleDataExchange(ShuffleDataExchange {
            destination_ids,
            exchange_expression,
        })
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MergeExchange {
    pub destination_id: String,
}

impl MergeExchange {
    pub fn create(destination_id: String) -> DataExchange {
        DataExchange::Merge(MergeExchange { destination_id })
    }
}
