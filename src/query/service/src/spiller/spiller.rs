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

use opendal::Operator;

/// Spiller type, currently only supports HashJoin
enum SpillerType {
    HashJoin, /* Todo: Add more spiller type
               * OrderBy
               * Aggregation */
}

/// Spiller configuration
pub struct SpillerConfig {
    location_prefix: String,
}

impl SpillerConfig {
    pub fn create(location_prefix: String) -> Self {
        Self { location_prefix }
    }
}

/// Spiller is a unified framework for operators which need to spill data from memory.
/// It provides the following features:
/// 1. Collection data that needs to be spilled.
/// 2. Partition data by the specified algorithm which specifies by operator
/// 3. Serialization and deserialization input data
/// 4. Interact with the underlying storage engine to write and read spilled data
pub struct Spiller {
    operator: Operator,
    config: SpillerConfig,
    spiller_type: SpillerType,
}

impl Spiller {
    /// Create a new spiller
    pub fn create(operator: Operator, config: SpillerConfig) -> Self {
        let spiller_type = SpillerType::HashJoin;
        Self {
            operator,
            config,
            spiller_type,
        }
    }
}
