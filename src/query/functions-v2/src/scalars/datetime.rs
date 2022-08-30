// Copyright 2021 Datafuse Labs.
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

use common_expression::types::number::Int64Type;
use common_expression::types::timestamp::Timestamp;
use common_expression::types::TimestampType;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;

pub const MICROS_IN_A_SEC: i64 = 1_000_000;
pub const MICROS_IN_A_MILLI: i64 = 1_000;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_1_arg::<Int64Type, TimestampType, _, _>(
        "to_timestamp",
        FunctionProperty::default(),
        |_| None,
        |val| {
            if -31536000000 < val && val < 31536000000 {
                Timestamp {
                    ts: val * MICROS_IN_A_SEC,
                    precision: 0,
                }
            } else if -31536000000000 < val && val < 31536000000000 {
                Timestamp {
                    ts: val * MICROS_IN_A_MILLI,
                    precision: 3,
                }
            } else {
                Timestamp {
                    ts: val,
                    precision: 6,
                }
            }
        },
    );
}
