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
use common_expression::vectorize_with_builder_1_arg;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;

pub const MICROS_IN_A_SEC: i64 = 1_000_000;
pub const MICROS_IN_A_MILLI: i64 = 1_000;

/// timestamp ranges from 1000-01-01 00:00:00.000000 to 9999-12-31 23:59:59.999999
/// timestamp_max and timestamp_min means days offset from 1970-01-01 00:00:00.000000
/// any timestamp not in the range will be invalid
pub const TIMESTAMP_MIN: i64 = -30610224000000000;
pub const TIMESTAMP_MAX: i64 = 253402300799999999;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<Int64Type, TimestampType, _, _>(
        "to_timestamp",
        FunctionProperty::default(),
        |_| None,
        vectorize_with_builder_1_arg::<Int64Type, TimestampType>(|val, output| {
            if (-31536000000..=31536000000).contains(&val) {
                output.push(Timestamp {
                    ts: val * MICROS_IN_A_SEC,
                    precision: 0,
                });
            } else if (-31536000000000..=31536000000000).contains(&val) {
                output.push(Timestamp {
                    ts: val * MICROS_IN_A_MILLI,
                    precision: 3,
                })
            } else if (TIMESTAMP_MIN..=TIMESTAMP_MAX).contains(&val) {
                output.push(Timestamp {
                    ts: val,
                    precision: 6,
                })
            } else {
                return Err(format!("timestamp `{}` is out of range", Timestamp {
                    ts: val,
                    precision: 6,
                }));
            }
            Ok(())
        }),
    );
}
