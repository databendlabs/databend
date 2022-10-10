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

use common_expression::types::date::check_date;
use common_expression::types::date::DateType;
use common_expression::types::number::Int64Type;
use common_expression::types::timestamp::check_timestamp;
use common_expression::types::timestamp::Timestamp;
use common_expression::types::timestamp::MAX_TIMESTAMP;
use common_expression::types::timestamp::MIN_TIMESTAMP;
use common_expression::types::TimestampType;
use common_expression::vectorize_with_builder_1_arg;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;

pub fn register(registry: &mut FunctionRegistry) {
    // TODO: convert to CAST
    registry.register_aliases("to_timestamp", &["to_datetime"]);
    // TODO: convert to CAST
    registry.register_passthrough_nullable_1_arg::<Int64Type, TimestampType, _, _>(
        "to_timestamp",
        FunctionProperty::default(),
        |_| None,
        vectorize_with_builder_1_arg::<Int64Type, TimestampType>(|val, output, _| {
            let (precision, base) = check_timestamp(val)?;
            output.push(Timestamp {
                ts: val * base,
                precision,
            });
            Ok(())
        }),
    );
    // TODO: convert to CAST
    registry.register_passthrough_nullable_1_arg::<Int64Type, DateType, _, _>(
        "to_date",
        FunctionProperty::default(),
        |_| None,
        vectorize_with_builder_1_arg::<Int64Type, DateType>(|val, output, _| {
            check_date(val)?;
            // check_date will check the range of the date, so we can safely unwrap here.
            output.push(val.try_into().unwrap());
            Ok(())
        }),
    );
}
