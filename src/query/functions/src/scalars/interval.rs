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

use databend_common_column::types::months_days_ns;
use databend_common_expression::error_to_null;
use databend_common_expression::types::interval::interval_to_string;
use databend_common_expression::types::interval::string_to_interval;
use databend_common_expression::types::IntervalType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::StringType;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::EvalContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Value;
use databend_common_io::cursor_ext::Interval;

pub fn register(registry: &mut FunctionRegistry) {
    // cast(xx AS interval)
    // to_interval(xx)
    register_string_to_interval(registry);
    register_interval_to_string(registry);
}

fn register_string_to_interval(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<StringType, IntervalType, _, _>(
        "to_interval",
        |_, _| FunctionDomain::MayThrow,
        eval_string_to_interval,
    );
    registry.register_combine_nullable_1_arg::<StringType, IntervalType, _, _>(
        "try_to_interval",
        |_, _| FunctionDomain::Full,
        error_to_null(eval_string_to_interval),
    );

    fn eval_string_to_interval(
        val: Value<StringType>,
        ctx: &mut EvalContext,
    ) -> Value<IntervalType> {
        vectorize_with_builder_1_arg::<StringType, IntervalType>(|val, output, ctx| {
            match string_to_interval(val) {
                Ok(interval) => output.push(months_days_ns(
                    interval.months,
                    interval.days,
                    interval.micros,
                )),
                Err(e) => {
                    ctx.set_error(
                        output.len(),
                        format!("cannot parse to type `INTERVAL`. {}", e),
                    );
                    output.push(months_days_ns(0, 0, 0));
                }
            }
        })(val, ctx)
    }
}

fn register_interval_to_string(registry: &mut FunctionRegistry) {
    registry.register_combine_nullable_1_arg::<IntervalType, StringType, _, _>(
        "to_string",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<IntervalType, NullableType<StringType>>(
            |interval, output, _| {
                let i = Interval {
                    months: interval.0,
                    days: interval.1,
                    micros: interval.2,
                };
                let res = interval_to_string(i).to_string();
                println!("interval to string res is {}", res);
                output.push(&res);
            },
        ),
    );
}
