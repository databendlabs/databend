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

//! Overloads such as `to_yyyymmdd(ts, 'Asia/Shanghai')`, which read the time zone from
//! their last argument instead of the session. Their results do not depend on the
//! session, so they can be used as partition keys.

use chrono_tz::Tz;
use databend_common_expression::EvalContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Value;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::number::SimpleDomain;
use databend_common_expression::types::string::StringDomain;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;

/// Functions registered with a `(timestamp, time_zone)` overload by
/// [`register_timestamp_with_timezone`], by canonical name.
pub const TIMEZONE_ARGUMENT_FUNCTIONS: &[&str] = &[
    "to_date",
    // date_extract
    "to_yyyymm",
    "to_yyyymmdd",
    "to_yyyymmddhh",
    "to_yyyymmddhhmmss",
    "to_year",
    "to_iso_year",
    "to_quarter",
    "to_month",
    "to_day_of_year",
    "to_day_of_month",
    "to_day_of_week",
    "dayofweek",
    "yearweek",
    "millennium",
    "to_week_of_year",
    "to_hour",
    "to_minute",
    "to_second",
    // date_round
    "to_start_of_second",
    "to_start_of_minute",
    "to_start_of_five_minutes",
    "to_start_of_ten_minutes",
    "to_start_of_fifteen_minutes",
    "to_start_of_hour",
    "to_start_of_day",
    "time_slot",
    "to_monday",
    "to_start_of_month",
    "to_start_of_quarter",
    "to_start_of_year",
    "to_start_of_iso_year",
    "to_last_of_week",
    "to_last_of_month",
    "to_last_of_quarter",
    "to_last_of_year",
    "to_previous_monday",
    "to_previous_tuesday",
    "to_previous_wednesday",
    "to_previous_thursday",
    "to_previous_friday",
    "to_previous_saturday",
    "to_previous_sunday",
    "to_next_monday",
    "to_next_tuesday",
    "to_next_wednesday",
    "to_next_thursday",
    "to_next_friday",
    "to_next_saturday",
    "to_next_sunday",
];

/// Register `name(Timestamp, String)`, which evaluates `eval` in the time zone named by
/// the second argument.
///
/// `calc_domain` is used only when that argument is a valid constant. Callers register it
/// outside `register_context_dependent`, since it does not read the session, and list
/// `name` in [`TIMEZONE_ARGUMENT_FUNCTIONS`].
pub(crate) fn register_timestamp_with_timezone<O, D, E>(
    registry: &mut FunctionRegistry,
    name: &str,
    calc_domain: D,
    eval: E,
) where
    O: ArgType,
    D: Fn(&Tz, &SimpleDomain<i64>) -> FunctionDomain<O> + Copy + Send + Sync + 'static,
    E: Fn(i64, &Tz) -> Result<O::Scalar, String> + Copy + Send + Sync + 'static,
{
    registry.register_passthrough_nullable_2_arg::<TimestampType, StringType, O, _, _>(
        name,
        move |_, domain, tz| match constant_timezone(tz) {
            Some(tz) => calc_domain(&tz, domain),
            None => FunctionDomain::MayThrow,
        },
        move |ts, tz, ctx| eval_with_timezone::<O, E>(ts, tz, ctx, eval),
    );
}

/// The time zone named by a constant argument.
pub(crate) fn constant_timezone(domain: &StringDomain) -> Option<Tz> {
    (domain.max.as_ref() == Some(&domain.min))
        .then(|| domain.min.parse().ok())
        .flatten()
}

fn eval_with_timezone<O, E>(
    ts: Value<TimestampType>,
    tz: Value<StringType>,
    ctx: &mut EvalContext,
    eval: E,
) -> Value<O>
where
    O: ArgType,
    E: Fn(i64, &Tz) -> Result<O::Scalar, String> + Copy + Send + Sync,
{
    // A constant time zone is parsed once.
    if let Value::Scalar(name) = &tz
        && let Ok(tz) = name.parse::<Tz>()
    {
        return vectorize_with_builder_1_arg::<TimestampType, O>(move |ts, output, ctx| {
            push_result::<O>(eval(ts, &tz), output, ctx)
        })(ts, ctx);
    }

    vectorize_with_builder_2_arg::<TimestampType, StringType, O>(move |ts, tz, output, ctx| {
        if let Some(validity) = &ctx.validity
            && !validity.get_bit(O::builder_len(output))
        {
            O::push_default(output);
            return;
        }
        let result = tz
            .parse::<Tz>()
            .map_err(|err| format!("cannot parse time zone `{tz}`: {err}"))
            .and_then(|tz| eval(ts, &tz));
        push_result::<O>(result, output, ctx)
    })(ts, tz, ctx)
}

fn push_result<O: ArgType>(
    result: Result<O::Scalar, String>,
    output: &mut O::ColumnBuilder,
    ctx: &mut EvalContext,
) {
    match result {
        Ok(value) => O::push_item(output, O::to_scalar_ref(&value)),
        Err(err) => {
            ctx.set_error(O::builder_len(output), err);
            O::push_default(output);
        }
    }
}
