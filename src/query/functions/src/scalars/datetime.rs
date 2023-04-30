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

use std::io::Write;

use chrono::Datelike;
use chrono::Utc;
use common_arrow::arrow::temporal_conversions::EPOCH_DAYS_FROM_CE;
use common_expression::error_to_null;
use common_expression::types::date::check_date;
use common_expression::types::date::date_to_string;
use common_expression::types::date::string_to_date;
use common_expression::types::date::DATE_MAX;
use common_expression::types::date::DATE_MIN;
use common_expression::types::nullable::NullableColumn;
use common_expression::types::nullable::NullableDomain;
use common_expression::types::number::Int64Type;
use common_expression::types::number::SimpleDomain;
use common_expression::types::number::UInt16Type;
use common_expression::types::number::UInt32Type;
use common_expression::types::number::UInt64Type;
use common_expression::types::number::UInt8Type;
use common_expression::types::string::StringDomain;
use common_expression::types::timestamp::check_timestamp;
use common_expression::types::timestamp::microseconds_to_days;
use common_expression::types::timestamp::string_to_timestamp;
use common_expression::types::timestamp::timestamp_to_string;
use common_expression::types::timestamp::MICROS_IN_A_MILLI;
use common_expression::types::timestamp::MICROS_IN_A_SEC;
use common_expression::types::DateType;
use common_expression::types::Int32Type;
use common_expression::types::NullableType;
use common_expression::types::NumberType;
use common_expression::types::StringType;
use common_expression::types::TimestampType;
use common_expression::utils::arrow::constant_bitmap;
use common_expression::utils::date_helper::*;
use common_expression::vectorize_1_arg;
use common_expression::vectorize_2_arg;
use common_expression::vectorize_with_builder_1_arg;
use common_expression::vectorize_with_builder_2_arg;
use common_expression::EvalContext;
use common_expression::FunctionDomain;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::Value;
use common_expression::ValueRef;
use num_traits::AsPrimitive;

pub fn register(registry: &mut FunctionRegistry) {
    // cast(xx AS timestamp)
    // to_timestamp(xx)
    register_string_to_timestamp(registry);
    register_date_to_timestamp(registry);
    register_number_to_timestamp(registry);

    // cast(xx AS date)
    // to_date(xx)
    register_string_to_date(registry);
    register_timestamp_to_date(registry);
    register_number_to_date(registry);

    // cast([date | timestamp] AS string)
    // to_string([date | timestamp])
    register_to_string(registry);

    // cast([date | timestamp] AS [uint8 | int8 | ...])
    // to_[uint8 | int8 | ...]([date | timestamp])
    register_to_number(registry);

    // [add | subtract]_[years | months | days | hours | minutes | seconds]([date | timestamp], number)
    // date_[add | sub]([year | quarter | month | week | day | hour | minute | second], [date | timestamp], number)
    // [date | timestamp] [+ | -] interval number [year | quarter | month | week | day | hour | minute | second]
    register_add_functions(registry);
    register_sub_functions(registry);

    // now, today, yesterday, tomorrow
    register_real_time_functions(registry);

    // to_*([date | timestamp]) -> number
    register_to_number_functions(registry);

    // to_*([date | timestamp]) -> [date | timestamp]
    register_rounder_functions(registry);

    // [date | timestamp] +/- number
    register_timestamp_add_sub(registry);
}

/// Check if timestamp is within range, and return the timestamp in micros.
#[inline]
fn int64_to_timestamp(n: i64) -> Result<i64, String> {
    if -31536000000 < n && n < 31536000000 {
        Ok(n * MICROS_IN_A_SEC)
    } else if -31536000000000 < n && n < 31536000000000 {
        Ok(n * MICROS_IN_A_MILLI)
    } else {
        check_timestamp(n)
    }
}

fn int64_domain_to_timestamp_domain<T: AsPrimitive<i64>>(
    domain: &SimpleDomain<T>,
) -> Option<SimpleDomain<i64>> {
    Some(SimpleDomain {
        min: int64_to_timestamp(domain.min.as_()).ok()?,
        max: int64_to_timestamp(domain.max.as_()).ok()?,
    })
}

fn register_string_to_timestamp(registry: &mut FunctionRegistry) {
    registry.register_aliases("to_timestamp", &["to_datetime"]);
    registry.register_aliases("try_to_timestamp", &["try_to_datetime"]);

    registry.register_passthrough_nullable_1_arg::<StringType, TimestampType, _, _>(
        "to_timestamp",
        |_| FunctionDomain::MayThrow,
        eval_string_to_timestamp,
    );
    registry.register_combine_nullable_1_arg::<StringType, TimestampType, _, _>(
        "try_to_timestamp",
        |_| FunctionDomain::Full,
        error_to_null(eval_string_to_timestamp),
    );

    fn eval_string_to_timestamp(
        val: ValueRef<StringType>,
        ctx: &mut EvalContext,
    ) -> Value<TimestampType> {
        vectorize_with_builder_1_arg::<StringType, TimestampType>(|val, output, ctx| {
            match string_to_timestamp(val, ctx.func_ctx.tz.tz) {
                Some(ts) => output.push(ts.timestamp_micros()),
                None => {
                    ctx.set_error(output.len(), "cannot parse to type `TIMESTAMP`");
                    output.push(0);
                }
            }
        })(val, ctx)
    }
}

fn register_date_to_timestamp(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<DateType, TimestampType, _, _>(
        "to_timestamp",
        |domain| {
            FunctionDomain::Domain(SimpleDomain {
                min: domain.min as i64 * 24 * 3600 * 1000000,
                max: domain.max as i64 * 24 * 3600 * 1000000,
            })
        },
        eval_date_to_timestamp,
    );
    registry.register_combine_nullable_1_arg::<DateType, TimestampType, _, _>(
        "try_to_timestamp",
        |domain| {
            FunctionDomain::Domain(NullableDomain {
                has_null: false,
                value: Some(Box::new(SimpleDomain {
                    min: domain.min as i64 * 24 * 3600 * 1000000,
                    max: domain.max as i64 * 24 * 3600 * 1000000,
                })),
            })
        },
        error_to_null(eval_date_to_timestamp),
    );

    fn eval_date_to_timestamp(
        val: ValueRef<DateType>,
        ctx: &mut EvalContext,
    ) -> Value<TimestampType> {
        vectorize_with_builder_1_arg::<DateType, TimestampType>(|val, output, _| {
            let ts = (val as i64) * 24 * 3600 * MICROS_IN_A_SEC;
            output.push(ts);
        })(val, ctx)
    }
}

fn register_number_to_timestamp(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<Int64Type, TimestampType, _, _>(
        "to_timestamp",
        |domain| {
            int64_domain_to_timestamp_domain(domain)
                .map(FunctionDomain::Domain)
                .unwrap_or(FunctionDomain::MayThrow)
        },
        eval_number_to_timestamp,
    );
    registry.register_combine_nullable_1_arg::<Int64Type, TimestampType, _, _>(
        "try_to_timestamp",
        |domain| {
            if let Some(domain) = int64_domain_to_timestamp_domain(domain) {
                FunctionDomain::Domain(NullableDomain {
                    has_null: false,
                    value: Some(Box::new(domain)),
                })
            } else {
                FunctionDomain::Full
            }
        },
        error_to_null(eval_number_to_timestamp),
    );

    fn eval_number_to_timestamp(
        val: ValueRef<Int64Type>,
        ctx: &mut EvalContext,
    ) -> Value<TimestampType> {
        vectorize_with_builder_1_arg::<Int64Type, TimestampType>(|val, output, ctx| {
            match int64_to_timestamp(val) {
                Ok(ts) => output.push(ts),
                Err(e) => {
                    ctx.set_error(output.len(), e);
                    output.push(0);
                }
            }
        })(val, ctx)
    }
}

fn register_string_to_date(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<StringType, DateType, _, _>(
        "to_date",
        |_| FunctionDomain::MayThrow,
        eval_string_to_date,
    );
    registry.register_combine_nullable_1_arg::<StringType, DateType, _, _>(
        "try_to_date",
        |_| FunctionDomain::Full,
        error_to_null(eval_string_to_date),
    );

    fn eval_string_to_date(val: ValueRef<StringType>, ctx: &mut EvalContext) -> Value<DateType> {
        vectorize_with_builder_1_arg::<StringType, DateType>(
            |val, output, ctx| match string_to_date(val, ctx.func_ctx.tz.tz) {
                Some(d) => output.push(d.num_days_from_ce() - EPOCH_DAYS_FROM_CE),
                None => {
                    ctx.set_error(output.len(), "cannot parse to type `DATE`");
                    output.push(0);
                }
            },
        )(val, ctx)
    }
}

fn register_timestamp_to_date(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<TimestampType, DateType, _, _>(
        "to_date",
        |domain| {
            FunctionDomain::Domain(SimpleDomain {
                min: (domain.min / 1000000 / 24 / 3600) as i32,
                max: (domain.max / 1000000 / 24 / 3600) as i32,
            })
        },
        eval_timestamp_to_date,
    );
    registry.register_combine_nullable_1_arg::<TimestampType, DateType, _, _>(
        "try_to_date",
        |domain| {
            FunctionDomain::Domain(NullableDomain {
                has_null: false,
                value: Some(Box::new(SimpleDomain {
                    min: (domain.min / 1000000 / 24 / 3600) as i32,
                    max: (domain.max / 1000000 / 24 / 3600) as i32,
                })),
            })
        },
        error_to_null(eval_timestamp_to_date),
    );

    fn eval_timestamp_to_date(
        val: ValueRef<TimestampType>,
        ctx: &mut EvalContext,
    ) -> Value<DateType> {
        vectorize_with_builder_1_arg::<TimestampType, DateType>(|val, output, _| {
            output.push(microseconds_to_days(val));
        })(val, ctx)
    }
}

fn register_number_to_date(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<Int64Type, DateType, _, _>(
        "to_date",
        |domain| {
            let (domain, overflowing) = domain.overflow_cast_with_minmax(DATE_MIN, DATE_MAX);
            if overflowing {
                FunctionDomain::MayThrow
            } else {
                FunctionDomain::Domain(domain)
            }
        },
        eval_number_to_date,
    );
    registry.register_combine_nullable_1_arg::<Int64Type, DateType, _, _>(
        "try_to_date",
        |domain| {
            let (domain, overflowing) = domain.overflow_cast_with_minmax(DATE_MIN, DATE_MAX);
            FunctionDomain::Domain(NullableDomain {
                has_null: overflowing,
                value: Some(Box::new(domain)),
            })
        },
        error_to_null(eval_number_to_date),
    );

    fn eval_number_to_date(val: ValueRef<Int64Type>, ctx: &mut EvalContext) -> Value<DateType> {
        vectorize_with_builder_1_arg::<Int64Type, DateType>(|val, output, ctx| {
            match check_date(val) {
                Ok(d) => output.push(d),
                Err(e) => {
                    ctx.set_error(output.len(), e);
                    output.push(0);
                }
            }
        })(val, ctx)
    }
}

fn register_to_string(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_1_arg::<DateType, StringType, _, _>(
        "to_string",
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<DateType, StringType>(|val, output, ctx| {
            write!(output.data, "{}", date_to_string(val, ctx.func_ctx.tz.tz)).unwrap();
            output.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<TimestampType, StringType, _, _>(
        "to_string",
        |_| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<TimestampType, StringType>(|val, output, ctx| {
            write!(
                output.data,
                "{}",
                timestamp_to_string(val, ctx.func_ctx.tz.tz)
            )
            .unwrap();
            output.commit_row();
        }),
    );

    registry.register_combine_nullable_1_arg::<DateType, StringType, _, _>(
        "try_to_string",
        |_| {
            FunctionDomain::Domain(NullableDomain {
                has_null: false,
                value: Some(Box::new(StringDomain {
                    min: vec![],
                    max: None,
                })),
            })
        },
        vectorize_with_builder_1_arg::<DateType, NullableType<StringType>>(|val, output, ctx| {
            write!(
                output.builder.data,
                "{}",
                date_to_string(val, ctx.func_ctx.tz.tz)
            )
            .unwrap();
            output.builder.commit_row();
            output.validity.push(true);
        }),
    );

    registry.register_combine_nullable_1_arg::<TimestampType, StringType, _, _>(
        "try_to_string",
        |_| {
            FunctionDomain::Domain(NullableDomain {
                has_null: false,
                value: Some(Box::new(StringDomain {
                    min: vec![],
                    max: None,
                })),
            })
        },
        vectorize_with_builder_1_arg::<TimestampType, NullableType<StringType>>(
            |val, output, ctx| {
                write!(
                    output.builder.data,
                    "{}",
                    timestamp_to_string(val, ctx.func_ctx.tz.tz)
                )
                .unwrap();
                output.builder.commit_row();
                output.validity.push(true);
            },
        ),
    );
}

fn register_to_number(registry: &mut FunctionRegistry) {
    registry.register_1_arg::<DateType, NumberType<i64>, _, _>(
        "to_int64",
        |domain| FunctionDomain::Domain(domain.overflow_cast().0),
        |val, _| val as i64,
    );

    registry.register_passthrough_nullable_1_arg::<TimestampType, NumberType<i64>, _, _>(
        "to_int64",
        |domain| FunctionDomain::Domain(*domain),
        |val, _| match val {
            ValueRef::Scalar(scalar) => Value::Scalar(scalar),
            ValueRef::Column(col) => Value::Column(col),
        },
    );

    registry.register_combine_nullable_1_arg::<DateType, NumberType<i64>, _, _>(
        "try_to_int64",
        |domain| {
            FunctionDomain::Domain(NullableDomain {
                has_null: false,
                value: Some(Box::new(domain.overflow_cast().0)),
            })
        },
        |val, _| match val {
            ValueRef::Scalar(scalar) => Value::Scalar(Some(scalar as i64)),
            ValueRef::Column(col) => Value::Column(NullableColumn {
                validity: constant_bitmap(true, col.len()).into(),
                column: col.iter().map(|val| *val as i64).collect(),
            }),
        },
    );

    registry.register_combine_nullable_1_arg::<TimestampType, NumberType<i64>, _, _>(
        "try_to_int64",
        |domain| {
            FunctionDomain::Domain(NullableDomain {
                has_null: false,
                value: Some(Box::new(*domain)),
            })
        },
        |val, _| match val {
            ValueRef::Scalar(scalar) => Value::Scalar(Some(scalar)),
            ValueRef::Column(col) => Value::Column(NullableColumn {
                validity: constant_bitmap(true, col.len()).into(),
                column: col,
            }),
        },
    );
}

macro_rules! signed_ident {
    ($name: ident) => {
        -$name
    };
}

macro_rules! unsigned_ident {
    ($name: ident) => {
        $name
    };
}

macro_rules! impl_register_arith_functions {
    ($name: ident, $op: literal, $signed_wrapper: tt) => {
        fn $name(registry: &mut FunctionRegistry) {
            registry.register_passthrough_nullable_2_arg::<DateType, Int64Type, DateType, _, _>(
                concat!($op, "_years"),

                |_, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<DateType, Int64Type, DateType>(|date, delta, builder, ctx| {
                    match AddYearsImpl::eval_date(date, ctx.func_ctx.tz, $signed_wrapper!{delta}) {
                        Ok(t) => builder.push(t),
                        Err(e) => {
                            ctx.set_error(builder.len(), e);
                            builder.push(0);
                        },
                    }
                }),
            );
            registry.register_passthrough_nullable_2_arg::<TimestampType, Int64Type, TimestampType, _, _>(
                concat!($op, "_years"),

                |_, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<TimestampType, Int64Type, TimestampType>(
                    |ts, delta, builder, ctx| {
                        match AddYearsImpl::eval_timestamp(ts, ctx.func_ctx.tz, $signed_wrapper!{delta}) {
                            Ok(t) => builder.push(t),
                            Err(e) => {
                                ctx.set_error(builder.len(), e);
                                builder.push(0);
                            },
                        }
                    },
                ),
            );

            registry.register_passthrough_nullable_2_arg::<DateType, Int64Type, DateType, _, _>(
                concat!($op, "_quarters"),

                |_, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<DateType, Int64Type, DateType>(|date, delta, builder, ctx| {
                    match AddMonthsImpl::eval_date(date, ctx.func_ctx.tz, $signed_wrapper!{delta} * 3) {
                        Ok(t) => builder.push(t),
                        Err(e) => {
                            ctx.set_error(builder.len(), e);
                            builder.push(0);
                        },
                    }
                }),
            );
            registry.register_passthrough_nullable_2_arg::<TimestampType, Int64Type, TimestampType, _, _>(
                concat!($op, "_quarters"),

                |_, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<TimestampType, Int64Type, TimestampType>(
                    |ts, delta, builder, ctx| {
                        match AddMonthsImpl::eval_timestamp(ts, ctx.func_ctx.tz, $signed_wrapper!{delta} * 3) {
                            Ok(t) => builder.push(t),
                            Err(e) => {
                                ctx.set_error(builder.len(), e);
                                builder.push(0);
                            },
                        }
                    },
                ),
            );

            registry.register_passthrough_nullable_2_arg::<DateType, Int64Type, DateType, _, _>(
                concat!($op, "_months"),

                |_, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<DateType, Int64Type, DateType>(|date, delta, builder, ctx| {
                    match AddMonthsImpl::eval_date(date, ctx.func_ctx.tz, $signed_wrapper!{delta}) {
                        Ok(t) => builder.push(t),
                        Err(e) => {
                            ctx.set_error(builder.len(), e);
                            builder.push(0);
                        },
                    }
                }),
            );
            registry.register_passthrough_nullable_2_arg::<TimestampType, Int64Type, TimestampType, _, _>(
                concat!($op, "_months"),

                |_, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<TimestampType, Int64Type, TimestampType>(
                    |ts, delta, builder, ctx| {
                        match AddMonthsImpl::eval_timestamp(ts, ctx.func_ctx.tz, $signed_wrapper!{delta}) {
                            Ok(t) => builder.push(t),
                            Err(e) => {
                                ctx.set_error(builder.len(), e);
                                builder.push(0);
                            },
                        }
                    },
                ),
            );

            registry.register_passthrough_nullable_2_arg::<DateType, Int64Type, DateType, _, _>(
                concat!($op, "_days"),

                |_, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<DateType, Int64Type, DateType>(|date, delta, builder, ctx| {
                    match AddDaysImpl::eval_date(date, $signed_wrapper!{delta}) {
                        Ok(t) => builder.push(t),
                        Err(e) => {
                            ctx.set_error(builder.len(), e);
                            builder.push(0);
                        },
                    }
                }),
            );
            registry.register_passthrough_nullable_2_arg::<TimestampType, Int64Type, TimestampType, _, _>(
                concat!($op, "_days"),

                |_, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<TimestampType, Int64Type, TimestampType>(
                    |ts, delta, builder, ctx| {
                        match AddDaysImpl::eval_timestamp(ts, $signed_wrapper!{delta}) {
                            Ok(t) => builder.push(t),
                            Err(e) => {
                                ctx.set_error(builder.len(), e);
                                builder.push(0);
                            },
                        }
                    },
                ),
            );

            registry.register_passthrough_nullable_2_arg::<DateType, Int64Type, TimestampType, _, _>(
                concat!($op, "_hours"),

                |_, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<DateType, Int64Type, TimestampType>(
                    |ts, delta, builder, ctx| {
                        let val = (ts as i64) * 24 * 3600 * MICROS_IN_A_SEC;
                        match AddTimesImpl::eval_timestamp(
                            val,
                            $signed_wrapper!{delta},
                            FACTOR_HOUR,
                        ) {
                            Ok(t) => builder.push(t),
                            Err(e) => {
                                ctx.set_error(builder.len(), e);
                                builder.push(0);
                            },
                        }
                    },
                ),
            );
            registry.register_passthrough_nullable_2_arg::<TimestampType, Int64Type, TimestampType, _, _>(
                concat!($op, "_hours"),

                |_, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<TimestampType, Int64Type, TimestampType>(
                    |ts, delta, builder, ctx| {
                        match AddTimesImpl::eval_timestamp(
                            ts,
                            $signed_wrapper!{delta},
                            FACTOR_HOUR,
                        ) {
                            Ok(t) => builder.push(t),
                            Err(e) => {
                                ctx.set_error(builder.len(), e);
                                builder.push(0);
                            },
                        }
                    },
                ),
            );

            registry.register_passthrough_nullable_2_arg::<DateType, Int64Type, TimestampType, _, _>(
                concat!($op, "_minutes"),

                |_, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<DateType, Int64Type, TimestampType>(
                    |ts, delta, builder, ctx| {
                        let val = (ts as i64) * 24 * 3600 * MICROS_IN_A_SEC;

                        match AddTimesImpl::eval_timestamp(
                            val,
                            $signed_wrapper!{delta},
                            FACTOR_MINUTE,
                        ) {
                            Ok(t) => builder.push(t),
                            Err(e) => {
                                ctx.set_error(builder.len(), e);
                                builder.push(0);
                            },
                        }
                    },
                ),
            );
            registry.register_passthrough_nullable_2_arg::<TimestampType, Int64Type, TimestampType, _, _>(
                concat!($op, "_minutes"),

                |_, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<TimestampType, Int64Type, TimestampType>(
                    |ts, delta, builder, ctx| {
                        match AddTimesImpl::eval_timestamp(
                            ts,
                            $signed_wrapper!{delta},
                            FACTOR_MINUTE,
                        ) {
                            Ok(t) => builder.push(t),
                            Err(e) => {
                                ctx.set_error(builder.len(), e);
                                builder.push(0);
                            },
                        }
                    },
                ),
            );

            registry.register_passthrough_nullable_2_arg::<DateType, Int64Type, TimestampType, _, _>(
                concat!($op, "_seconds"),

                |_, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<DateType, Int64Type, TimestampType>(
                    |ts, delta, builder, ctx| {
                        let val = (ts as i64) * 24 * 3600 * MICROS_IN_A_SEC;

                        match AddTimesImpl::eval_timestamp(
                            val,
                            $signed_wrapper!{delta},
                            FACTOR_SECOND,
                        ) {
                            Ok(t) => builder.push(t),
                            Err(e) => {
                                ctx.set_error(builder.len(), e);
                                builder.push(0);
                            },
                        }
                    },
                ),
            );
            registry.register_passthrough_nullable_2_arg::<TimestampType, Int64Type, TimestampType, _, _>(
                concat!($op, "_seconds"),

                |_, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<TimestampType, Int64Type, TimestampType>(
                    |ts, delta, builder, ctx| {
                        match AddTimesImpl::eval_timestamp(
                            ts,
                            $signed_wrapper!{delta},
                            FACTOR_SECOND,
                        ) {
                            Ok(t) => builder.push(t),
                            Err(e) => {
                                ctx.set_error(builder.len(), e);
                                builder.push(0);
                            },
                        }
                    },
                ),
            );
        }
    };
}

impl_register_arith_functions!(register_add_functions, "add", unsigned_ident);
impl_register_arith_functions!(register_sub_functions, "subtract", signed_ident);

fn register_real_time_functions(registry: &mut FunctionRegistry) {
    registry.properties.insert(
        "now".to_string(),
        FunctionProperty::default().non_deterministic(),
    );
    registry.properties.insert(
        "today".to_string(),
        FunctionProperty::default().non_deterministic(),
    );
    registry.properties.insert(
        "yesterday".to_string(),
        FunctionProperty::default().non_deterministic(),
    );
    registry.properties.insert(
        "tomorrow".to_string(),
        FunctionProperty::default().non_deterministic(),
    );

    registry.register_0_arg_core::<TimestampType, _, _>(
        "now",
        || FunctionDomain::Full,
        |_| Value::Scalar(Utc::now().timestamp_micros()),
    );

    registry.register_0_arg_core::<DateType, _, _>(
        "today",
        || FunctionDomain::Full,
        |_| Value::Scalar(today_date()),
    );

    registry.register_0_arg_core::<DateType, _, _>(
        "yesterday",
        || FunctionDomain::Full,
        |_| Value::Scalar(today_date() - 1),
    );

    registry.register_0_arg_core::<DateType, _, _>(
        "tomorrow",
        || FunctionDomain::Full,
        |_| Value::Scalar(today_date() + 1),
    );
}

fn register_to_number_functions(registry: &mut FunctionRegistry) {
    // date
    registry.register_passthrough_nullable_1_arg::<DateType, UInt32Type, _, _>(
        "to_yyyymm",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt32Type>(|val, ctx| {
            ToNumberImpl::eval_date::<ToYYYYMM, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt32Type, _, _>(
        "to_yyyymmdd",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt32Type>(|val, ctx| {
            ToNumberImpl::eval_date::<ToYYYYMMDD, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt64Type, _, _>(
        "to_yyyymmddhhmmss",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt64Type>(|val, ctx| {
            ToNumberImpl::eval_date::<ToYYYYMMDDHHMMSS, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt16Type, _, _>(
        "to_year",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt16Type>(|val, ctx| {
            ToNumberImpl::eval_date::<ToYear, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt8Type, _, _>(
        "to_month",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt8Type>(|val, ctx| {
            ToNumberImpl::eval_date::<ToMonth, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt16Type, _, _>(
        "to_day_of_year",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt16Type>(|val, ctx| {
            ToNumberImpl::eval_date::<ToDayOfYear, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt8Type, _, _>(
        "to_day_of_month",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt8Type>(|val, ctx| {
            ToNumberImpl::eval_date::<ToDayOfMonth, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<DateType, UInt8Type, _, _>(
        "to_day_of_week",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<DateType, UInt8Type>(|val, ctx| {
            ToNumberImpl::eval_date::<ToDayOfWeek, _>(val, ctx.func_ctx.tz)
        }),
    );
    // timestamp
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt32Type, _, _>(
        "to_yyyymm",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, UInt32Type>(|val, ctx| {
            ToNumberImpl::eval_timestamp::<ToYYYYMM, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt32Type, _, _>(
        "to_yyyymmdd",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, UInt32Type>(|val, ctx| {
            ToNumberImpl::eval_timestamp::<ToYYYYMMDD, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt64Type, _, _>(
        "to_yyyymmddhhmmss",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, UInt64Type>(|val, ctx| {
            ToNumberImpl::eval_timestamp::<ToYYYYMMDDHHMMSS, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt16Type, _, _>(
        "to_year",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, UInt16Type>(|val, ctx| {
            ToNumberImpl::eval_timestamp::<ToYear, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt8Type, _, _>(
        "to_month",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, UInt8Type>(|val, ctx| {
            ToNumberImpl::eval_timestamp::<ToMonth, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt16Type, _, _>(
        "to_day_of_year",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, UInt16Type>(|val, ctx| {
            ToNumberImpl::eval_timestamp::<ToDayOfYear, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt8Type, _, _>(
        "to_day_of_month",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, UInt8Type>(|val, ctx| {
            ToNumberImpl::eval_timestamp::<ToDayOfMonth, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt8Type, _, _>(
        "to_day_of_week",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, UInt8Type>(|val, ctx| {
            ToNumberImpl::eval_timestamp::<ToDayOfWeek, _>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, Int64Type, _, _>(
        "to_unix_timestamp",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, Int64Type>(|val, ctx| {
            ToNumberImpl::eval_timestamp::<ToUnixTimestamp, _>(val, ctx.func_ctx.tz)
        }),
    );

    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt8Type, _, _>(
        "to_hour",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, UInt8Type>(|val, ctx| ctx.func_ctx.tz.to_hour(val)),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt8Type, _, _>(
        "to_minute",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, UInt8Type>(|val, ctx| ctx.func_ctx.tz.to_minute(val)),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, UInt8Type, _, _>(
        "to_second",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, UInt8Type>(|val, ctx| ctx.func_ctx.tz.to_second(val)),
    );
}

fn register_timestamp_add_sub(registry: &mut FunctionRegistry) {
    registry.register_2_arg::<DateType, Int64Type, DateType, _, _>(
        "plus",
        |lhs, rhs| {
            (|| {
                let lm = lhs.max;
                let ln = lhs.min;
                let rm: i32 = num_traits::cast::cast(rhs.max)?;
                let rn: i32 = num_traits::cast::cast(rhs.min)?;

                Some(FunctionDomain::Domain(SimpleDomain::<i32> {
                    min: ln.checked_add(rn)?,
                    max: lm.checked_add(rm)?,
                }))
            })()
            .unwrap_or(FunctionDomain::Full)
        },
        |a, b, _| a + (b as i32),
    );

    registry.register_2_arg::<DateType, DateType, Int32Type, _, _>(
        "plus",
        |lhs, rhs| {
            (|| {
                let lm = lhs.max;
                let ln = lhs.min;
                let rm: i32 = num_traits::cast::cast(rhs.max)?;
                let rn: i32 = num_traits::cast::cast(rhs.min)?;

                Some(FunctionDomain::Domain(SimpleDomain::<i32> {
                    min: ln.checked_add(rn)?,
                    max: lm.checked_add(rm)?,
                }))
            })()
            .unwrap_or(FunctionDomain::Full)
        },
        |a, b, _| a + b,
    );

    registry.register_2_arg::<TimestampType, Int64Type, TimestampType, _, _>(
        "plus",
        |lhs, rhs| {
            (|| {
                let lm = lhs.max;
                let ln = lhs.min;
                let rm = rhs.max;
                let rn = rhs.min;
                Some(FunctionDomain::Domain(SimpleDomain::<i64> {
                    min: ln.checked_add(rn)?,
                    max: lm.checked_add(rm)?,
                }))
            })()
            .unwrap_or(FunctionDomain::Full)
        },
        |a, b, _| a + b,
    );

    registry.register_2_arg::<TimestampType, TimestampType, Int64Type, _, _>(
        "plus",
        |lhs, rhs| {
            (|| {
                let lm = lhs.max;
                let ln = lhs.min;
                let rm = rhs.max;
                let rn = rhs.min;
                Some(FunctionDomain::Domain(SimpleDomain::<i64> {
                    min: ln.checked_add(rn)?,
                    max: lm.checked_add(rm)?,
                }))
            })()
            .unwrap_or(FunctionDomain::Full)
        },
        |a, b, _| a + b,
    );

    registry.register_2_arg::<DateType, Int64Type, DateType, _, _>(
        "minus",
        |lhs, rhs| {
            (|| {
                let lm = lhs.max;
                let ln = lhs.min;
                let rm: i32 = num_traits::cast::cast(rhs.max)?;
                let rn: i32 = num_traits::cast::cast(rhs.min)?;

                Some(FunctionDomain::Domain(SimpleDomain::<i32> {
                    min: ln.checked_sub(rm)?,
                    max: lm.checked_sub(rn)?,
                }))
            })()
            .unwrap_or(FunctionDomain::Full)
        },
        |a, b, _| a - b as i32,
    );

    registry.register_2_arg::<DateType, DateType, Int32Type, _, _>(
        "minus",
        |lhs, rhs| {
            (|| {
                let lm = lhs.max;
                let ln = lhs.min;
                let rm: i32 = num_traits::cast::cast(rhs.max)?;
                let rn: i32 = num_traits::cast::cast(rhs.min)?;

                Some(FunctionDomain::Domain(SimpleDomain::<i32> {
                    min: ln.checked_sub(rm)?,
                    max: lm.checked_sub(rn)?,
                }))
            })()
            .unwrap_or(FunctionDomain::Full)
        },
        |a, b, _| a - b,
    );

    registry.register_2_arg::<TimestampType, Int64Type, TimestampType, _, _>(
        "minus",
        |lhs, rhs| {
            (|| {
                let lm = lhs.max;
                let ln = lhs.min;
                let rm = rhs.max;
                let rn = rhs.min;

                Some(FunctionDomain::Domain(SimpleDomain::<i64> {
                    min: ln.checked_sub(rm)?,
                    max: lm.checked_sub(rn)?,
                }))
            })()
            .unwrap_or(FunctionDomain::Full)
        },
        |a, b, _| a - b,
    );

    registry.register_2_arg::<TimestampType, TimestampType, Int64Type, _, _>(
        "minus",
        |lhs, rhs| {
            (|| {
                let lm = lhs.max;
                let ln = lhs.min;
                let rm = rhs.max;
                let rn = rhs.min;

                Some(FunctionDomain::Domain(SimpleDomain::<i64> {
                    min: ln.checked_sub(rm)?,
                    max: lm.checked_sub(rn)?,
                }))
            })()
            .unwrap_or(FunctionDomain::Full)
        },
        |a, b, _| a - b,
    );
}

fn register_rounder_functions(registry: &mut FunctionRegistry) {
    // timestamp -> timestamp
    registry.register_passthrough_nullable_1_arg::<TimestampType, TimestampType, _, _>(
        "to_start_of_second",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, TimestampType>(|val, ctx| {
            ctx.func_ctx.tz.round_us(val, Round::Second)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, TimestampType, _, _>(
        "to_start_of_minute",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, TimestampType>(|val, ctx| {
            ctx.func_ctx.tz.round_us(val, Round::Minute)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, TimestampType, _, _>(
        "to_start_of_five_minutes",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, TimestampType>(|val, ctx| {
            ctx.func_ctx.tz.round_us(val, Round::FiveMinutes)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, TimestampType, _, _>(
        "to_start_of_ten_minutes",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, TimestampType>(|val, ctx| {
            ctx.func_ctx.tz.round_us(val, Round::TenMinutes)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, TimestampType, _, _>(
        "to_start_of_fifteen_minutes",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, TimestampType>(|val, ctx| {
            ctx.func_ctx.tz.round_us(val, Round::FifteenMinutes)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, TimestampType, _, _>(
        "to_start_of_hour",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, TimestampType>(|val, ctx| {
            ctx.func_ctx.tz.round_us(val, Round::Hour)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, TimestampType, _, _>(
        "to_start_of_day",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, TimestampType>(|val, ctx| {
            ctx.func_ctx.tz.round_us(val, Round::Day)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, TimestampType, _, _>(
        "time_slot",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, TimestampType>(|val, ctx| {
            ctx.func_ctx.tz.round_us(val, Round::TimeSlot)
        }),
    );

    // date | timestamp -> date
    registry.register_passthrough_nullable_1_arg::<DateType, DateType, _, _>(
        "to_monday",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<DateType, DateType>(|val, ctx| {
            DateRounder::eval_date::<ToLastMonday>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, DateType, _, _>(
        "to_monday",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, DateType>(|val, ctx| {
            DateRounder::eval_timestamp::<ToLastMonday>(val, ctx.func_ctx.tz)
        }),
    );

    registry.register_passthrough_nullable_1_arg::<DateType, DateType, _, _>(
        "to_start_of_week",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<DateType, DateType>(|val, ctx| {
            DateRounder::eval_date::<ToLastSunday>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, DateType, _, _>(
        "to_start_of_week",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, DateType>(|val, ctx| {
            DateRounder::eval_timestamp::<ToLastSunday>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_2_arg::<DateType, Int64Type, DateType, _, _>(
        "to_start_of_week",
        |_, _| FunctionDomain::Full,
        vectorize_2_arg::<DateType, Int64Type, DateType>(|val, mode, ctx| {
            if mode == 0 {
                DateRounder::eval_date::<ToLastSunday>(val, ctx.func_ctx.tz)
            } else {
                DateRounder::eval_date::<ToLastMonday>(val, ctx.func_ctx.tz)
            }
        }),
    );
    registry.register_passthrough_nullable_2_arg::<TimestampType, Int64Type, DateType, _, _>(
        "to_start_of_week",
        |_, _| FunctionDomain::Full,
        vectorize_2_arg::<TimestampType, Int64Type, DateType>(|val, mode, ctx| {
            if mode == 0 {
                DateRounder::eval_timestamp::<ToLastSunday>(val, ctx.func_ctx.tz)
            } else {
                DateRounder::eval_timestamp::<ToLastMonday>(val, ctx.func_ctx.tz)
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<DateType, DateType, _, _>(
        "to_start_of_month",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<DateType, DateType>(|val, ctx| {
            DateRounder::eval_date::<ToStartOfMonth>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, DateType, _, _>(
        "to_start_of_month",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, DateType>(|val, ctx| {
            DateRounder::eval_timestamp::<ToStartOfMonth>(val, ctx.func_ctx.tz)
        }),
    );

    registry.register_passthrough_nullable_1_arg::<DateType, DateType, _, _>(
        "to_start_of_quarter",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<DateType, DateType>(|val, ctx| {
            DateRounder::eval_date::<ToStartOfQuarter>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, DateType, _, _>(
        "to_start_of_quarter",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, DateType>(|val, ctx| {
            DateRounder::eval_timestamp::<ToStartOfQuarter>(val, ctx.func_ctx.tz)
        }),
    );

    registry.register_passthrough_nullable_1_arg::<DateType, DateType, _, _>(
        "to_start_of_year",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<DateType, DateType>(|val, ctx| {
            DateRounder::eval_date::<ToStartOfYear>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, DateType, _, _>(
        "to_start_of_year",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, DateType>(|val, ctx| {
            DateRounder::eval_timestamp::<ToStartOfYear>(val, ctx.func_ctx.tz)
        }),
    );

    registry.register_passthrough_nullable_1_arg::<DateType, DateType, _, _>(
        "to_start_of_iso_year",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<DateType, DateType>(|val, ctx| {
            DateRounder::eval_date::<ToStartOfISOYear>(val, ctx.func_ctx.tz)
        }),
    );
    registry.register_passthrough_nullable_1_arg::<TimestampType, DateType, _, _>(
        "to_start_of_iso_year",
        |_| FunctionDomain::Full,
        vectorize_1_arg::<TimestampType, DateType>(|val, ctx| {
            DateRounder::eval_timestamp::<ToStartOfISOYear>(val, ctx.func_ctx.tz)
        }),
    );
}
