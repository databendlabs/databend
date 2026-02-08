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

use std::cmp::Ordering;
use std::f64::consts::E;
use std::f64::consts::PI;
use std::marker::PhantomData;

use databend_common_base::base::OrderedFloat;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Value;
use databend_common_expression::function_stat::Ndv;
use databend_common_expression::function_stat::ReturnStat;
use databend_common_expression::types::ALL_FLOAT_TYPES;
use databend_common_expression::types::ALL_INTEGER_TYPES;
use databend_common_expression::types::ALL_NUMERICS_TYPES;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::boolean::BooleanDomain;
use databend_common_expression::types::number::F64;
use databend_common_expression::types::number::SimpleDomain;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::with_number_mapped_type;
use databend_functions_scalar_decimal::register_decimal_math;
use num_traits::AsPrimitive;
use num_traits::Float;
use num_traits::Pow;

pub fn register(registry: &mut FunctionRegistry) {
    register_decimal_math(registry);

    registry.register_1_arg::<NumberType<F64>, NumberType<F64>, _>(
        "sin",
        |_, _| {
            FunctionDomain::Domain(SimpleDomain {
                min: OrderedFloat(-1.0),
                max: OrderedFloat(1.0),
            })
        },
        |f: F64, _| f.sin(),
    );

    registry.register_1_arg::<NumberType<F64>, NumberType<F64>, _>(
        "cos",
        |_, _| {
            FunctionDomain::Domain(SimpleDomain {
                min: OrderedFloat(-1.0),
                max: OrderedFloat(1.0),
            })
        },
        |f: F64, _| f.cos(),
    );

    registry.register_1_arg::<NumberType<F64>, NumberType<F64>, _>(
        "tan",
        |_, _| FunctionDomain::Full,
        |f: F64, _| f.tan(),
    );

    registry.register_1_arg::<NumberType<F64>, NumberType<F64>, _>(
        "cot",
        |_, _| FunctionDomain::Full,
        |f: F64, _| OrderedFloat(1.0f64) / f.tan(),
    );

    registry.register_1_arg::<NumberType<F64>, NumberType<F64>, _>(
        "acos",
        |_, _| {
            FunctionDomain::Domain(SimpleDomain {
                min: OrderedFloat(0.0),
                max: OrderedFloat(PI),
            })
        },
        |f: F64, _| f.acos(),
    );

    registry.register_1_arg::<NumberType<F64>, NumberType<F64>, _>(
        "asin",
        |_, _| {
            FunctionDomain::Domain(SimpleDomain {
                min: OrderedFloat(0.0),
                max: OrderedFloat(2.0 * PI),
            })
        },
        |f: F64, _| f.asin(),
    );

    registry.register_1_arg::<NumberType<F64>, NumberType<F64>, _>(
        "atan",
        |_, _| {
            FunctionDomain::Domain(SimpleDomain {
                min: OrderedFloat(-PI / 2.0),
                max: OrderedFloat(PI / 2.0),
            })
        },
        |f: F64, _| f.atan(),
    );

    registry.register_2_arg::<NumberType<F64>, NumberType<F64>, NumberType<F64>, _>(
        "atan2",
        |_, _, _| {
            FunctionDomain::Domain(SimpleDomain {
                min: OrderedFloat(-PI),
                max: OrderedFloat(PI),
            })
        },
        |f: F64, r: F64, _| f.atan2(r),
    );

    registry
        .scalar_builder("pi")
        .function()
        .typed_0_arg::<Float64Type>()
        .calc_domain(|_| {
            FunctionDomain::Domain(SimpleDomain {
                min: OrderedFloat(PI),
                max: OrderedFloat(PI),
            })
        })
        .derive_stat(|_, _| {
            Ok(Some(ReturnStat {
                ndv: Ndv::Stat(1.0),
                null_count: 0,
                domain: Float64Type::upcast_domain(SimpleDomain {
                    min: OrderedFloat(PI),
                    max: OrderedFloat(PI),
                }),
                histogram: None,
            }))
        })
        .vectorized(|_| Value::Scalar(OrderedFloat(PI)))
        .register();

    fn sign(val: F64) -> i8 {
        match val.partial_cmp(&OrderedFloat(0.0f64)) {
            Some(Ordering::Greater) => 1,
            Some(Ordering::Less) => -1,
            _ => 0,
        }
    }

    registry.register_1_arg::<NumberType<F64>, NumberType<i8>, _>(
        "sign",
        move |_, domain| {
            FunctionDomain::Domain(SimpleDomain {
                min: sign(domain.min),
                max: sign(domain.max),
            })
        },
        move |val, _| sign(val),
    );

    registry.register_1_arg::<NumberType<u64>, NumberType<u64>, _>(
        "abs",
        |_, domain| FunctionDomain::Domain(*domain),
        |val, _| val,
    );

    registry.register_1_arg::<NumberType<i64>, NumberType<u64>, _>(
        "abs",
        |_, domain| {
            let max = domain.min.unsigned_abs().max(domain.max.unsigned_abs());
            let mut min = domain.min.unsigned_abs().min(domain.max.unsigned_abs());

            if domain.max >= 0 && domain.min <= 0 {
                min = 0;
            }
            FunctionDomain::Domain(SimpleDomain { min, max })
        },
        |val, _| val.unsigned_abs(),
    );

    registry.register_1_arg::<NumberType<F64>, NumberType<F64>, _>(
        "abs",
        |_, domain| {
            let max = Ord::max(domain.min.abs(), domain.max.abs());
            let mut min = Ord::min(domain.min.abs(), domain.max.abs());

            if domain.max > OrderedFloat(0.0) && domain.min <= OrderedFloat(0.0) {
                min = OrderedFloat(0.0);
            }
            FunctionDomain::Domain(SimpleDomain { min, max })
        },
        |val, _| val.abs(),
    );

    for ty in ALL_INTEGER_TYPES {
        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry.register_1_arg::<NumberType<NUM_TYPE>, NumberType<NUM_TYPE>, _>(
                    "ceil",
                    |_, _| FunctionDomain::Full,
                    |val, _| val,
                );
            }
        })
    }

    for ty in ALL_FLOAT_TYPES {
        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry.register_1_arg::<NumberType<NUM_TYPE>, NumberType<F64>, _>(
                    "ceil",
                    |_, _| FunctionDomain::Full,
                    |val, _| (F64::from(AsPrimitive::<f64>::as_(val))).ceil(),
                );
            }
        })
    }

    registry.register_aliases("ceil", &["ceiling"]);
    registry.register_aliases("truncate", &["trunc"]);

    registry.register_1_arg::<StringType, NumberType<u32>, _>(
        "crc32",
        |_, _| FunctionDomain::Full,
        |val, _| crc32fast::hash(val.as_bytes()),
    );

    registry.register_1_arg::<NumberType<F64>, NumberType<F64>, _>(
        "degrees",
        |_, _| FunctionDomain::Full,
        |val, _| val.to_degrees(),
    );

    registry.register_1_arg::<NumberType<F64>, NumberType<F64>, _>(
        "radians",
        |_, _| FunctionDomain::Full,
        |val, _| val.to_radians(),
    );

    for ty in ALL_NUMERICS_TYPES {
        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry.register_1_arg::<NumberType<NUM_TYPE>, NumberType<F64>, _>(
                    "exp",
                    |_, _| FunctionDomain::Full,
                    |val, _| (F64::from(AsPrimitive::<f64>::as_(val))).exp(),
                );
            }
        })
    }

    registry.register_1_arg::<NumberType<F64>, NumberType<F64>, _>(
        "floor",
        |_, _| FunctionDomain::Full,
        |val, _| val.floor(),
    );

    registry.register_2_arg::<NumberType<F64>, NumberType<F64>, NumberType<F64>, _>(
        "pow",
        |_, _, _| FunctionDomain::Full,
        |lhs, rhs, _| OrderedFloat(lhs.0.pow(rhs.0)),
    );

    for ty in ALL_NUMERICS_TYPES {
        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry.register_1_arg::<NumberType<NUM_TYPE>, NumberType<F64>, _>(
                    "round",
                    |_, _| FunctionDomain::Full,
                    |val, _| (F64::from(AsPrimitive::<f64>::as_(val))).round(),
                );
            }
        });

        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry
                    .register_2_arg::<NumberType<NUM_TYPE>, NumberType<i64>, NumberType<F64>, _>(
                        "round",
                        |_, _, _| FunctionDomain::Full,
                        |val, to, _| match to.cmp(&0) {
                            Ordering::Greater => {
                                let z = 10_f64.powi(if to > 30 { 30 } else { to as i32 });
                                (F64::from(AsPrimitive::<f64>::as_(val)) * z).round() / z
                            }
                            Ordering::Less => {
                                let z = 10_f64.powi(if to < -30 { 30 } else { -to as i32 });
                                (F64::from(AsPrimitive::<f64>::as_(val)) / z).round() * z
                            }
                            Ordering::Equal => (F64::from(AsPrimitive::<f64>::as_(val))).round(),
                        },
                    );
            }
        });

        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry.register_1_arg::<NumberType<NUM_TYPE>, NumberType<F64>, _>(
                    "truncate",
                    |_, _| FunctionDomain::Full,
                    |val, _| (F64::from(AsPrimitive::<f64>::as_(val))).trunc(),
                );
            }
        });

        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry
                    .register_2_arg::<NumberType<NUM_TYPE>, NumberType<i64>, NumberType<F64>, _>(
                        "truncate",
                        |_, _, _| FunctionDomain::Full,
                        |val, to, _| match to.cmp(&0) {
                            Ordering::Greater => {
                                let z = 10_f64.powi(if to > 30 { 30 } else { to as i32 });
                                (F64::from(AsPrimitive::<f64>::as_(val)) * z).trunc() / z
                            }
                            Ordering::Less => {
                                let z = 10_f64.powi(if to < -30 { 30 } else { -to as i32 });
                                (F64::from(AsPrimitive::<f64>::as_(val)) / z).trunc() * z
                            }
                            Ordering::Equal => (F64::from(AsPrimitive::<f64>::as_(val))).trunc(),
                        },
                    );
            }
        });

        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry.register_1_arg::<NumberType<NUM_TYPE>, NumberType<F64>, _>(
                    "sqrt",
                    |_, _| FunctionDomain::Full,
                    |val, _| (F64::from(AsPrimitive::<f64>::as_(val))).sqrt(),
                );
            }
        });

        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry.register_1_arg::<NumberType<NUM_TYPE>, NumberType<F64>, _>(
                    "cbrt",
                    |_, _| FunctionDomain::Full,
                    |val, _| (F64::from(AsPrimitive::<f64>::as_(val))).cbrt(),
                );
            }
        });

        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry.register_1_arg::<NumberType<NUM_TYPE>, NumberType<F64>, _>(
                    "ln",
                    |_, _| FunctionDomain::Full,
                    |val, _| LnFunction::log(val),
                );
            }
        });

        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry.register_1_arg::<NumberType<NUM_TYPE>, NumberType<F64>, _>(
                    "log2",
                    |_, _| FunctionDomain::Full,
                    |val, _| Log2Function::log(val),
                );
            }
        });

        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry.register_1_arg::<NumberType<NUM_TYPE>, NumberType<F64>, _>(
                    "log10",
                    |_, _| FunctionDomain::Full,
                    |val, _| Log10Function::log(val),
                );
            }
        });

        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry.register_1_arg::<NumberType<NUM_TYPE>, NumberType<F64>, _>(
                    "log",
                    |_, _| FunctionDomain::Full,
                    |val, _| LogFunction::log(val),
                );
            }
        });

        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry
                    .register_2_arg::<NumberType<NUM_TYPE>, NumberType<F64>, NumberType<F64>, _>(
                        "log",
                        |_, _, _| FunctionDomain::Full,
                        |base, val, _| LogFunction::log_with_base(base, val),
                    );
            }
        });
    }

    const MAX_FACTORIAL_NUMBER: i64 = 20;
    for ty in ALL_INTEGER_TYPES {
        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry.register_passthrough_nullable_1_arg::<NumberType<NUM_TYPE>, NumberType<i64>, _>(
                    "factorial",
                    |_, _| FunctionDomain::MayThrow,
                    vectorize_with_builder_1_arg::<NumberType<NUM_TYPE>, NumberType<i64>>(
                        |val, output, ctx| {
                            let n: i64 = AsPrimitive::<i64>::as_(val);
                            if n > MAX_FACTORIAL_NUMBER {
                                ctx.set_error(output.len(), format!("factorial number is out of range, max is: {}", MAX_FACTORIAL_NUMBER));
                                output.push(0);
                            } else {
                                output.push(factorial(n));
                            }
                        },
                    ),
                );
            }
        })
    }

    // Register isnan function for float types
    for ty in ALL_FLOAT_TYPES {
        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry.register_1_arg::<NumberType<NUM_TYPE>, BooleanType, _>(
                    "isnan",
                    |_, _| {
                        FunctionDomain::Domain(BooleanDomain {
                            has_true: true,
                            has_false: true,
                        })
                    },
                    |val, _| {
                        let f64_val: f64 = AsPrimitive::<f64>::as_(val);
                        f64_val.is_nan()
                    },
                );
            }
        })
    }

    // Register isinf function for float types
    for ty in ALL_FLOAT_TYPES {
        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry.register_1_arg::<NumberType<NUM_TYPE>, BooleanType, _>(
                    "isinf",
                    |_, _| {
                        FunctionDomain::Domain(BooleanDomain {
                            has_true: true,
                            has_false: true,
                        })
                    },
                    |val, _| {
                        let f64_val: f64 = AsPrimitive::<f64>::as_(val);
                        f64_val.is_infinite()
                    },
                );
            }
        })
    }
}

/// Const f64 is now allowed.
/// feature(adt_const_params) is not stable & complete
trait Base: Send + Sync + Clone + 'static {
    fn base() -> F64;
}

#[derive(Clone)]
struct EBase;

#[derive(Clone)]
struct TenBase;

#[derive(Clone)]
struct TwoBase;

impl Base for EBase {
    fn base() -> F64 {
        OrderedFloat(E)
    }
}

impl Base for TenBase {
    fn base() -> F64 {
        OrderedFloat(10f64)
    }
}

impl Base for TwoBase {
    fn base() -> F64 {
        OrderedFloat(2f64)
    }
}
#[derive(Clone)]
struct GenericLogFunction<T> {
    t: PhantomData<T>,
}

impl<T: Base> GenericLogFunction<T> {
    fn log<S>(val: S) -> F64
    where S: AsPrimitive<F64> {
        val.as_().log(T::base())
    }

    fn log_with_base<S, B>(base: S, val: B) -> F64
    where
        S: AsPrimitive<F64>,
        B: AsPrimitive<F64>,
    {
        val.as_().log(base.as_())
    }
}

type LnFunction = GenericLogFunction<EBase>;
type LogFunction = GenericLogFunction<EBase>;
type Log10Function = GenericLogFunction<TenBase>;
type Log2Function = GenericLogFunction<TwoBase>;

fn factorial(n: i64) -> i64 {
    if n <= 0 { 1 } else { n * factorial(n - 1) }
}
