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

use std::f64::consts::PI;

use common_expression::types::NumberType;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::NumberDomain;
use common_expression::Value;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_1_arg::<NumberType<f64>, NumberType<f64>, _, _>(
        "sin",
        FunctionProperty::default(),
        |_| {
            Some(NumberDomain::<f64> {
                min: -1.0,
                max: 1.0,
            })
        },
        |f: f64| f.sin(),
    );

    registry.register_1_arg::<NumberType<f64>, NumberType<f64>, _, _>(
        "cos",
        FunctionProperty::default(),
        |_| {
            Some(NumberDomain::<f64> {
                min: -1.0,
                max: 1.0,
            })
        },
        |f: f64| f.cos(),
    );

    registry.register_1_arg::<NumberType<f64>, NumberType<f64>, _, _>(
        "tan",
        FunctionProperty::default(),
        |_| None,
        |f: f64| f.tan(),
    );

    registry.register_1_arg::<NumberType<f64>, NumberType<f64>, _, _>(
        "cot",
        FunctionProperty::default(),
        |_| None,
        |f: f64| 1.0f64 / f.tan(),
    );

    registry.register_1_arg::<NumberType<f64>, NumberType<f64>, _, _>(
        "acos",
        FunctionProperty::default(),
        |_| Some(NumberDomain::<f64> { min: 0.0, max: PI }),
        |f: f64| f.acos(),
    );

    registry.register_1_arg::<NumberType<f64>, NumberType<f64>, _, _>(
        "asin",
        FunctionProperty::default(),
        |_| {
            Some(NumberDomain::<f64> {
                min: 0.0,
                max: 2.0 * PI,
            })
        },
        |f: f64| f.asin(),
    );

    registry.register_1_arg::<NumberType<f64>, NumberType<f64>, _, _>(
        "atan",
        FunctionProperty::default(),
        |_| {
            Some(NumberDomain::<f64> {
                min: -PI / 2.0,
                max: PI / 2.0,
            })
        },
        |f: f64| f.atan(),
    );

    registry.register_2_arg::<NumberType<f64>, NumberType<f64>, NumberType<f64>, _, _>(
        "atan2",
        FunctionProperty::default(),
        |_, _| Some(NumberDomain::<f64> { min: -PI, max: PI }),
        |f: f64, r: f64| f.atan2(r),
    );

    registry.register_0_arg_core::<NumberType<f64>, _, _>(
        "pi",
        FunctionProperty::default(),
        || Some(NumberDomain::<f64> { min: PI, max: PI }),
        |_| Ok(Value::Scalar(PI)),
    );

    let sign = |val: f64| match val.partial_cmp(&0.0f64) {
        Some(std::cmp::Ordering::Greater) => 1,
        Some(std::cmp::Ordering::Less) => -1,
        _ => 0,
    };

    registry.register_1_arg::<NumberType<f64>, NumberType<i8>, _, _>(
        "sign",
        FunctionProperty::default(),
        move |domain| {
            Some(NumberDomain::<i8> {
                min: sign(domain.min),
                max: sign(domain.max),
            })
        },
        sign,
    );

    registry.register_1_arg::<NumberType<u64>, NumberType<u64>, _, _>(
        "abs",
        FunctionProperty::default(),
        |domain| Some(domain.clone()),
        |val| val,
    );

    registry.register_1_arg::<NumberType<i64>, NumberType<u64>, _, _>(
        "abs",
        FunctionProperty::default(),
        |domain| {
            let max = domain.min.unsigned_abs().max(domain.max.unsigned_abs());
            let mut min = domain.min.unsigned_abs().min(domain.max.unsigned_abs());

            if domain.max >= 0 && domain.min <= 0 {
                min = 0;
            }
            Some(NumberDomain::<u64> { min, max })
        },
        |val| val.unsigned_abs(),
    );

    registry.register_1_arg::<NumberType<f64>, NumberType<f64>, _, _>(
        "abs",
        FunctionProperty::default(),
        |domain| {
            let max = domain.min.abs().max(domain.max.abs());
            let mut min = domain.min.abs().min(domain.max.abs());

            if domain.max > 0.0 && domain.min <= 0.0 {
                min = 0.0;
            }
            Some(NumberDomain::<f64> { min, max })
        },
        |val| val.abs(),
    );
}
