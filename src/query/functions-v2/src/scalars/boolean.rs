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

use common_expression::types::boolean::BooleanDomain;
use common_expression::types::nullable::NullableDomain;
use common_expression::types::BooleanType;
use common_expression::types::NullableType;
use common_expression::vectorize_2_arg;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::Value;
use common_expression::ValueRef;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_1_arg::<BooleanType, BooleanType, _, _>(
        "not",
        FunctionProperty::default(),
        |arg| {
            Some(BooleanDomain {
                has_false: arg.has_true,
                has_true: arg.has_false,
            })
        },
        |val| !val,
    );

    // special function to combine the filter efficiently
    registry.register_passthrough_nullable_2_arg::<BooleanType, BooleanType, BooleanType, _, _>(
        "and_filters",
        FunctionProperty::default(),
        |lhs, rhs| {
            Some(BooleanDomain {
                has_false: lhs.has_false || rhs.has_false,
                has_true: lhs.has_true && rhs.has_true,
            })
        },
        |lhs, rhs, _| match (lhs, rhs) {
            (ValueRef::Scalar(flag), other) => {
                if flag {
                    Ok(other.to_owned())
                } else {
                    Ok(Value::Scalar(false))
                }
            }
            (other, ValueRef::Scalar(flag)) => {
                if flag {
                    Ok(other.to_owned())
                } else {
                    Ok(Value::Scalar(false))
                }
            }
            (ValueRef::Column(a), ValueRef::Column(b)) => Ok(Value::Column(&a & &b)),
        },
    );

    registry.register_2_arg_core::<BooleanType, BooleanType, BooleanType, _, _>(
        "and",
        FunctionProperty::default(),
        |lhs, rhs| {
            Some(BooleanDomain {
                has_false: lhs.has_false || rhs.has_false,
                has_true: lhs.has_true && rhs.has_true,
            })
        },
        |lhs, rhs, _| match (lhs, rhs) {
            (ValueRef::Scalar(flag), other) => {
                if flag {
                    Ok(other.to_owned())
                } else {
                    Ok(Value::Scalar(false))
                }
            }
            (other, ValueRef::Scalar(flag)) => {
                if flag {
                    Ok(other.to_owned())
                } else {
                    Ok(Value::Scalar(false))
                }
            }
            (ValueRef::Column(a), ValueRef::Column(b)) => Ok(Value::Column(&a & &b)),
        },
    );

    registry.register_2_arg_core::<BooleanType, BooleanType, BooleanType, _, _>(
        "or",
        FunctionProperty::default(),
        |lhs, rhs| {
            Some(BooleanDomain {
                has_false: lhs.has_false && rhs.has_false,
                has_true: lhs.has_true || rhs.has_true,
            })
        },
        |lhs, rhs, _| match (lhs, rhs) {
            (ValueRef::Scalar(flag), other) => {
                if flag {
                    Ok(Value::Scalar(true))
                } else {
                    Ok(other.to_owned())
                }
            }
            (other, ValueRef::Scalar(flag)) => {
                if flag {
                    Ok(Value::Scalar(true))
                } else {
                    Ok(other.to_owned())
                }
            }
            (ValueRef::Column(a), ValueRef::Column(b)) => Ok(Value::Column(&a | &b)),
        },
    );

    // https://en.wikibooks.org/wiki/Structured_Query_Language/NULLs_and_the_Three_Valued_Logic
    registry.register_2_arg_core::<NullableType<BooleanType>, NullableType<BooleanType>, NullableType<BooleanType>, _, _>(
        "and",
        FunctionProperty::default(),
        |lhs, rhs| {
            if !lhs.has_null && !rhs.has_null {
                let bools = match (&lhs.value, &rhs.value) {
                    (Some(a), Some(b)) => Some(Box::new(BooleanDomain {
                    has_false: a.has_false || b.has_false,
                    has_true: a.has_true && b.has_true,
                    })),
                    _ => return None,
                };
                return Some(NullableDomain::<BooleanType> {
                    has_null: false,
                    value: bools,
                });
            }
            None
        },
        vectorize_2_arg::<NullableType<BooleanType>, NullableType<BooleanType>, NullableType<BooleanType>>(|lhs, rhs| {
            let lhs_v = lhs.is_some();
            let rhs_v = rhs.is_some();
            let valid = (lhs_v & rhs_v) | (lhs == Some(true)) | (rhs == Some(true));
            if valid {
                Some(lhs.unwrap_or_default() & rhs.unwrap_or_default())
            } else {
                None
            }
        }),
    );

    registry.register_2_arg_core::<NullableType<BooleanType>, NullableType<BooleanType>, NullableType<BooleanType>, _, _>(
        "or",
        FunctionProperty::default(),
        |lhs, rhs| {
            if !lhs.has_null && !rhs.has_null {
                let bools = match (&lhs.value, &rhs.value) {
                    (Some(a), Some(b)) => Some(Box::new(BooleanDomain {
                        has_false: a.has_false && b.has_false,
                        has_true: a.has_true || b.has_true,
                    })),
                    _ => return None,
                };
                return Some(NullableDomain::<BooleanType> {
                    has_null: false,
                    value: bools,
                });
            }
            None
        },
        vectorize_2_arg::<NullableType<BooleanType>, NullableType<BooleanType>, NullableType<BooleanType>>(|lhs, rhs| {
            let valid = (lhs.is_some() & rhs.is_some()) | lhs.unwrap_or_default() | rhs.unwrap_or_default();
            if valid {
                Some(lhs.unwrap_or_default() | rhs.unwrap_or_default())
            } else {
                None
            }
        }),
    );

    registry.register_2_arg::<BooleanType, BooleanType, BooleanType, _, _>(
        "xor",
        FunctionProperty::default(),
        |lhs, rhs| {
            Some(BooleanDomain {
                has_false: (lhs.has_false && rhs.has_false) || (lhs.has_true && rhs.has_true),
                has_true: (lhs.has_false && rhs.has_true) || (lhs.has_true && rhs.has_false),
            })
        },
        |lhs, rhs| lhs ^ rhs,
    );
}
