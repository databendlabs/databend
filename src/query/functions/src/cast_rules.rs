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

use databend_common_expression::FunctionRegistry;
use databend_common_expression::auto_cast_rules::cast_from_number_rules;
use databend_common_expression::auto_cast_rules::cast_from_string_rules;
use databend_common_expression::auto_cast_rules::cast_from_variant_rules;
use databend_common_expression::auto_cast_rules::cast_int_to_uint64_rules;
use databend_common_expression::auto_cast_rules::general_cast_rules;
use databend_common_expression::types::ALL_INTEGER_TYPES;
use databend_common_expression::types::DataType;

use crate::scalars::ALL_COMP_FUNC_NAMES;
use crate::scalars::ALL_STRING_FUNC_NAMES;
use crate::scalars::PURE_STRING_FUNC_NAMES;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_default_cast_rules(general_cast_rules());
    registry.register_default_cast_rules(cast_from_string_rules());
    registry.register_default_cast_rules(cast_from_variant_rules());

    // Time arithmetic returns a Timestamp even for Date inputs. Prefer parsing strings as
    // Timestamp so that a time component is not silently discarded by the Date overload.
    let time_arith_cast_rules = registry
        .default_cast_rules()
        .iter()
        .filter(|(src, dest)| !matches!((src, dest), (DataType::String, DataType::Date)))
        .cloned()
        .collect::<Vec<_>>();
    for func_name in [
        "add_hours",
        "add_minutes",
        "add_seconds",
        "subtract_hours",
        "subtract_minutes",
        "subtract_seconds",
    ] {
        registry.register_additional_cast_rules(func_name, time_arith_cast_rules.iter().cloned());
    }

    for func_name in ["and", "or", "not", "xor", "and_filters", "or_filters"] {
        registry.register_additional_cast_rules(
            func_name,
            ALL_INTEGER_TYPES
                .iter()
                .map(|data_type| (DataType::Number(*data_type), DataType::Boolean)),
        );
        registry.register_additional_cast_rules(func_name, general_cast_rules());
        registry.register_additional_cast_rules(func_name, cast_from_string_rules());
        registry.register_additional_cast_rules(func_name, cast_from_variant_rules());
    }

    for func_name in ALL_STRING_FUNC_NAMES {
        registry.register_additional_cast_rules(func_name, general_cast_rules());
        if PURE_STRING_FUNC_NAMES.contains(func_name) {
            registry.register_dynamic_cast_rules(
                func_name,
                std::sync::Arc::new(|arg_type, result_type| {
                    let is_nullable_or_null = arg_type.is_nullable_or_null();
                    let result_is_nullable_or_null = result_type.is_nullable_or_null();

                    (!is_nullable_or_null && matches!(result_type, DataType::String))
                        || (is_nullable_or_null
                            && result_is_nullable_or_null
                            && matches!(result_type.remove_nullable(), DataType::String))
                }),
            );
        } else {
            registry.register_additional_cast_rules(func_name, cast_from_string_rules());
        }
        registry.register_additional_cast_rules(func_name, cast_from_variant_rules());
        registry.register_additional_cast_rules(func_name, cast_int_to_uint64_rules());
    }

    for func_name in ["slice", "get"] {
        registry.register_additional_cast_rules(func_name, general_cast_rules());
        registry.register_additional_cast_rules(func_name, cast_from_string_rules());
        registry.register_additional_cast_rules(func_name, cast_int_to_uint64_rules());
    }

    for func_name in ALL_COMP_FUNC_NAMES {
        registry.register_additional_cast_rules(func_name, general_cast_rules());
        registry.register_additional_cast_rules(func_name, cast_from_variant_rules());
        registry.register_additional_cast_rules(func_name, cast_from_number_rules());
    }
    registry.register_additional_cast_rules("contains", general_cast_rules());
    registry.register_additional_cast_rules("contains", cast_from_variant_rules());
}
