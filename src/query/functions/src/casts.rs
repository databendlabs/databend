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

//! The casts that the functions of this crate implement.
//!
//! A cast step converts a value with the function registered here, and its domain is the domain
//! of that function. A cast without a function of its own is registered as a chain. Casts that
//! need the internals of a sub-crate are registered there.

use databend_common_expression::FunctionRegistry;
use databend_common_expression::auto_cast_rules::general_cast_rules;
use databend_common_expression::types::ALL_INTEGER_TYPES;
use databend_common_expression::types::ALL_NUMERICS_TYPES;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;

pub fn register(registry: &mut FunctionRegistry) {
    register_conversion_functions(registry);
    register_number_casts(registry);
    register_boolean_casts(registry);
    register_string_casts(registry);
    register_variant_casts(registry);
    register_binary_casts(registry);
    databend_functions_scalar_decimal::register_decimal_casts(registry);
    databend_functions_scalar_datetime::datetime::register_casts(registry);
    register_cast_chains(registry);
    register_conversion_auto_cast_rules(registry);
}

/// A conversion function takes an argument of another type through the general rules only, not by
/// parsing a string or extracting a Variant. A cast registered as a chain converts through its
/// intermediate type, and so does the conversion function: `to_int8(date)` checks as
/// `to_int8(CAST(date AS Int64))`, as `CAST(date AS Int8)` runs.
fn register_conversion_auto_cast_rules(registry: &mut FunctionRegistry) {
    let function_names = registry
        .conversion_functions
        .values()
        .cloned()
        .chain(["parse_json".to_string()])
        .collect::<Vec<_>>();
    for function_name in &function_names {
        registry.register_additional_cast_rules(function_name, general_cast_rules());
    }

    let chains = registry
        .cast_chains
        .iter()
        .map(|((src, dest), via)| (src.clone(), via.clone(), dest.clone()))
        .collect::<Vec<_>>();
    for (src, via, dest) in chains {
        // A decimal of any size casts to Float64 by `decimal_auto_cast`.
        if matches!(src, DataType::Decimal(_)) {
            continue;
        }
        let function_name = registry
            .get_conversion_function(&dest)
            .unwrap_or_else(|| panic!("no conversion function to {dest}"))
            .to_string();
        registry.register_additional_cast_rules(&function_name, [(src, via)]);
    }
}

/// `to_x` converts values to X, and a cast to X runs it unless registered with another function.
/// `parse_json` is not one: it reparses a Variant.
fn register_conversion_functions(registry: &mut FunctionRegistry) {
    for (data_type, function_name) in [
        (DataType::Boolean, "to_boolean"),
        (DataType::String, "to_string"),
        (DataType::Binary, "to_binary"),
        (DataType::Decimal(Default::default()), "to_decimal"),
        (DataType::Date, "to_date"),
        (DataType::Timestamp, "to_timestamp"),
        (DataType::TimestampTz, "to_timestamp_tz"),
        (DataType::Interval, "to_interval"),
        (DataType::Variant, "to_variant"),
        (DataType::Bitmap, "to_bitmap"),
        (DataType::Geometry, "to_geometry"),
        (DataType::Geography, "to_geography"),
    ] {
        registry.register_conversion_function(data_type, function_name);
    }
    for num in ALL_NUMERICS_TYPES {
        let function_name = format!("to_{}", num.to_string().to_lowercase());
        registry.register_conversion_function(DataType::Number(*num), &function_name);
    }
}

fn register_cast(registry: &mut FunctionRegistry, src: DataType, dest: DataType) {
    registry.register_cast(Some(src), dest);
}

/// Numbers convert with the number functions: a value out of the range of the destination
/// fails, and floats are rounded when the context asks for it.
fn register_number_casts(registry: &mut FunctionRegistry) {
    for src in ALL_NUMERICS_TYPES {
        for dest in ALL_NUMERICS_TYPES {
            if src != dest {
                register_cast(registry, DataType::Number(*src), DataType::Number(*dest));
            }
        }
    }
}

/// Booleans convert to 0 and 1, and numbers to whether they are not 0.
fn register_boolean_casts(registry: &mut FunctionRegistry) {
    for num in ALL_NUMERICS_TYPES {
        register_cast(registry, DataType::Boolean, DataType::Number(*num));
        register_cast(registry, DataType::Number(*num), DataType::Boolean);
    }
}

/// Every type converts to a string, and a string parses to the types it can represent.
fn register_string_casts(registry: &mut FunctionRegistry) {
    let mut to_string = vec![
        DataType::Boolean,
        DataType::Decimal(Default::default()),
        DataType::Date,
        DataType::Timestamp,
        DataType::TimestampTz,
        DataType::Interval,
        DataType::Bitmap,
        DataType::Geometry,
        DataType::Geography,
    ];
    to_string.extend(ALL_NUMERICS_TYPES.iter().map(|num| DataType::Number(*num)));
    for src in to_string {
        register_cast(registry, src, DataType::String);
    }

    let mut from_string = vec![
        DataType::Boolean,
        DataType::Interval,
        DataType::Bitmap,
        DataType::Geometry,
        DataType::Geography,
        DataType::Binary,
    ];
    from_string.extend(ALL_NUMERICS_TYPES.iter().map(|num| DataType::Number(*num)));
    for dest in from_string {
        register_cast(registry, DataType::String, dest);
    }
    // A string converts to Variant as JSON text, not as a JSON string.
    registry.register_cast_with(Some(DataType::String), DataType::Variant, "parse_json");
    register_cast(
        registry,
        DataType::Number(NumberDataType::UInt64),
        DataType::Bitmap,
    );
}

/// Every type converts to Variant. A Variant converts to a NULL for a JSON null, and fails on a
/// value of another JSON type, except for a string, which every value converts to. A JSON null
/// is not a geometry, so it fails like other values.
fn register_variant_casts(registry: &mut FunctionRegistry) {
    registry.register_cast(None, DataType::Variant);

    let mut dests = vec![
        DataType::Boolean,
        DataType::Binary,
        DataType::Decimal(Default::default()),
        DataType::Date,
        DataType::Timestamp,
        DataType::TimestampTz,
        DataType::Interval,
        DataType::String,
        DataType::Geometry,
        DataType::Geography,
    ];
    dests.extend(ALL_NUMERICS_TYPES.iter().map(|num| DataType::Number(*num)));
    for dest in dests {
        register_cast(registry, DataType::Variant, dest);
    }
}

/// Binary values of other types convert to their bytes; bytes parse as a geometry, and as a
/// string when they are valid UTF-8.
fn register_binary_casts(registry: &mut FunctionRegistry) {
    for src in [DataType::Bitmap, DataType::Geometry, DataType::Geography] {
        register_cast(registry, src, DataType::Binary);
    }
    for dest in [DataType::String, DataType::Geometry, DataType::Geography] {
        register_cast(registry, DataType::Binary, dest);
    }
}

/// Casts without a function of their own convert through an intermediate type. An error in
/// either step belongs to its row, so `TRY_CAST` turns it into NULL.
fn register_cast_chains(registry: &mut FunctionRegistry) {
    let int64 = DataType::Number(NumberDataType::Int64);
    let uint64 = DataType::Number(NumberDataType::UInt64);

    // Known issue: these include casts that fail on almost every value, e.g. a timestamp, in
    // microseconds, to Int8, Int16 or UInt8, and lose precision to Float32. They type check as
    // they did before, so they are kept.
    for src in [DataType::Date, DataType::Timestamp] {
        for dest in ALL_NUMERICS_TYPES {
            if *dest != NumberDataType::Int64 {
                registry.register_cast_chain(src.clone(), int64.clone(), DataType::Number(*dest));
            }
        }
    }
    for src in ALL_INTEGER_TYPES {
        if *src != NumberDataType::Int64 {
            for dest in [DataType::Date, DataType::Timestamp] {
                registry.register_cast_chain(DataType::Number(*src), int64.clone(), dest);
            }
        }
    }
    for src in [
        NumberDataType::UInt8,
        NumberDataType::UInt16,
        NumberDataType::UInt32,
    ] {
        registry.register_cast_chain(DataType::Number(src), uint64.clone(), DataType::Bitmap);
    }
    // A decimal is true when it is not zero, which converting to Float64 keeps.
    registry.register_cast_chain(
        DataType::Decimal(Default::default()),
        DataType::Number(NumberDataType::Float64),
        DataType::Boolean,
    );
}
