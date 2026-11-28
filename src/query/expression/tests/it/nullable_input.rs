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

use std::sync::Arc;

use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionFactory;
use databend_common_expression::FunctionID;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::NullableInput;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_expression::domain_evaluator;
use databend_common_expression::scalar_evaluator;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;

fn builtin(name: &str, id: usize) -> FunctionID {
    FunctionID::Builtin {
        name: name.to_string(),
        id,
    }
}

/// A factory of `pick`, returning its argument, for arguments that are
/// nullable or not as `nullable` says.
fn pick_factory(nullable: bool) -> FunctionFactory {
    FunctionFactory::Closure(Box::new(move |_, args_type: &[DataType]| {
        let [arg] = args_type else {
            return None;
        };
        (arg.is_nullable() == nullable).then(|| {
            Arc::new(Function {
                signature: FunctionSignature {
                    name: "pick".to_string(),
                    args_type: args_type.to_vec(),
                    return_type: arg.clone(),
                },
                eval: FunctionEval::Scalar {
                    calc_domain: domain_evaluator(|_, _| FunctionDomain::Full),
                    eval: scalar_evaluator(|_, _| Value::Scalar(Scalar::Null)),
                    derive_stat: None,
                },
            })
        })
    }))
}

fn registry() -> FunctionRegistry {
    let mut registry = FunctionRegistry::empty();
    registry.register_passthrough_nullable_2_arg::<Int32Type, Int32Type, Int32Type, _, _>(
        "plus32",
        |_, _, _| FunctionDomain::Full,
        |lhs, _, _| lhs,
    );
    registry.register_1_arg::<Int32Type, Int32Type, _>(
        "neg32",
        |_, _| FunctionDomain::Full,
        |value, _| value,
    );
    registry.register_combine_nullable_1_arg::<Int32Type, StringType, _, _>(
        "try_str",
        |_, _| FunctionDomain::MayThrow,
        |_, _| Value::Scalar(None),
    );
    registry.register_1_arg_core::<Int32Type, Int32Type, _, _>(
        "strict32",
        |_, _| FunctionDomain::Full,
        |value, _| value,
    );
    registry.register_nullable_variants(
        |registry| {
            registry.register_1_arg_core::<BooleanType, BooleanType, _, _>(
                "truthy",
                |_, _| FunctionDomain::Full,
                |value, _| value.to_owned(),
            );
            registry.register_function_factory("pick", pick_factory(false));
        },
        |registry| {
            registry.register_1_arg_core::<NullableType<BooleanType>, BooleanType, _, _>(
                "truthy",
                |_, _| FunctionDomain::Full,
                |_, _| Value::Scalar(false),
            );
            registry.register_function_factory("pick", pick_factory(true));
        },
    );
    registry.register_passthrough_nullable_factory("same", |_, args_type: &[DataType]| {
        let [arg] = args_type else {
            return None;
        };
        let arg = arg.remove_nullable();
        Some(Function {
            signature: FunctionSignature {
                name: "same".to_string(),
                args_type: vec![arg.clone()],
                return_type: arg,
            },
            eval: FunctionEval::Scalar {
                calc_domain: domain_evaluator(|_, _| FunctionDomain::Full),
                eval: scalar_evaluator(|args, _| args[0].clone()),
                derive_stat: None,
            },
        })
    });
    registry
}

#[test]
fn registration_records_how_overloads_take_nullable_arguments() {
    let registry = registry();
    assert_eq!(
        registry.nullable_input(&builtin("plus32", 0)),
        Some(&NullableInput::PassthroughVariant { variant: 1 })
    );
    // The nullable variant takes nullable arguments by its signature.
    assert_eq!(registry.nullable_input(&builtin("plus32", 1)), None);
    // A builder with `passthrough_nullable`.
    assert_eq!(
        registry.nullable_input(&builtin("neg32", 0)),
        Some(&NullableInput::PassthroughVariant { variant: 1 })
    );
    assert_eq!(
        registry.nullable_input(&builtin("try_str", 0)),
        Some(&NullableInput::CombineVariant { variant: 1 })
    );
    assert_eq!(registry.nullable_input(&builtin("strict32", 0)), None);
    assert_eq!(
        registry.nullable_input(&builtin("truthy", 0)),
        Some(&NullableInput::NullAwareVariant { variant: 1 })
    );
    assert_eq!(
        registry.nullable_input(&builtin("pick", 0)),
        Some(&NullableInput::NullAwareFactories { factories: vec![1] })
    );
    assert_eq!(
        registry.nullable_input(&builtin("same", 0)),
        Some(&NullableInput::PassthroughFactory { factory: 1 })
    );
}

/// The passthrough factory creates the passthrough nullable function of the
/// function of its factory.
#[test]
fn passthrough_factory_wraps_its_function() {
    let registry = registry();
    let int32 = DataType::Number(NumberDataType::Int32);
    let (_, function) = registry
        .create_factory_function("same", 1, &[], &[int32.wrap_nullable()])
        .unwrap();
    assert_eq!(function.signature.args_type, vec![int32.wrap_nullable()]);
    assert_eq!(function.signature.return_type, int32.wrap_nullable());
}

/// A nullable variant is the one whose arguments are those of its overload
/// made nullable, whatever order they are registered in.
#[test]
fn nullable_variants_pair_by_arguments() {
    let mut registry = FunctionRegistry::empty();
    registry.register_nullable_variants(
        |registry| {
            registry.register_1_arg_core::<BooleanType, BooleanType, _, _>(
                "flip",
                |_, _| FunctionDomain::Full,
                |value, _| value.to_owned(),
            );
            registry.register_1_arg_core::<Int32Type, BooleanType, _, _>(
                "flip",
                |_, _| FunctionDomain::Full,
                |_, _| Value::Scalar(false),
            );
        },
        |registry| {
            registry.register_1_arg_core::<NullableType<Int32Type>, BooleanType, _, _>(
                "flip",
                |_, _| FunctionDomain::Full,
                |_, _| Value::Scalar(false),
            );
            registry.register_1_arg_core::<NullableType<BooleanType>, BooleanType, _, _>(
                "flip",
                |_, _| FunctionDomain::Full,
                |_, _| Value::Scalar(false),
            );
        },
    );
    assert_eq!(
        registry.nullable_input(&builtin("flip", 0)),
        Some(&NullableInput::NullAwareVariant { variant: 3 })
    );
    assert_eq!(
        registry.nullable_input(&builtin("flip", 1)),
        Some(&NullableInput::NullAwareVariant { variant: 2 })
    );
}

/// A nullable variant takes the arguments of its overload, made nullable.
#[test]
#[should_panic(expected = "overload 0 of `flip` has no nullable variant")]
fn nullable_variant_needs_the_arguments_of_its_overload() {
    let mut registry = FunctionRegistry::empty();
    registry.register_nullable_variants(
        |registry| {
            registry.register_1_arg_core::<BooleanType, BooleanType, _, _>(
                "flip",
                |_, _| FunctionDomain::Full,
                |value, _| value.to_owned(),
            );
        },
        |registry| {
            registry.register_1_arg_core::<NullableType<Int32Type>, BooleanType, _, _>(
                "flip",
                |_, _| FunctionDomain::Full,
                |_, _| Value::Scalar(false),
            );
        },
    );
}
