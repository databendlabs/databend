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

const MAX_ARGS: usize = 5;

use std::fmt::Write as _;
use std::fs::File;
use std::io::Write;
use std::process::Command;

use itertools::Itertools;

pub fn codegen_register() {
    let mut source = String::new();

    // Write imports.
    writeln!(
        source,
        "
            #![allow(unused_parens)]
            #![allow(unused_variables)]
            #![allow(clippy::redundant_closure)]
            use crate::FunctionEval;
            use crate::Function;
            use crate::EvalContext;
            use crate::FunctionContext;
            use crate::FunctionDomain;
            use crate::FunctionRegistry;
            use crate::FunctionSignature;
            use crate::property::Domain;
            use crate::types::nullable::NullableColumn;
            use crate::types::nullable::NullableDomain;
            use crate::types::*;
            use crate::values::Value;
            use crate::values::ValueRef;
        "
    )
    .unwrap();

    // Write `impl FunctionRegistry`.
    writeln!(source, "impl FunctionRegistry {{").unwrap();

    // Write `register_x_arg`.
    for n_args in 1..=MAX_ARGS {
        let arg_generics_bound = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("I{n}: ArgType, "))
            .join("");
        let arg_f_closure_sig = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("&I{n}::Domain, "))
            .join("");
        let arg_g_closure_sig = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("I{n}::ScalarRef<'_>, "))
            .join("");
        let arg_generics = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("I{n}, "))
            .join("");
        writeln!(
            source,
            "
                pub fn register_{n_args}_arg<{arg_generics_bound} O: ArgType, F, G>(
                    &mut self,
                    name: &str,
                    calc_domain: F,
                    func: G,
                ) where
                    F: Fn(&FunctionContext, {arg_f_closure_sig}) -> FunctionDomain<O> + 'static + Clone + Copy + Send + Sync,
                    G: Fn({arg_g_closure_sig} &mut EvalContext) -> O::Scalar + 'static + Clone + Copy + Send + Sync,
                {{
                    self.register_passthrough_nullable_{n_args}_arg::<{arg_generics} O, _, _>(
                        name,
                        calc_domain,
                        vectorize_{n_args}_arg(func),
                    )
                }}
            "
        )
        .unwrap();
    }

    // Write `register_passthrough_nullable_x_arg`.
    for n_args in 1..=MAX_ARGS {
        let arg_generics_bound = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("I{n}: ArgType, "))
            .join("");
        let arg_f_closure_sig = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("&I{n}::Domain, "))
            .join("");
        let arg_g_closure_sig = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("ValueRef<'a, I{n}>, "))
            .join("");
        let arg_sig_type = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("I{n}::data_type(), "))
            .join("");
        let arg_generics = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("I{n}, "))
            .join("");
        let arg_nullable_generics = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("NullableType<I{n}>, "))
            .join("");
        let closure_args = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("arg{n},"))
            .join("");
        let closure_args_value = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("&arg{n}.value"))
            .join(",");
        let some_values = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("Some(value{n})"))
            .join(",");
        let values = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("value{n},"))
            .join("");
        let any_arg_has_null = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("arg{n}.has_null"))
            .join("||");

        writeln!(
            source,
            "
                pub fn register_passthrough_nullable_{n_args}_arg<{arg_generics_bound} O: ArgType, F, G>(
                    &mut self,
                    name: &str,
                    calc_domain: F,
                    func: G,
                ) where
                    F: Fn(&FunctionContext, {arg_f_closure_sig}) -> FunctionDomain<O> + 'static + Clone + Copy + Send + Sync,
                    G: for<'a> Fn({arg_g_closure_sig} &mut EvalContext) -> Value<O> + 'static + Clone + Copy + Send + Sync,
                {{
                    let has_nullable = &[{arg_sig_type} O::data_type()]
                        .iter()
                        .any(|ty| ty.as_nullable().is_some() || ty.is_null());

                    assert!(
                        !has_nullable,
                        \"Function {{}} has nullable argument or output, please use register_{n_args}_arg_core instead\",
                        name
                    );

                    self.register_{n_args}_arg_core::<{arg_generics} O, _, _>(name, calc_domain, func);

                    self.register_{n_args}_arg_core::<{arg_nullable_generics} NullableType<O>, _, _>(
                        name,
                        move |ctx, {closure_args}| {{
                            match ({closure_args_value}) {{
                                ({some_values}) => {{
                                    if let Some(domain) = calc_domain(ctx, {values}).normalize() {{
                                        FunctionDomain::Domain(NullableDomain {{
                                            has_null: {any_arg_has_null},
                                            value: Some(Box::new(domain)),
                                        }})
                                    }} else {{
                                        FunctionDomain::MayThrow
                                    }}
                                }},
                                _ => {{
                                    FunctionDomain::Domain(NullableDomain {{
                                        has_null: true,
                                        value: None,
                                    }})
                                }},
                            }}
                        }},
                        passthrough_nullable_{n_args}_arg(func),
                    );
                }}
            "
        )
        .unwrap();
    }

    // Write `register_combine_nullable_x_arg`.
    for n_args in 1..=MAX_ARGS {
        let arg_generics_bound = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("I{n}: ArgType, "))
            .join("");
        let arg_f_closure_sig = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("&I{n}::Domain, "))
            .join("");
        let arg_g_closure_sig = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("ValueRef<'a, I{n}>, "))
            .join("");
        let arg_sig_type = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("I{n}::data_type(), "))
            .join("");
        let arg_generics = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("I{n}, "))
            .join("");
        let arg_nullable_generics = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("NullableType<I{n}>, "))
            .join("");
        let closure_args = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("arg{n},"))
            .join("");
        let closure_args_value = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("&arg{n}.value"))
            .join(",");
        let some_values = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("Some(value{n})"))
            .join(",");
        let values = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("value{n},"))
            .join("");
        let any_arg_has_null = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("arg{n}.has_null"))
            .join("||");

        writeln!(
            source,
            "
                pub fn register_combine_nullable_{n_args}_arg<{arg_generics_bound} O: ArgType, F, G>(
                    &mut self,
                    name: &str,
                    calc_domain: F,
                    func: G,
                ) where
                    F: Fn(&FunctionContext, {arg_f_closure_sig}) -> FunctionDomain<NullableType<O>> + 'static + Clone + Copy + Send + Sync,
                    G: for<'a> Fn({arg_g_closure_sig} &mut EvalContext) -> Value<NullableType<O>> + 'static + Clone + Copy + Send + Sync,
                {{
                    let has_nullable = &[{arg_sig_type} O::data_type()]
                        .iter()
                        .any(|ty| ty.as_nullable().is_some() || ty.is_null());

                    assert!(
                        !has_nullable,
                        \"Function {{}} has nullable argument or output, please use register_{n_args}_arg_core instead\",
                        name
                    );

                    self.register_{n_args}_arg_core::<{arg_generics} NullableType<O>, _, _>(
                        name,
                        calc_domain,
                        func
                    );

                    self.register_{n_args}_arg_core::<{arg_nullable_generics} NullableType<O>, _, _>(
                        name,
                        move |ctx, {closure_args}| {{
                            match ({closure_args_value}) {{
                                ({some_values}) => {{
                                    if let Some(domain) = calc_domain(ctx, {values}).normalize() {{
                                        FunctionDomain::Domain(NullableDomain {{
                                            has_null: {any_arg_has_null} || domain.has_null,
                                            value: domain.value,
                                        }})
                                    }} else {{
                                        FunctionDomain::MayThrow
                                    }}
                                }}
                                _ => {{
                                    FunctionDomain::Domain(NullableDomain {{
                                        has_null: true,
                                        value: None,
                                    }})
                                }},
                            }}
                        }},
                        combine_nullable_{n_args}_arg(func),
                    );
                }}
            "
        )
        .unwrap();
    }

    // Write `register_x_arg_core`.
    for n_args in 0..=MAX_ARGS {
        let arg_generics_bound = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("I{n}: ArgType, "))
            .join("");
        let arg_f_closure_sig = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("&I{n}::Domain, "))
            .join("");
        let arg_g_closure_sig = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("ValueRef<'a, I{n}>, "))
            .join("");
        let arg_sig_type = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("I{n}::data_type()"))
            .join(", ");
        let arg_generics = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("I{n}, "))
            .join("");
        writeln!(
            source,
            "
                pub fn register_{n_args}_arg_core<{arg_generics_bound} O: ArgType, F, G>(
                    &mut self,
                    name: &str,
                    calc_domain: F,
                    func: G,
                ) where
                    F: Fn(&FunctionContext, {arg_f_closure_sig}) -> FunctionDomain<O> + 'static + Clone + Copy + Send + Sync,
                    G: for <'a> Fn({arg_g_closure_sig} &mut EvalContext) -> Value<O> + 'static + Clone + Copy + Send + Sync,
                {{
                    let func = Function {{
                        signature: FunctionSignature {{
                            name: name.to_string(),
                            args_type: vec![{arg_sig_type}],
                            return_type: O::data_type(),
                        }},
                        eval: FunctionEval::Scalar {{
                            calc_domain: Box::new(erase_calc_domain_generic_{n_args}_arg::<{arg_generics} O>(calc_domain)),
                            eval: Box::new(erase_function_generic_{n_args}_arg(func)),
                        }},
                    }};
                    self.register_function(func);
                }}
            "
        )
        .unwrap();
    }
    writeln!(source, "}}").unwrap();

    // Write `vectorize_x_arg`.
    for n_args in 1..=MAX_ARGS {
        let arg_generics_bound = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("I{n}: ArgType, "))
            .join("");
        let arg_input_closure_sig = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("I{n}::ScalarRef<'_>, "))
            .join("");
        let arg_output_closure_sig = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("ValueRef<I{n}>, "))
            .join("");
        let func_args = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("arg{n}, "))
            .join("");
        let args_tuple = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("arg{n}"))
            .join(", ");
        let arg_scalar = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("ValueRef::Scalar(arg{n})"))
            .join(", ");
        let match_arms = (1..(1 << n_args))
            .map(|idx| {
                let columns = (0..n_args)
                    .filter(|n| idx & (1 << n) != 0)
                    .collect::<Vec<_>>();
                let arm_pat = (0..n_args)
                    .map(|n| {
                        if columns.contains(&n) {
                            format!("ValueRef::Column(arg{})", n + 1)
                        } else {
                            format!("ValueRef::Scalar(arg{})", n + 1)
                        }
                    })
                    .join(", ");
                let arg_iter = (0..n_args)
                    .filter(|n| columns.contains(n))
                    .map(|n| n + 1)
                    .map(|n| format!("let arg{n}_iter = I{n}::iter_column(&arg{n});"))
                    .join("");
                let zipped_iter = columns
                    .iter()
                    .map(|n| format!("arg{}_iter", n + 1))
                    .reduce(|acc, item| format!("{acc}.zip({item})"))
                    .unwrap();
                let col_arg = columns
                    .iter()
                    .map(|n| format!("arg{}", n + 1))
                    .reduce(|acc, item| format!("({acc}, {item})"))
                    .unwrap();
                let func_arg = (0..n_args)
                    .map(|n| {
                        if columns.contains(&n) {
                            format!("arg{}, ", n + 1)
                        } else {
                            format!("arg{}.clone(), ", n + 1)
                        }
                    })
                    .join("");

                format!(
                    "({arm_pat}) => {{
                        let generics = &(ctx.generics.to_owned());
                        {arg_iter}
                        let iter = {zipped_iter}.map(|{col_arg}| func({func_arg} ctx));
                        let col = O::column_from_iter(iter, generics);
                        Value::Column(col)
                    }}"
                )
            })
            .join("");
        writeln!(
            source,
            "
                pub fn vectorize_{n_args}_arg<{arg_generics_bound} O: ArgType>(
                    func: impl Fn({arg_input_closure_sig} &mut EvalContext) -> O::Scalar + Copy + Send + Sync,
                ) -> impl Fn({arg_output_closure_sig} &mut EvalContext) -> Value<O> + Copy + Send + Sync {{
                    move |{func_args} ctx| match ({args_tuple}) {{
                        ({arg_scalar}) => Value::Scalar(func({func_args} ctx)),
                        {match_arms}
                    }}
                }}
            "
        )
        .unwrap();
    }

    // Write `vectorize_with_builder_x_arg`.
    for n_args in 1..=MAX_ARGS {
        let arg_generics_bound = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("I{n}: ArgType, "))
            .join("");
        let arg_input_closure_sig = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("I{n}::ScalarRef<'_>, "))
            .join("");
        let arg_output_closure_sig = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("ValueRef<I{n}>, "))
            .join("");
        let func_args = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("arg{n}, "))
            .join("");
        let args_tuple = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("arg{n}"))
            .join(", ");
        let arg_scalar = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("ValueRef::Scalar(arg{n})"))
            .join(", ");
        let match_arms = (1..(1 << n_args))
            .map(|idx| {
                let columns = (0..n_args)
                    .filter(|n| idx & (1 << n) != 0)
                    .collect::<Vec<_>>();
                let arm_pat = (0..n_args)
                    .map(|n| {
                        if columns.contains(&n) {
                            format!("ValueRef::Column(arg{})", n + 1)
                        } else {
                            format!("ValueRef::Scalar(arg{})", n + 1)
                        }
                    })
                    .join(", ");
                let arg_iter = (0..n_args)
                    .filter(|n| columns.contains(n))
                    .map(|n| n + 1)
                    .map(|n| format!("let arg{n}_iter = I{n}::iter_column(&arg{n});"))
                    .join("");
                let zipped_iter = columns
                    .iter()
                    .map(|n| format!("arg{}_iter", n + 1))
                    .reduce(|acc, item| format!("{acc}.zip({item})"))
                    .unwrap();
                let col_arg = columns
                    .iter()
                    .map(|n| format!("arg{}", n + 1))
                    .reduce(|acc, item| format!("({acc}, {item})"))
                    .unwrap();
                let func_arg = (0..n_args)
                    .map(|n| {
                        if columns.contains(&n) {
                            format!("arg{}, ", n + 1)
                        } else {
                            format!("arg{}.clone(), ", n + 1)
                        }
                    })
                    .join("");

                format!(
                    "({arm_pat}) => {{
                        let generics = &(ctx.generics.to_owned());
                        {arg_iter}
                        let iter = {zipped_iter};
                        let mut builder = O::create_builder(iter.size_hint().0, generics);
                        for {col_arg} in iter {{
                            func({func_arg} &mut builder, ctx);
                        }}
                        Value::Column(O::build_column(builder))
                    }}"
                )
            })
            .join("");
        writeln!(
            source,
            "
                pub fn vectorize_with_builder_{n_args}_arg<{arg_generics_bound} O: ArgType>(
                    func: impl Fn({arg_input_closure_sig} &mut O::ColumnBuilder, &mut EvalContext) + Copy + Send + Sync,
                ) -> impl Fn({arg_output_closure_sig} &mut EvalContext) -> Value<O> + Copy + Send + Sync {{
                    move |{func_args} ctx| match ({args_tuple}) {{
                        ({arg_scalar}) => {{
                            let generics = &(ctx.generics.to_owned());
                            let mut builder = O::create_builder(1, generics);
                            func({func_args} &mut builder, ctx);
                            Value::Scalar(O::build_scalar(builder))
                        }}
                        {match_arms}
                    }}
                }}
            "
        )
        .unwrap();
    }

    // Write `passthrough_nullable_x_arg`.
    for n_args in 1..=MAX_ARGS {
        let arg_generics_bound = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("I{n}: ArgType, "))
            .join("");
        let arg_input_closure_sig = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("ValueRef<'a, I{n}>, "))
            .join("");
        let arg_output_closure_sig = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("ValueRef<'a, NullableType<I{n}>>, "))
            .join("");
        let closure_args = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("arg{n}, "))
            .join("");
        let args_tuple = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("arg{n}"))
            .join(", ");
        let arg_scalar = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("ValueRef::Scalar(Some(arg{n}))"))
            .join(", ");
        let scalar_func_args = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("ValueRef::Scalar(arg{n}), "))
            .join("");
        let scalar_nones_pats = (0..n_args)
            .map(|n| {
                let pat = (0..n_args)
                    .map(|nth| {
                        if nth == n {
                            "ValueRef::Scalar(None)"
                        } else {
                            "_"
                        }
                    })
                    .join(",");
                format!("({pat})")
            })
            .reduce(|acc, item| format!("{acc} | {item}"))
            .unwrap();
        let match_arms = (1..(1 << n_args))
            .map(|idx| {
                let columns = (0..n_args)
                    .filter(|n| idx & (1 << n) != 0)
                    .collect::<Vec<_>>();
                let arm_pat = (0..n_args)
                    .map(|n| {
                        if columns.contains(&n) {
                            format!("ValueRef::Column(arg{})", n + 1)
                        } else {
                            format!("ValueRef::Scalar(Some(arg{}))", n + 1)
                        }
                    })
                    .join(", ");
                let and_validity = columns
                    .iter()
                    .map(|n| format!("arg{}.validity", n + 1))
                    .reduce(|acc, item| {
                        format!("databend_common_arrow::arrow::bitmap::and(&{acc}, &{item})")
                    })
                    .unwrap();
                let func_arg = (0..n_args)
                    .map(|n| {
                        if columns.contains(&n) {
                            format!("ValueRef::Column(arg{}.column), ", n + 1)
                        } else {
                            format!("ValueRef::Scalar(arg{}), ", n + 1)
                        }
                    })
                    .join("");

                format!(
                    "({arm_pat}) => {{
                        let and_validity = {and_validity};
                        let validity = ctx.validity.as_ref().map(|valid| valid & (&and_validity)).unwrap_or(and_validity);
                        ctx.validity = Some(validity.clone());
                        let column = func({func_arg} ctx).into_column().unwrap();
                        Value::Column(NullableColumn::new(column, validity))
                    }}"
                )
            })
            .join("");
        writeln!(
            source,
            "
                pub fn passthrough_nullable_{n_args}_arg<{arg_generics_bound} O: ArgType>(
                    func: impl for <'a> Fn({arg_input_closure_sig} &mut EvalContext) -> Value<O> + Copy + Send + Sync,
                ) -> impl for <'a> Fn({arg_output_closure_sig} &mut EvalContext) -> Value<NullableType<O>> + Copy + Send + Sync {{
                    move |{closure_args} ctx| match ({args_tuple}) {{
                        {scalar_nones_pats} => Value::Scalar(None),
                        ({arg_scalar}) => Value::Scalar(Some(
                            func({scalar_func_args} ctx)
                                .into_scalar()
                                .unwrap(),
                        )),
                        {match_arms}
                    }}
                }}
            "
        )
        .unwrap();
    }

    // Write `combine_nullable_x_arg`.
    for n_args in 1..=MAX_ARGS {
        let arg_generics_bound = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("I{n}: ArgType, "))
            .join("");
        let arg_input_closure_sig = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("ValueRef<'a, I{n}>, "))
            .join("");
        let arg_output_closure_sig = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("ValueRef<'a, NullableType<I{n}>>, "))
            .join("");
        let closure_args = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("arg{n}, "))
            .join("");
        let args_tuple = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("arg{n}"))
            .join(", ");
        let arg_scalar = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("ValueRef::Scalar(Some(arg{n}))"))
            .join(", ");
        let scalar_func_args = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("ValueRef::Scalar(arg{n}), "))
            .join("");
        let scalar_nones_pats = (0..n_args)
            .map(|n| {
                let pat = (0..n_args)
                    .map(|nth| {
                        if nth == n {
                            "ValueRef::Scalar(None)"
                        } else {
                            "_"
                        }
                    })
                    .join(",");
                format!("({pat})")
            })
            .reduce(|acc, item| format!("{acc} | {item}"))
            .unwrap();
        let match_arms = (1..(1 << n_args))
            .map(|idx| {
                let columns = (0..n_args)
                    .filter(|n| idx & (1 << n) != 0)
                    .collect::<Vec<_>>();
                let arm_pat = (0..n_args)
                    .map(|n| {
                        if columns.contains(&n) {
                            format!("ValueRef::Column(arg{})", n + 1)
                        } else {
                            format!("ValueRef::Scalar(Some(arg{}))", n + 1)
                        }
                    })
                    .join(", ");
                let and_validity = columns
                    .iter()
                    .map(|n| format!("arg{}.validity", n + 1))
                    .reduce(|acc, item| {
                        format!("databend_common_arrow::arrow::bitmap::and(&{acc}, &{item})")
                    })
                    .unwrap();
                let func_arg = (0..n_args)
                    .map(|n| {
                        if columns.contains(&n) {
                            format!("ValueRef::Column(arg{}.column), ", n + 1)
                        } else {
                            format!("ValueRef::Scalar(arg{}), ", n + 1)
                        }
                    })
                    .join("");

                format!(
                    "({arm_pat}) => {{
                        let and_validity = {and_validity};
                        let validity = ctx.validity.as_ref().map(|valid| valid & (&and_validity)).unwrap_or(and_validity);
                        ctx.validity = Some(validity.clone());
                        let nullable_column = func({func_arg} ctx).into_column().unwrap();
                        let combine_validity = databend_common_arrow::arrow::bitmap::and(&validity, &nullable_column.validity);
                        Value::Column(NullableColumn::new(nullable_column.column, combine_validity))
                    }}"
                )
            })
            .join("");
        writeln!(
            source,
            "
                pub fn combine_nullable_{n_args}_arg<{arg_generics_bound} O: ArgType>(
                    func: impl for <'a> Fn({arg_input_closure_sig} &mut EvalContext) -> Value<NullableType<O>> + Copy + Send + Sync,
                ) -> impl for <'a> Fn({arg_output_closure_sig} &mut EvalContext) -> Value<NullableType<O>> + Copy + Send + Sync {{
                    move |{closure_args} ctx| match ({args_tuple}) {{
                        {scalar_nones_pats} => Value::Scalar(None),
                        ({arg_scalar}) => Value::Scalar(
                            func({scalar_func_args} ctx)
                                .into_scalar()
                                .unwrap(),
                        ),
                        {match_arms}
                    }}
                }}
            "
        )
        .unwrap();
    }

    // Write `erase_calc_domain_generic_x_arg`.
    for n_args in 0..=MAX_ARGS {
        let arg_generics_bound = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("I{n}: ArgType, "))
            .join("");
        let arg_f_closure_sig = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("&I{n}::Domain, "))
            .join("");
        let let_args = (0..n_args)
            .map(|n| n + 1)
            .map(|n| {
                format!(
                    "let arg{n} = I{n}::try_downcast_domain(&args[{}]).unwrap();",
                    n - 1
                )
            })
            .join("");
        let func_args = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("&arg{n},"))
            .join("");
        writeln!(
            source,
            "
                fn erase_calc_domain_generic_{n_args}_arg<{arg_generics_bound} O: ArgType>(
                    func: impl Fn(&FunctionContext, {arg_f_closure_sig}) -> FunctionDomain<O>,
                ) -> impl Fn(&FunctionContext, &[Domain]) -> FunctionDomain<AnyType> {{
                    move |ctx, args| {{
                        {let_args}
                        func(ctx, {func_args}).map(O::upcast_domain)
                    }}
                }}
            "
        )
        .unwrap();
    }

    // Write `erase_function_generic_x_arg`.
    for n_args in 0..=MAX_ARGS {
        let arg_generics_bound = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("I{n}: ArgType, "))
            .join("");
        let arg_g_closure_sig = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("ValueRef<'a, I{n}>, "))
            .join("");
        let let_args = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("let arg{n} = args[{}].try_downcast().unwrap();", n - 1))
            .join("");
        let func_args = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("arg{n}, "))
            .join("");
        writeln!(
            source,
            "
                fn erase_function_generic_{n_args}_arg<{arg_generics_bound} O: ArgType>(
                    func: impl for <'a> Fn({arg_g_closure_sig} &mut EvalContext) -> Value<O>,
                ) -> impl Fn(&[ValueRef<AnyType>], &mut EvalContext) -> Value<AnyType> {{
                    move |args, ctx| {{
                        {let_args}
                        Value::upcast(func({func_args} ctx))
                    }}
                }}
            "
        )
        .unwrap();
    }

    format_and_save("src/query/expression/src/register.rs", &source);
}

fn format_and_save(path: &str, src: &str) {
    let mut file = File::create(path).expect("open");

    // Write the head.
    let codegen_src_path = file!();
    writeln!(
        file,
        "// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the \"License\");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an \"AS IS\" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This code is generated by {codegen_src_path}. DO NOT EDIT.
"
    )
    .unwrap();

    writeln!(file, "{src}").unwrap();

    file.flush().unwrap();

    Command::new("cargo")
        .arg("fmt")
        .arg("--")
        .arg(path)
        .status()
        .unwrap()
        .exit_ok()
        .unwrap();
}
