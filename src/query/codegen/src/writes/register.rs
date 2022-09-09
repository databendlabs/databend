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

            use std::sync::Arc;
            
            use crate::Function;
            use crate::FunctionRegistry;
            use crate::FunctionSignature;
            use crate::property::Domain;
            use crate::property::FunctionProperty;
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
                    name: &'static str,
                    property: FunctionProperty,
                    calc_domain: F,
                    func: G,
                ) where
                    F: Fn({arg_f_closure_sig}) -> Option<O::Domain> + 'static + Clone + Copy,
                    G: Fn({arg_g_closure_sig}) -> O::Scalar + 'static + Clone + Copy,
                {{
                    self.register_passthrough_nullable_{n_args}_arg::<{arg_generics} O, _, _>(
                        name,
                        property,
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
            .map(|n| format!("ValueRef<I{n}>, "))
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
            .map(|n| format!("arg{n}"))
            .join(",");
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
            .map(|n| format!("value{n}"))
            .join(",");
        let any_arg_has_null = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("arg{n}.has_null"))
            .join("||");

        let null_overloads = (0..n_args)
            .map(|n| {
                let null_types = (0..n_args)
                    .map(|nth| {
                        if nth == n {
                            "NullType,".to_string()
                        } else {
                            format!("NullableType<I{}>,", nth + 1)
                        }
                    })
                    .join("");
                let n_widecards = "_,".repeat(n_args);
                format!(
                    "
                        self.register_{n_args}_arg_core::<{null_types} NullType, _, _>(
                            name,
                            property.clone(),
                            |{n_widecards}| Some(()),
                            |{n_widecards} _| Ok(Value::Scalar(())),
                        );
                    "
                )
            })
            .join("");

        writeln!(
            source,
            "
                pub fn register_passthrough_nullable_{n_args}_arg<{arg_generics_bound} O: ArgType, F, G>(
                    &mut self,
                    name: &'static str,
                    property: FunctionProperty,
                    calc_domain: F,
                    func: G,
                ) where
                    F: Fn({arg_f_closure_sig}) -> Option<O::Domain> + 'static + Clone + Copy,
                    G: Fn({arg_g_closure_sig} &GenericMap) -> Result<Value<O>, String> + 'static + Clone + Copy,
                {{
                    let has_nullable = &[{arg_sig_type} O::data_type()]
                        .iter()
                        .any(|ty| ty.as_nullable().is_some() || ty.is_null());

                    assert!(
                        !has_nullable,
                        \"Function {{}} has nullable argument or output, please use register_{n_args}_arg_core instead\",
                        name
                    );

                    {null_overloads}

                    self.register_{n_args}_arg_core::<{arg_generics} O, _, _>(name, property.clone(), calc_domain, func);

                    self.register_{n_args}_arg_core::<{arg_nullable_generics} NullableType<O>, _, _>(
                        name,
                        property,
                        move |{closure_args}| {{
                            let value = match ({closure_args_value}) {{
                                ({some_values}) => Some(calc_domain({values})?),
                                _ => None,
                            }};
                            Some(NullableDomain {{
                                has_null: {any_arg_has_null},
                                value: value.map(Box::new),
                            }})
                        }},
                        passthrough_nullable_{n_args}_arg(func),
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
            .map(|n| format!("ValueRef<I{n}>, "))
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
                    name: &'static str,
                    property: FunctionProperty,
                    calc_domain: F,
                    func: G,
                ) where
                    F: Fn({arg_f_closure_sig}) -> Option<O::Domain> + 'static + Clone + Copy,
                    G: Fn({arg_g_closure_sig} &GenericMap) -> Result<Value<O>, String> + 'static + Clone + Copy,
                {{
                    self.funcs
                        .entry(name)
                        .or_insert_with(Vec::new)
                        .push(Arc::new(Function {{
                            signature: FunctionSignature {{
                                name,
                                args_type: vec![{arg_sig_type}],
                                return_type: O::data_type(),
                                property,
                            }},
                            calc_domain: Box::new(erase_calc_domain_generic_{n_args}_arg::<{arg_generics} O>(calc_domain)),
                            eval: Box::new(erase_function_generic_{n_args}_arg(func)),
                        }}));
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
                            format!("arg{}", n + 1)
                        } else {
                            format!("arg{}.clone()", n + 1)
                        }
                    })
                    .join(", ");

                format!(
                    "({arm_pat}) => {{
                        {arg_iter}
                        let iter = {zipped_iter}.map(|{col_arg}| func({func_arg}));
                        let col = O::column_from_iter(iter, generics);
                        Ok(Value::Column(col))
                    }}"
                )
            })
            .join("");
        writeln!(
            source,
            "
                pub fn vectorize_{n_args}_arg<{arg_generics_bound} O: ArgType>(
                    func: impl Fn({arg_input_closure_sig}) -> O::Scalar + Copy,
                ) -> impl Fn({arg_output_closure_sig} &GenericMap) -> Result<Value<O>, String> + Copy {{
                    move |{func_args} generics| match ({args_tuple}) {{
                        ({arg_scalar}) => Ok(Value::Scalar(func({func_args}))),
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
                        {arg_iter}
                        let iter = {zipped_iter};
                        let mut builder = O::create_builder(iter.size_hint().0, generics);
                        for {col_arg} in iter {{
                            func({func_arg} &mut builder)?;
                        }}
                        Ok(Value::Column(O::build_column(builder)))
                    }}"
                )
            })
            .join("");
        writeln!(
            source,
            "
                pub fn vectorize_with_builder_{n_args}_arg<{arg_generics_bound} O: ArgType>(
                    func: impl Fn({arg_input_closure_sig} &mut O::ColumnBuilder) -> Result<(), String> + Copy,
                ) -> impl Fn({arg_output_closure_sig} &GenericMap) -> Result<Value<O>, String> + Copy {{
                    move |{func_args} generics| match ({args_tuple}) {{
                        ({arg_scalar}) => {{
                            let mut builder = O::create_builder(1, generics);
                            func({func_args} &mut builder)?;
                            Ok(Value::Scalar(O::build_scalar(builder)))
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
            .map(|n| format!("ValueRef<I{n}>, "))
            .join("");
        let arg_output_closure_sig = (0..n_args)
            .map(|n| n + 1)
            .map(|n| format!("ValueRef<NullableType<I{n}>>, "))
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
                        format!("common_arrow::arrow::bitmap::and(&{acc}, &{item})")
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
                        let column = func({func_arg} generics)?.into_column().unwrap();
                        let validity = {and_validity};
                        Ok(Value::Column(NullableColumn {{ column, validity }}))
                    }}"
                )
            })
            .join("");
        writeln!(
            source,
            "
                pub fn passthrough_nullable_{n_args}_arg<{arg_generics_bound} O: ArgType>(
                    func: impl Fn({arg_input_closure_sig} &GenericMap) -> Result<Value<O>, String> + Copy,
                ) -> impl Fn({arg_output_closure_sig} &GenericMap) -> Result<Value<NullableType<O>>, String> + Copy {{
                    move |{closure_args} generics| match ({args_tuple}) {{
                        {scalar_nones_pats} => Ok(Value::Scalar(None)),
                        ({arg_scalar}) => Ok(Value::Scalar(Some(
                            func({scalar_func_args} generics)?
                                .into_scalar()
                                .unwrap(),
                        ))),
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
            .map(|n| format!("&arg{n}"))
            .join(",");
        writeln!(
            source,
            "
                fn erase_calc_domain_generic_{n_args}_arg<{arg_generics_bound} O: ArgType>(
                    func: impl Fn({arg_f_closure_sig}) -> Option<O::Domain>,
                ) -> impl Fn(&[Domain], &GenericMap) -> Option<Domain> {{
                    move |args, _generics| {{
                        {let_args}
                        func({func_args}).map(O::upcast_domain)
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
            .map(|n| format!("ValueRef<I{n}>, "))
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
                    func: impl Fn({arg_g_closure_sig} &GenericMap) -> Result<Value<O>, String>,
                ) -> impl Fn(&[ValueRef<AnyType>], &GenericMap) -> Result<Value<AnyType>, String> {{
                    move |args, generics| {{
                        {let_args}
                        func({func_args} generics).map(Value::upcast)
                    }}
                }}
            "
        )
        .unwrap();
    }

    format_and_save("src/query/expression/src/register.rs", &source);
}

fn format_and_save(path: &str, src: &str) {
    let mut file = File::create(&path).expect("open");

    // Write the head.
    let codegen_src_path = file!();
    writeln!(
        file,
        "// Copyright 2021 Datafuse Labs.
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
