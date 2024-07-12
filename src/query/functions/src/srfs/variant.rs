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

use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::types::nullable::NullableColumnBuilder;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::VariantType;
use databend_common_expression::Column;
use databend_common_expression::FromData;
use databend_common_expression::Function;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionKind;
use databend_common_expression::FunctionProperty;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;
use databend_common_expression::ValueRef;
use jaq_core;
use jaq_interpret::Ctx;
use jaq_interpret::FilterT;
use jaq_interpret::ParseCtx;
use jaq_interpret::RcIter;
use jaq_interpret::Val;
use jaq_parse;
use jaq_std;
use jsonb::array_length;
use jsonb::array_values;
use jsonb::as_str;
use jsonb::get_by_index;
use jsonb::get_by_name;
use jsonb::jsonpath::parse_json_path;
use jsonb::jsonpath::Mode as SelectorMode;
use jsonb::jsonpath::Selector;
use jsonb::object_each;
use jsonb::object_keys;
use jsonb::to_serde_json;

pub fn register(registry: &mut FunctionRegistry) {
    registry.properties.insert(
        "json_path_query".to_string(),
        FunctionProperty::default().kind(FunctionKind::SRF),
    );

    registry.register_function_factory("json_path_query", |_, args_type| {
        if args_type.len() != 2 {
            return None;
        }
        if (args_type[0].remove_nullable() != DataType::Variant && args_type[0] != DataType::Null)
            || (args_type[1].remove_nullable() != DataType::String
                && args_type[1] != DataType::Null)
        {
            return None;
        }

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "json_path_query".to_string(),
                args_type: args_type.to_vec(),
                return_type: DataType::Tuple(vec![DataType::Nullable(Box::new(DataType::Variant))]),
            },

            eval: FunctionEval::SRF {
                eval: Box::new(|args, ctx, max_nums_per_row| {
                    let val_arg = args[0].clone().to_owned();
                    let path_arg = args[1].clone().to_owned();
                    let mut results = Vec::with_capacity(ctx.num_rows);

                    match path_arg {
                        Value::Scalar(Scalar::String(path)) => {
                            match parse_json_path(path.as_bytes()) {
                                Ok(json_path) => {
                                    let selector = Selector::new(json_path, SelectorMode::All);
                                    for (row, max_nums_per_row) in
                                        max_nums_per_row.iter_mut().enumerate().take(ctx.num_rows)
                                    {
                                        let val = unsafe { val_arg.index_unchecked(row) };
                                        let mut builder = BinaryColumnBuilder::with_capacity(0, 0);
                                        if let ScalarRef::Variant(val) = val {
                                            if selector
                                                .select(
                                                    val,
                                                    &mut builder.data,
                                                    &mut builder.offsets,
                                                )
                                                .is_err()
                                            {
                                                ctx.set_error(
                                                    0,
                                                    format!(
                                                        "Invalid JSONB value '0x{}'",
                                                        hex::encode(val)
                                                    ),
                                                );
                                                break;
                                            }
                                        }
                                        let array =
                                            Column::Variant(builder.build()).wrap_nullable(None);
                                        let array_len = array.len();
                                        *max_nums_per_row =
                                            std::cmp::max(*max_nums_per_row, array_len);
                                        results.push((
                                            Value::Column(Column::Tuple(vec![array])),
                                            array_len,
                                        ));
                                    }
                                }
                                Err(_) => {
                                    ctx.set_error(0, format!("Invalid JSON Path '{}'", &path,));
                                }
                            }
                        }
                        _ => {
                            for (row, max_nums_per_row) in
                                max_nums_per_row.iter_mut().enumerate().take(ctx.num_rows)
                            {
                                let val = unsafe { val_arg.index_unchecked(row) };
                                let path = unsafe { path_arg.index_unchecked(row) };
                                let mut builder = BinaryColumnBuilder::with_capacity(0, 0);
                                if let ScalarRef::String(path) = path {
                                    match parse_json_path(path.as_bytes()) {
                                        Ok(json_path) => {
                                            if let ScalarRef::Variant(val) = val {
                                                let selector =
                                                    Selector::new(json_path, SelectorMode::All);
                                                if selector
                                                    .select(
                                                        val,
                                                        &mut builder.data,
                                                        &mut builder.offsets,
                                                    )
                                                    .is_err()
                                                {
                                                    ctx.set_error(
                                                        0,
                                                        format!(
                                                            "Invalid JSONB value '0x{}'",
                                                            hex::encode(val)
                                                        ),
                                                    );
                                                    break;
                                                }
                                            }
                                        }
                                        Err(_) => {
                                            ctx.set_error(
                                                row,
                                                format!("Invalid JSON Path '{}'", &path,),
                                            );
                                            break;
                                        }
                                    }
                                }
                                let array = Column::Variant(builder.build()).wrap_nullable(None);
                                let array_len = array.len();
                                *max_nums_per_row = std::cmp::max(*max_nums_per_row, array_len);
                                results
                                    .push((Value::Column(Column::Tuple(vec![array])), array_len));
                            }
                        }
                    }
                    results
                }),
            },
        }))
    });

    registry.properties.insert(
        "json_array_elements".to_string(),
        FunctionProperty::default().kind(FunctionKind::SRF),
    );
    registry.register_function_factory("json_array_elements", |_, args_type| {
        if args_type.len() != 1 {
            return None;
        }
        if args_type[0].remove_nullable() != DataType::Variant && args_type[0] != DataType::Null {
            return None;
        }
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "json_array_elements".to_string(),
                args_type: args_type.to_vec(),
                return_type: DataType::Tuple(vec![DataType::Nullable(Box::new(DataType::Variant))]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(|args, ctx, max_nums_per_row| {
                    let arg = args[0].clone().to_owned();
                    (0..ctx.num_rows)
                        .map(|row| match arg.index(row).unwrap() {
                            ScalarRef::Null => {
                                (Value::Scalar(Scalar::Tuple(vec![Scalar::Null])), 0)
                            }
                            ScalarRef::Variant(val) => {
                                unnest_variant_array(val, row, max_nums_per_row)
                            }
                            _ => unreachable!(),
                        })
                        .collect()
                }),
            },
        }))
    });

    registry.properties.insert(
        "json_each".to_string(),
        FunctionProperty::default().kind(FunctionKind::SRF),
    );
    registry.register_function_factory("json_each", |_, args_type| {
        if args_type.len() != 1 {
            return None;
        }
        if args_type[0].remove_nullable() != DataType::Variant && args_type[0] != DataType::Null {
            return None;
        }
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "json_each".to_string(),
                args_type: args_type.to_vec(),
                return_type: DataType::Tuple(vec![
                    DataType::Nullable(Box::new(DataType::String)),
                    DataType::Nullable(Box::new(DataType::Variant)),
                ]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(|args, ctx, max_nums_per_row| {
                    let arg = args[0].clone().to_owned();
                    (0..ctx.num_rows)
                        .map(|row| match arg.index(row).unwrap() {
                            ScalarRef::Null => (
                                Value::Scalar(Scalar::Tuple(vec![Scalar::Null, Scalar::Null])),
                                0,
                            ),
                            ScalarRef::Variant(val) => {
                                unnest_variant_obj(val, row, max_nums_per_row)
                            }
                            _ => unreachable!(),
                        })
                        .collect()
                }),
            },
        }))
    });

    registry.properties.insert(
        "flatten".to_string(),
        FunctionProperty::default().kind(FunctionKind::SRF),
    );
    registry.register_function_factory("flatten", |params, args_type| {
        if args_type.is_empty() || args_type.len() > 5 {
            return None;
        }
        if args_type[0].remove_nullable() != DataType::Variant && args_type[0] != DataType::Null {
            return None;
        }
        if args_type.len() >= 2
            && args_type[1] != DataType::String
            && args_type[1] != DataType::Null
        {
            return None;
        }
        if args_type.len() >= 3
            && args_type[2] != DataType::Boolean
            && args_type[2] != DataType::Null
        {
            return None;
        }
        if args_type.len() >= 4
            && args_type[3] != DataType::Boolean
            && args_type[3] != DataType::Null
        {
            return None;
        }
        if args_type.len() >= 5
            && args_type[4] != DataType::String
            && args_type[4] != DataType::Null
        {
            return None;
        }
        let params: Vec<i64> = params.iter().map(|x| x.get_i64().unwrap()).collect();

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "flatten".to_string(),
                args_type: args_type.to_vec(),
                return_type: DataType::Tuple(vec![
                    DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64))),
                    DataType::Nullable(Box::new(DataType::String)),
                    DataType::Nullable(Box::new(DataType::String)),
                    DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64))),
                    DataType::Nullable(Box::new(DataType::Variant)),
                    DataType::Nullable(Box::new(DataType::Variant)),
                ]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(move |args, ctx, max_nums_per_row| {
                    let arg = args[0].clone().to_owned();

                    let mut json_path = None;
                    let mut outer = false;
                    let mut recursive = false;
                    let mut mode = FlattenMode::Both;
                    let mut results = Vec::with_capacity(ctx.num_rows);

                    if args.len() >= 2 {
                        match &args[1] {
                            ValueRef::Scalar(ScalarRef::String(v)) => {
                                match parse_json_path(v.as_bytes()) {
                                    Ok(jsonpath) => {
                                        let selector = Selector::new(jsonpath, SelectorMode::First);
                                        json_path = Some((v, selector));
                                    }
                                    Err(_) => {
                                        ctx.set_error(0, format!("Invalid JSON Path {v:?}",));
                                        return results;
                                    }
                                }
                            }
                            ValueRef::Column(_) => {
                                ctx.set_error(
                                    0,
                                    "argument `path` to function FLATTEN needs to be constant"
                                        .to_string(),
                                );
                                return results;
                            }
                            _ => {}
                        }
                    }
                    if args.len() >= 3 {
                        match &args[2] {
                            ValueRef::Scalar(ScalarRef::Boolean(v)) => {
                                outer = *v;
                            }
                            ValueRef::Column(_) => {
                                ctx.set_error(
                                    0,
                                    "argument `outer` to function FLATTEN needs to be constant"
                                        .to_string(),
                                );
                                return results;
                            }
                            _ => {}
                        }
                    }
                    if args.len() >= 4 {
                        match &args[3] {
                            ValueRef::Scalar(ScalarRef::Boolean(v)) => {
                                recursive = *v;
                            }
                            ValueRef::Column(_) => {
                                ctx.set_error(
                                    0,
                                    "argument `recursive` to function FLATTEN needs to be constant"
                                        .to_string(),
                                );
                                return results;
                            }
                            _ => {}
                        }
                    }
                    if args.len() >= 5 {
                        match args[4] {
                            ValueRef::Scalar(ScalarRef::String(v)) => {
                                match v.to_lowercase().as_str() {
                                    "object" => {
                                        mode = FlattenMode::Object;
                                    }
                                    "array" => {
                                        mode = FlattenMode::Array;
                                    }
                                    "both" => {
                                        mode = FlattenMode::Both;
                                    }
                                    _ => {
                                        ctx.set_error(0, format!("Invalid mode {v:?}"));
                                        return results;
                                    }
                                }
                            }
                            ValueRef::Column(_) => {
                                ctx.set_error(
                                    0,
                                    "argument `mode` to function FLATTEN needs to be constant"
                                        .to_string(),
                                );
                                return results;
                            }
                            _ => {}
                        }
                    }
                    let mut generator = FlattenGenerator::create(outer, recursive, mode);

                    for (row, max_nums_per_row) in
                        max_nums_per_row.iter_mut().enumerate().take(ctx.num_rows)
                    {
                        match arg.index(row).unwrap() {
                            ScalarRef::Null => {
                                results.push((
                                    Value::Scalar(Scalar::Tuple(vec![
                                        Scalar::Null,
                                        Scalar::Null,
                                        Scalar::Null,
                                        Scalar::Null,
                                        Scalar::Null,
                                        Scalar::Null,
                                    ])),
                                    0,
                                ));
                            }
                            ScalarRef::Variant(val) => {
                                let columns = match json_path {
                                    Some((path, ref selector)) => {
                                        // get inner input values by path
                                        let mut builder = BinaryColumnBuilder::with_capacity(0, 0);
                                        if selector
                                            .select(val, &mut builder.data, &mut builder.offsets)
                                            .is_err()
                                        {
                                            ctx.set_error(
                                                0,
                                                format!(
                                                    "Invalid JSONB value '0x{}'",
                                                    hex::encode(val)
                                                ),
                                            );
                                            break;
                                        }
                                        let inner_val = builder.pop().unwrap_or_default();
                                        generator.generate(
                                            (row + 1) as u64,
                                            &inner_val,
                                            path,
                                            &params,
                                        )
                                    }
                                    None => generator.generate((row + 1) as u64, val, "", &params),
                                };
                                let len = columns[0].len();
                                *max_nums_per_row = std::cmp::max(*max_nums_per_row, len);

                                results.push((Value::Column(Column::Tuple(columns)), len));
                            }
                            _ => unreachable!(),
                        }
                    }
                    results
                }),
            },
        }))
    });

    registry.properties.insert(
        "jq".to_string(),
        FunctionProperty::default().kind(FunctionKind::SRF),
    );
    registry.register_function_factory("jq", |_, args_type| {
        if args_type.len() != 2 {
            return None;
        }
        if args_type[0].remove_nullable() != DataType::String {
            return None;
        }
        if args_type[1].remove_nullable() != DataType::Variant && args_type[1] != DataType::Null {
            return None;
        }

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "jq".to_string(),
                args_type: args_type.to_vec(),
                return_type: DataType::Tuple(vec![DataType::Nullable(Box::new(DataType::Variant))]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(|args, ctx, max_nums_per_row| {
                    let jq_filter_col = args[0].clone().to_owned();
                    let jq_filter = match jq_filter_col.index(0) {
                        Some(ScalarRef::String(s)) => s,
                        _ => {
                            ctx.set_error(0, "jq filter must be a scalar string");
                            return vec![];
                        }
                    };

                    let mut defs = ParseCtx::new(vec![]);
                    defs.insert_natives(jaq_core::core());
                    defs.insert_defs(jaq_std::std());
                    assert!(defs.errs.is_empty());
                    let (filter, errs) = jaq_parse::parse(jq_filter, jaq_parse::main());
                    if !errs.is_empty() {
                        ctx.set_error(0, errs[0].to_string());
                        return vec![];
                    }

                    let filter = defs.compile(filter.unwrap());
                    if !defs.errs.is_empty() {
                        let err_str = defs
                            .errs
                            .iter()
                            .map(|e| format!("err: {} location: {:?}", e.0, e.1))
                            .collect::<Vec<_>>()
                            .join("\n");
                        ctx.set_error(0, err_str);
                        return vec![];
                    }

                    let jaq_args = vec![];
                    // You can pass additional scalar inputs as args to the jq filter.
                    // This could be a useful enhancement, but leaving it out for mow.
                    let inputs = RcIter::new(core::iter::empty());
                    let jaq_ctx = Ctx::new(jaq_args, &inputs);

                    let json_arg = args[1].clone().to_owned();
                    (0..ctx.num_rows)
                        .map(|row| {
                            // with an SRF each row returns a Value::Column or Value::Scalar of results
                            // so it's sort of a pivot or an array of arrays, where each row returns
                            // a column representing multiple results for that row.
                            // if a row returns null, you return a null vec in a Value::Scalar.
                            let mut res_builder = BinaryColumnBuilder::with_capacity(0, 1);
                            let null_result = (Value::Scalar(Scalar::Tuple(vec![Scalar::Null])), 0);

                            match json_arg.index(row) {
                                Some(ScalarRef::Null) => {
                                    return null_result;
                                }
                                Some(ScalarRef::Variant(v)) => {
                                    let s = to_serde_json(v);
                                    match s {
                                        Err(e) => {
                                            ctx.set_error(row, e.to_string());
                                            return null_result;
                                        }
                                        Ok(s) => {
                                            let jaq_val = Val::from(s);
                                            let jaq_out = filter.run((jaq_ctx.clone(), jaq_val));

                                            for res in jaq_out {
                                                match res {
                                                    Err(err) => {
                                                        ctx.set_error(row, err.to_string());
                                                        return null_result;
                                                    }
                                                    Ok(res) => {
                                                        let res_json_str = jaq_val_to_json(&res);
                                                        res_builder.put_str(&res_json_str);
                                                        res_builder.commit_row();
                                                    }
                                                };
                                            }
                                        }
                                    }
                                }
                                None => {
                                    return null_result;
                                }
                                _ => unreachable!(),
                            }

                            let res_col = Column::Variant(res_builder.build()).wrap_nullable(None);
                            let res_len = res_col.len();
                            max_nums_per_row[row] = std::cmp::max(max_nums_per_row[row], res_len);
                            (Value::Column(Column::Tuple(vec![res_col])), res_len)
                        })
                        .collect()
                }),
            },
        }))
    });
}

// This comes straight from the jaq source. It converts a Jaq val type to a JSON string.
// https://github.com/01mf02/jaq/blob/426fdab46c95e7ed0dadc5c049b3d83388271b1a/jaq/src/main.rs#L511
// there may be an opportunity to format directly into
// jsonb data so it doesn't need to be re-parsed.
fn jaq_val_to_json(val: &Val) -> String {
    match val {
        Val::Null => "null".to_string(),
        Val::Bool(b) => b.to_string(),
        Val::Num(n) => n.to_string(),
        Val::Float(f) if f.is_finite() => f.to_string(),
        Val::Float(_) => "null".to_string(),
        Val::Int(i) => i.to_string(),
        Val::Str(s) => format!("\"{}\"", s),
        Val::Arr(a) => {
            let mut res = "[".to_string();
            for (i, v) in a.iter().enumerate() {
                if i > 0 {
                    res.push_str(", ");
                }
                res.push_str(&jaq_val_to_json(v));
            }
            res.push(']');
            res
        }
        Val::Obj(o) => {
            let mut res = "{".to_string();
            for (i, (k, v)) in o.iter().enumerate() {
                if i > 0 {
                    res.push_str(", ");
                }
                res.push_str(&format!("\"{}\": {}", k, jaq_val_to_json(v)));
            }
            res.push('}');
            res
        }
    }
}

pub(crate) fn unnest_variant_array(
    val: &[u8],
    row: usize,
    max_nums_per_row: &mut [usize],
) -> (Value<AnyType>, usize) {
    match array_values(val) {
        Some(vals) if !vals.is_empty() => {
            let len = vals.len();
            let mut builder = BinaryColumnBuilder::with_capacity(0, 0);

            max_nums_per_row[row] = std::cmp::max(max_nums_per_row[row], len);

            for val in vals {
                builder.put_slice(&val);
                builder.commit_row();
            }

            let col = Column::Variant(builder.build()).wrap_nullable(None);
            (Value::Column(Column::Tuple(vec![col])), len)
        }
        _ => (Value::Scalar(Scalar::Tuple(vec![Scalar::Null])), 0),
    }
}

fn unnest_variant_obj(
    val: &[u8],
    row: usize,
    max_nums_per_row: &mut [usize],
) -> (Value<AnyType>, usize) {
    match object_each(val) {
        Some(vals) if !vals.is_empty() => {
            let len = vals.len();
            let mut val_builder = BinaryColumnBuilder::with_capacity(0, 0);
            let mut key_builder = StringColumnBuilder::with_capacity(0, 0);

            max_nums_per_row[row] = std::cmp::max(max_nums_per_row[row], len);

            for (key, val) in vals {
                key_builder.put_str(&String::from_utf8_lossy(&key));
                key_builder.commit_row();
                val_builder.put_slice(&val);
                val_builder.commit_row();
            }

            let key_col = Column::String(key_builder.build()).wrap_nullable(None);
            let val_col = Column::Variant(val_builder.build()).wrap_nullable(None);

            (Value::Column(Column::Tuple(vec![key_col, val_col])), len)
        }
        _ => (
            Value::Scalar(Scalar::Tuple(vec![Scalar::Null, Scalar::Null])),
            0,
        ),
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum FlattenMode {
    Both,
    Object,
    Array,
}

#[derive(Copy, Clone)]
struct FlattenGenerator {
    outer: bool,
    recursive: bool,
    mode: FlattenMode,
}

impl FlattenGenerator {
    fn create(outer: bool, recursive: bool, mode: FlattenMode) -> FlattenGenerator {
        Self {
            outer,
            recursive,
            mode,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn flatten(
        &mut self,
        input: &[u8],
        path: &str,
        key_builder: &mut Option<NullableColumnBuilder<StringType>>,
        path_builder: &mut Option<StringColumnBuilder>,
        index_builder: &mut Option<NullableColumnBuilder<UInt64Type>>,
        value_builder: &mut Option<BinaryColumnBuilder>,
        this_builder: &mut Option<BinaryColumnBuilder>,
        rows: &mut usize,
    ) {
        match self.mode {
            FlattenMode::Object => {
                self.flatten_object(
                    input,
                    path,
                    key_builder,
                    path_builder,
                    index_builder,
                    value_builder,
                    this_builder,
                    rows,
                );
            }
            FlattenMode::Array => {
                self.flatten_array(
                    input,
                    path,
                    key_builder,
                    path_builder,
                    index_builder,
                    value_builder,
                    this_builder,
                    rows,
                );
            }
            FlattenMode::Both => {
                self.flatten_array(
                    input,
                    path,
                    key_builder,
                    path_builder,
                    index_builder,
                    value_builder,
                    this_builder,
                    rows,
                );
                self.flatten_object(
                    input,
                    path,
                    key_builder,
                    path_builder,
                    index_builder,
                    value_builder,
                    this_builder,
                    rows,
                );
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn flatten_array(
        &mut self,
        input: &[u8],
        path: &str,
        key_builder: &mut Option<NullableColumnBuilder<StringType>>,
        path_builder: &mut Option<StringColumnBuilder>,
        index_builder: &mut Option<NullableColumnBuilder<UInt64Type>>,
        value_builder: &mut Option<BinaryColumnBuilder>,
        this_builder: &mut Option<BinaryColumnBuilder>,
        rows: &mut usize,
    ) {
        if let Some(len) = array_length(input) {
            for i in 0..len {
                let inner_path = format!("{}[{}]", path, i);
                let val = get_by_index(input, i).unwrap();

                if let Some(key_builder) = key_builder {
                    key_builder.push_null();
                }
                if let Some(path_builder) = path_builder {
                    path_builder.put_str(&inner_path);
                    path_builder.commit_row();
                }
                if let Some(index_builder) = index_builder {
                    index_builder.push(i.try_into().unwrap());
                }
                if let Some(value_builder) = value_builder {
                    value_builder.put_slice(&val);
                    value_builder.commit_row();
                }
                if let Some(this_builder) = this_builder {
                    this_builder.put_slice(input);
                    this_builder.commit_row();
                }
                *rows += 1;

                if self.recursive {
                    self.flatten(
                        &val,
                        &inner_path,
                        key_builder,
                        path_builder,
                        index_builder,
                        value_builder,
                        this_builder,
                        rows,
                    );
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn flatten_object(
        &mut self,
        input: &[u8],
        path: &str,
        key_builder: &mut Option<NullableColumnBuilder<StringType>>,
        path_builder: &mut Option<StringColumnBuilder>,
        index_builder: &mut Option<NullableColumnBuilder<UInt64Type>>,
        value_builder: &mut Option<BinaryColumnBuilder>,
        this_builder: &mut Option<BinaryColumnBuilder>,
        rows: &mut usize,
    ) {
        if let Some(obj_keys) = object_keys(input) {
            if let Some(len) = array_length(&obj_keys) {
                for i in 0..len {
                    let key = get_by_index(&obj_keys, i).unwrap();
                    let name = as_str(&key).unwrap();
                    let val = get_by_name(input, &name, false).unwrap();
                    let inner_path = if !path.is_empty() {
                        format!("{}.{}", path, name)
                    } else {
                        name.to_string()
                    };

                    if let Some(key_builder) = key_builder {
                        key_builder.push(name.as_ref());
                    }
                    if let Some(path_builder) = path_builder {
                        path_builder.put_str(&inner_path);
                        path_builder.commit_row();
                    }
                    if let Some(index_builder) = index_builder {
                        index_builder.push_null();
                    }
                    if let Some(value_builder) = value_builder {
                        value_builder.put_slice(&val);
                        value_builder.commit_row();
                    }
                    if let Some(this_builder) = this_builder {
                        this_builder.put_slice(input);
                        this_builder.commit_row();
                    }
                    *rows += 1;

                    if self.recursive {
                        self.flatten(
                            &val,
                            &inner_path,
                            key_builder,
                            path_builder,
                            index_builder,
                            value_builder,
                            this_builder,
                            rows,
                        );
                    }
                }
            }
        }
    }

    fn generate(&mut self, seq: u64, input: &[u8], path: &str, params: &[i64]) -> Vec<Column> {
        // Only columns required by parent plan need a builder.
        let mut key_builder = if params.is_empty() || params.contains(&2) {
            Some(NullableColumnBuilder::<StringType>::with_capacity(0, &[]))
        } else {
            None
        };
        let mut path_builder = if params.is_empty() || params.contains(&3) {
            Some(StringColumnBuilder::with_capacity(0, 0))
        } else {
            None
        };
        let mut index_builder = if params.is_empty() || params.contains(&4) {
            Some(NullableColumnBuilder::<UInt64Type>::with_capacity(0, &[]))
        } else {
            None
        };
        let mut value_builder = if params.is_empty() || params.contains(&5) {
            Some(BinaryColumnBuilder::with_capacity(0, 0))
        } else {
            None
        };
        let mut this_builder = if params.is_empty() || params.contains(&6) {
            Some(BinaryColumnBuilder::with_capacity(0, 0))
        } else {
            None
        };
        let mut rows = 0;

        if !input.is_empty() {
            self.flatten(
                input,
                path,
                &mut key_builder,
                &mut path_builder,
                &mut index_builder,
                &mut value_builder,
                &mut this_builder,
                &mut rows,
            );
        }

        if self.outer && rows == 0 {
            // add an empty row.
            let columns = vec![
                UInt64Type::from_opt_data(vec![Some(seq)]),
                StringType::from_opt_data(vec![None::<&str>]),
                StringType::from_opt_data(vec![None::<&str>]),
                UInt64Type::from_opt_data(vec![None]),
                VariantType::from_opt_data(vec![None]),
                VariantType::from_opt_data(vec![None]),
            ];
            return columns;
        }

        // Generate an empty dummy column for columns that are not needed.
        let seq_column = UInt64Type::upcast_column(vec![seq; rows].into()).wrap_nullable(None);
        let key_column = if let Some(key_builder) = key_builder {
            NullableType::<StringType>::upcast_column(key_builder.build())
        } else {
            StringType::upcast_column(StringColumnBuilder::repeat("", rows).build())
                .wrap_nullable(None)
        };
        let path_column = if let Some(path_builder) = path_builder {
            StringType::upcast_column(path_builder.build()).wrap_nullable(None)
        } else {
            StringType::upcast_column(StringColumnBuilder::repeat("", rows).build())
                .wrap_nullable(None)
        };
        let index_column = if let Some(index_builder) = index_builder {
            NullableType::<UInt64Type>::upcast_column(index_builder.build())
        } else {
            UInt64Type::upcast_column(vec![0u64; rows].into()).wrap_nullable(None)
        };
        let value_column = if let Some(value_builder) = value_builder {
            VariantType::upcast_column(value_builder.build()).wrap_nullable(None)
        } else {
            VariantType::upcast_column(BinaryColumnBuilder::repeat(&[], rows).build())
                .wrap_nullable(None)
        };
        let this_column = if let Some(this_builder) = this_builder {
            VariantType::upcast_column(this_builder.build()).wrap_nullable(None)
        } else {
            VariantType::upcast_column(BinaryColumnBuilder::repeat(&[], rows).build())
                .wrap_nullable(None)
        };

        let columns = vec![
            seq_column,
            key_column,
            path_column,
            index_column,
            value_column,
            this_column,
        ];
        columns
    }
}
