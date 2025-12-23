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

use std::collections::BTreeMap;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::FromData;
use databend_common_expression::Function;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionFactory;
use databend_common_expression::FunctionKind;
use databend_common_expression::FunctionProperty;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::VariantType;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::types::nullable::NullableColumnBuilder;
use databend_common_expression::types::string::StringColumnBuilder;
use jaq_core;
use jaq_interpret::Ctx;
use jaq_interpret::FilterT;
use jaq_interpret::ParseCtx;
use jaq_interpret::RcIter;
use jaq_interpret::Val;
use jaq_parse;
use jaq_std;
use jsonb::OwnedJsonb;
use jsonb::RawJsonb;
use jsonb::from_raw_jsonb;
use jsonb::jsonpath::parse_json_path;

pub fn register(registry: &mut FunctionRegistry) {
    registry.properties.insert(
        "json_path_query".to_string(),
        FunctionProperty::default().kind(FunctionKind::SRF),
    );

    let json_path_query = FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
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

                    let scalar_json_path = match path_arg {
                        Value::Scalar(Scalar::String(ref path)) => {
                            let Ok(json_path) = parse_json_path(path.as_bytes()) else {
                                ctx.set_error(0, format!("Invalid JSON Path '{}'", &path));
                                return results;
                            };
                            Some(json_path)
                        }
                        _ => None,
                    };

                    for (row, max_nums_per_row) in
                        max_nums_per_row.iter_mut().enumerate().take(ctx.num_rows)
                    {
                        let val = unsafe { val_arg.index_unchecked(row) };
                        let mut builder = BinaryColumnBuilder::with_capacity(0, 0);
                        let res = match (val, &scalar_json_path) {
                            (ScalarRef::Variant(val), Some(json_path)) => {
                                RawJsonb::new(val).select_by_path(json_path)
                            }
                            (ScalarRef::Variant(val), None) => {
                                let path = unsafe { path_arg.index_unchecked(row) };
                                match path {
                                    ScalarRef::String(path) => {
                                        let Ok(json_path) = parse_json_path(path.as_bytes()) else {
                                            ctx.set_error(
                                                0,
                                                format!("Invalid JSON Path '{}'", &path),
                                            );
                                            return results;
                                        };
                                        RawJsonb::new(val).select_by_path(&json_path)
                                    }
                                    _ => Ok(vec![]),
                                }
                            }
                            (_, _) => Ok(vec![]),
                        };
                        match res {
                            Ok(owned_jsonbs) => {
                                for owned_jsonb in owned_jsonbs {
                                    builder.put_slice(owned_jsonb.as_ref());
                                    builder.commit_row();
                                }
                            }
                            Err(err) => {
                                ctx.set_error(0, format!("Select json path failed err: '{}'", err));
                                break;
                            }
                        }

                        let array = Column::Variant(builder.build()).wrap_nullable(None);
                        let array_len = array.len();
                        *max_nums_per_row = std::cmp::max(*max_nums_per_row, array_len);
                        results.push((Value::Column(Column::Tuple(vec![array])), array_len));
                    }
                    results
                }),
            },
        }))
    }));
    registry.register_function_factory("json_path_query", json_path_query);

    registry.properties.insert(
        "json_array_elements".to_string(),
        FunctionProperty::default().kind(FunctionKind::SRF),
    );
    let json_array_elements = FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
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
    }));
    registry.register_function_factory("json_array_elements", json_array_elements);

    registry.properties.insert(
        "json_each".to_string(),
        FunctionProperty::default().kind(FunctionKind::SRF),
    );
    let json_each = FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
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
    }));
    registry.register_function_factory("json_each", json_each);

    registry.properties.insert(
        "flatten".to_string(),
        FunctionProperty::default().kind(FunctionKind::SRF),
    );
    let flatten = FunctionFactory::Closure(Box::new(|params, args_type: &[DataType]| {
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
                            Value::Scalar(Scalar::String(v)) => {
                                match parse_json_path(v.as_bytes()) {
                                    Ok(jsonpath) => {
                                        json_path = Some((v, jsonpath));
                                    }
                                    Err(_) => {
                                        ctx.set_error(0, format!("Invalid JSON Path {v:?}",));
                                        return results;
                                    }
                                }
                            }
                            Value::Column(_) => {
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
                            Value::Scalar(Scalar::Boolean(v)) => {
                                outer = *v;
                            }
                            Value::Column(_) => {
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
                            Value::Scalar(Scalar::Boolean(v)) => {
                                recursive = *v;
                            }
                            Value::Column(_) => {
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
                        match &args[4] {
                            Value::Scalar(Scalar::String(v)) => match v.to_lowercase().as_str() {
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
                            },
                            Value::Column(_) => {
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
                                results
                                    .push((Value::Scalar(Scalar::Tuple(vec![Scalar::Null; 6])), 0));
                            }
                            ScalarRef::Variant(val) => {
                                let columns = match json_path {
                                    Some((path, ref json_path)) => {
                                        // get inner input values by path
                                        let res = match RawJsonb::new(val)
                                            .select_first_by_path(json_path)
                                        {
                                            Ok(res_jsonb) => res_jsonb,
                                            Err(err) => {
                                                ctx.set_error(
                                                    0,
                                                    format!(
                                                        "Select json path failed err: '{}'",
                                                        err
                                                    ),
                                                );
                                                break;
                                            }
                                        };
                                        if let Some(owned_jsonb) = res {
                                            generator.generate(
                                                (row + 1) as u64,
                                                Some(owned_jsonb.as_raw()),
                                                path,
                                                &params,
                                            )
                                        } else {
                                            generator.generate(
                                                (row + 1) as u64,
                                                None,
                                                path,
                                                &params,
                                            )
                                        }
                                    }
                                    None => generator.generate(
                                        (row + 1) as u64,
                                        Some(RawJsonb::new(val)),
                                        "",
                                        &params,
                                    ),
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
    }));
    registry.register_function_factory("flatten", flatten);

    registry.properties.insert(
        "jq".to_string(),
        FunctionProperty::default().kind(FunctionKind::SRF),
    );
    let jq = FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
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
                    // This could be a useful enhancement, but leaving it out for now.
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
                                    let raw_jsonb = RawJsonb::new(v);
                                    let s = from_raw_jsonb::<serde_json::Value>(&raw_jsonb);
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
                                                    Ok(res) => match jaq_val_to_jsonb(&res) {
                                                        Ok(res_jsonb) => {
                                                            res_builder.put_slice(&res_jsonb);
                                                            res_builder.commit_row();
                                                        }
                                                        Err(err) => {
                                                            ctx.set_error(row, err.to_string());
                                                            return null_result;
                                                        }
                                                    },
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
    }));
    registry.register_function_factory("jq", jq);
}

// Convert a Jaq val to a jsonb value.
fn jaq_val_to_jsonb(val: &Val) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let jsonb_value = match val {
        Val::Null => jsonb::Value::Null,
        Val::Bool(b) => jsonb::Value::Bool(*b),
        Val::Num(n) => {
            if let Ok(f) = n.parse::<f64>() {
                f.into()
            } else {
                return Err(ErrorCode::BadBytes(format!(
                    "parse string `{}` to f64 failed",
                    n
                )));
            }
        }
        Val::Float(f) => (*f).into(),
        Val::Int(i) => (*i).into(),
        Val::Str(s) => jsonb::Value::String((**s).clone().into()),
        Val::Arr(arr) => {
            let items = arr
                .iter()
                .map(jaq_val_to_jsonb)
                .collect::<Result<Vec<_>>>()?;
            let owned_jsonb = OwnedJsonb::build_array(items.iter().map(|v| RawJsonb::new(v)))
                .map_err(|e| {
                    ErrorCode::Internal(format!("failed to build array error: {:?}", e))
                })?;
            return Ok(owned_jsonb.to_vec());
        }
        Val::Obj(obj) => {
            let mut kvs = BTreeMap::new();
            for (k, v) in obj.iter() {
                let key = (**k).clone();
                let val = jaq_val_to_jsonb(v)?;
                kvs.insert(key, val);
            }
            let owned_jsonb =
                OwnedJsonb::build_object(kvs.iter().map(|(k, v)| (k, RawJsonb::new(&v[..]))))
                    .map_err(|e| {
                        ErrorCode::Internal(format!("failed to build object error: {:?}", e))
                    })?;
            return Ok(owned_jsonb.to_vec());
        }
    };
    jsonb_value.write_to_vec(&mut buf);
    Ok(buf)
}

pub(crate) fn unnest_variant_array(
    val: &[u8],
    row: usize,
    max_nums_per_row: &mut [usize],
) -> (Value<AnyType>, usize) {
    match RawJsonb::new(val).array_values() {
        Ok(Some(vals)) if !vals.is_empty() => {
            let len = vals.len();
            let mut builder = BinaryColumnBuilder::with_capacity(len, 0);

            max_nums_per_row[row] = std::cmp::max(max_nums_per_row[row], len);

            for val in vals {
                builder.put_slice(val.as_ref());
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
    match RawJsonb::new(val).object_each() {
        Ok(Some(key_vals)) if !key_vals.is_empty() => {
            let len = key_vals.len();
            let mut val_builder = BinaryColumnBuilder::with_capacity(len, 0);
            let mut key_builder = StringColumnBuilder::with_capacity(len);

            max_nums_per_row[row] = std::cmp::max(max_nums_per_row[row], len);

            for (key, val) in key_vals {
                key_builder.put_and_commit(&key);
                val_builder.put_slice(val.as_ref());
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

    fn flatten(
        &mut self,
        input: &RawJsonb,
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

    fn flatten_array(
        &mut self,
        input: &RawJsonb,
        path: &str,
        key_builder: &mut Option<NullableColumnBuilder<StringType>>,
        path_builder: &mut Option<StringColumnBuilder>,
        index_builder: &mut Option<NullableColumnBuilder<UInt64Type>>,
        value_builder: &mut Option<BinaryColumnBuilder>,
        this_builder: &mut Option<BinaryColumnBuilder>,
        rows: &mut usize,
    ) {
        if let Ok(Some(vals)) = input.array_values() {
            for (i, val) in vals.into_iter().enumerate() {
                let inner_path = if path_builder.is_some() {
                    format!("{}[{}]", path, i)
                } else {
                    "".to_string()
                };

                if let Some(key_builder) = key_builder {
                    key_builder.push_null();
                }
                if let Some(path_builder) = path_builder {
                    path_builder.put_and_commit(&inner_path);
                }
                if let Some(index_builder) = index_builder {
                    index_builder.push(i.try_into().unwrap());
                }
                if let Some(value_builder) = value_builder {
                    value_builder.put_slice(val.as_ref());
                    value_builder.commit_row();
                }
                if let Some(this_builder) = this_builder {
                    this_builder.put_slice(input.as_ref());
                    this_builder.commit_row();
                }
                *rows += 1;

                if self.recursive {
                    self.flatten(
                        &val.as_raw(),
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

    fn flatten_object(
        &mut self,
        input: &RawJsonb,
        path: &str,
        key_builder: &mut Option<NullableColumnBuilder<StringType>>,
        path_builder: &mut Option<StringColumnBuilder>,
        index_builder: &mut Option<NullableColumnBuilder<UInt64Type>>,
        value_builder: &mut Option<BinaryColumnBuilder>,
        this_builder: &mut Option<BinaryColumnBuilder>,
        rows: &mut usize,
    ) {
        if let Ok(Some(key_vals)) = input.object_each() {
            for (key, val) in key_vals {
                if let Some(key_builder) = key_builder {
                    key_builder.push(key.as_ref());
                }
                let inner_path = if path_builder.is_some() {
                    if !path.is_empty() {
                        format!("{}.{}", path, key)
                    } else {
                        key
                    }
                } else {
                    "".to_string()
                };
                if let Some(path_builder) = path_builder {
                    path_builder.put_and_commit(&inner_path);
                }
                if let Some(index_builder) = index_builder {
                    index_builder.push_null();
                }
                if let Some(value_builder) = value_builder {
                    value_builder.put_slice(val.as_ref());
                    value_builder.commit_row();
                }
                if let Some(this_builder) = this_builder {
                    this_builder.put_slice(input.as_ref());
                    this_builder.commit_row();
                }
                *rows += 1;

                if self.recursive {
                    self.flatten(
                        &val.as_raw(),
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

    fn generate(
        &mut self,
        seq: u64,
        input: Option<RawJsonb>,
        path: &str,
        params: &[i64],
    ) -> Vec<Column> {
        // Only columns required by parent plan need a builder.
        let mut key_builder = if params.is_empty() || params.contains(&2) {
            Some(NullableColumnBuilder::<StringType>::with_capacity(0, &[]))
        } else {
            None
        };
        let mut path_builder = if params.is_empty() || params.contains(&3) {
            Some(StringColumnBuilder::with_capacity(0))
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

        if let Some(input) = input {
            self.flatten(
                &input,
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

        let validity = Some(Bitmap::new_constant(true, rows));
        // Generate an empty dummy column for columns that are not needed.
        let seq_column =
            UInt64Type::upcast_column(vec![seq; rows].into()).wrap_nullable(validity.clone());
        let key_column = if let Some(key_builder) = key_builder {
            NullableType::<StringType>::upcast_column(key_builder.build())
        } else {
            StringType::upcast_column(StringColumnBuilder::repeat("", rows).build())
                .wrap_nullable(validity.clone())
        };
        let path_column = if let Some(path_builder) = path_builder {
            StringType::upcast_column(path_builder.build()).wrap_nullable(validity.clone())
        } else {
            StringType::upcast_column(StringColumnBuilder::repeat("", rows).build())
                .wrap_nullable(validity.clone())
        };
        let index_column = if let Some(index_builder) = index_builder {
            NullableType::<UInt64Type>::upcast_column(index_builder.build())
        } else {
            UInt64Type::upcast_column(vec![0u64; rows].into()).wrap_nullable(validity.clone())
        };
        let value_column = if let Some(value_builder) = value_builder {
            VariantType::upcast_column(value_builder.build()).wrap_nullable(validity.clone())
        } else {
            VariantType::upcast_column(BinaryColumnBuilder::repeat(&[], rows).build())
                .wrap_nullable(validity.clone())
        };
        let this_column = if let Some(this_builder) = this_builder {
            VariantType::upcast_column(this_builder.build()).wrap_nullable(validity.clone())
        } else {
            VariantType::upcast_column(BinaryColumnBuilder::repeat(&[], rows).build())
                .wrap_nullable(validity.clone())
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
