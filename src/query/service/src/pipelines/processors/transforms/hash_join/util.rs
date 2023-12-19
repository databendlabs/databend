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

use databend_common_exception::Result;
use databend_common_expression::type_check;
use databend_common_expression::types::AnyType;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethod;
use databend_common_expression::HashMethodKind;
use databend_common_expression::RawExpr;
use databend_common_expression::Value;
use databend_common_functions::BUILTIN_FUNCTIONS;
use xorf::BinaryFuse8;

pub(crate) fn build_schema_wrap_nullable(build_schema: &DataSchemaRef) -> DataSchemaRef {
    let mut nullable_field = Vec::with_capacity(build_schema.fields().len());
    for field in build_schema.fields() {
        nullable_field.push(DataField::new(
            field.name(),
            field.data_type().wrap_nullable(),
        ));
    }
    DataSchemaRefExt::create(nullable_field)
}

pub(crate) fn probe_schema_wrap_nullable(probe_schema: &DataSchemaRef) -> DataSchemaRef {
    let mut nullable_field = Vec::with_capacity(probe_schema.fields().len());
    for field in probe_schema.fields() {
        nullable_field.push(DataField::new(
            field.name(),
            field.data_type().wrap_nullable(),
        ));
    }
    DataSchemaRefExt::create(nullable_field)
}

// Construct inlist runtime filter
pub(crate) fn inlist_filter(
    probe_key: &Expr<String>,
    build_column: Value<AnyType>,
) -> Result<Option<Expr<String>>> {
    // Currently, only support key is a column, will support more later.
    // Such as t1.a + 1 = t2.a, or t1.a + t1.b = t2.a (left side is probe side)
    if let Expr::ColumnRef {
        span,
        id,
        data_type,
        display_name,
    } = probe_key
    {
        let raw_probe_key = RawExpr::ColumnRef {
            span: *span,
            id: id.to_string(),
            data_type: data_type.clone(),
            display_name: display_name.clone(),
        };
        let array = RawExpr::Constant {
            span: None,
            scalar: build_column.as_scalar().unwrap().to_owned(),
        };

        let args = vec![array, raw_probe_key];
        // Make contain function
        let contain_func = RawExpr::FunctionCall {
            span: None,
            name: "contains".to_string(),
            params: vec![],
            args,
        };
        let expr = type_check::check(&contain_func, &BUILTIN_FUNCTIONS)?;
        return Ok(Some(expr));
    }
    Ok(None)
}

pub(crate) fn bloom_filter(
    build_key: &Expr,
    probe_key: &Expr<String>,
    build_column: Value<AnyType>,
    num_rows: usize,
) -> Result<Option<(String, BinaryFuse8)>> {
    if !build_key.data_type().is_numeric() {
        return Ok(None);
    }
    if let Expr::ColumnRef { id, .. } = probe_key {
        // Generate bloom filter using build column
        let data_type = build_key.data_type().clone();
        let build_key_column = build_column.convert_to_full_column(&data_type, num_rows);
        let num_rows = build_key_column.len();
        let method = DataBlock::choose_hash_method_with_types(&[data_type.clone()], false)?;
        let mut hashes = Vec::with_capacity(num_rows);
        match method {
            HashMethodKind::KeysU8(hash_method) => {
                let key_state =
                    hash_method.build_keys_state(&[(build_key_column, data_type)], num_rows)?;
                hash_method.build_keys_accessor_and_hashes(key_state, &mut hashes)?;
            }
            HashMethodKind::KeysU16(hash_method) => {
                let key_state =
                    hash_method.build_keys_state(&[(build_key_column, data_type)], num_rows)?;
                hash_method.build_keys_accessor_and_hashes(key_state, &mut hashes)?;
            }
            HashMethodKind::KeysU32(hash_method) => {
                let key_state =
                    hash_method.build_keys_state(&[(build_key_column, data_type)], num_rows)?;
                hash_method.build_keys_accessor_and_hashes(key_state, &mut hashes)?;
            }
            HashMethodKind::KeysU64(hash_method) => {
                let key_state =
                    hash_method.build_keys_state(&[(build_key_column, data_type)], num_rows)?;
                hash_method.build_keys_accessor_and_hashes(key_state, &mut hashes)?;
            }
            _ => unreachable!(),
        }
        let filter = BinaryFuse8::try_from(hashes)?;
        return Ok(Some((id.to_string(), filter)));
    }
    Ok(None)
}

// Deduplicate build key colum
pub(crate) fn dedup_build_key_column(
    func_ctx: &FunctionContext,
    data_blocks: &[DataBlock],
    build_key: &Expr,
) -> Result<Option<Value<AnyType>>> {
    // Dedup build key column
    let mut columns = Vec::with_capacity(data_blocks.len());
    for block in data_blocks.iter() {
        if block.num_columns() == 0 {
            continue;
        }
        let evaluator = Evaluator::new(block, &func_ctx, &BUILTIN_FUNCTIONS);
        let column = evaluator
            .run(build_key)?
            .convert_to_full_column(build_key.data_type(), block.num_rows());
        columns.push(column);
    }
    if columns.is_empty() {
        return Ok(None);
    }
    let build_key_column = Column::concat_columns(columns.into_iter())?;
    let mut list = Vec::with_capacity(build_key_column.len());
    for value in build_key_column.iter() {
        list.push(RawExpr::Constant {
            span: None,
            scalar: value.to_owned(),
        })
    }
    let array = RawExpr::FunctionCall {
        span: None,
        name: "array".to_string(),
        params: vec![],
        args: list,
    };
    let distinct_list = RawExpr::FunctionCall {
        span: None,
        name: "array_distinct".to_string(),
        params: vec![],
        args: vec![array],
    };

    // Deduplicate build key column
    let empty_key_block = DataBlock::empty();
    let evaluator = Evaluator::new(&empty_key_block, &func_ctx, &BUILTIN_FUNCTIONS);
    Ok(Some(evaluator.run(&type_check::check(
        &distinct_list,
        &BUILTIN_FUNCTIONS,
    )?)?))
}
