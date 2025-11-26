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
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::Expr;
use databend_common_expression::HashMethod;
use databend_common_expression::HashMethodKind;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::RawExpr;
use databend_common_expression::Scalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_hashtable::BloomHash;
use databend_common_hashtable::FastHash;

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

// Get row hash by HashMethod (uses FastHash, i.e. the hashtable hash)
pub fn hash_by_method<T>(
    method: &HashMethodKind,
    columns: ProjectedBlock,
    num_rows: usize,
    hashes: &mut T,
) -> Result<()>
where
    T: Extend<u64>,
{
    match method {
        HashMethodKind::Serializer(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.fast_hash()),
            );
        }
        HashMethodKind::SingleBinary(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.fast_hash()),
            );
        }
        HashMethodKind::KeysU8(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.fast_hash()),
            );
        }
        HashMethodKind::KeysU16(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.fast_hash()),
            );
        }
        HashMethodKind::KeysU32(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.fast_hash()),
            );
        }
        HashMethodKind::KeysU64(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.fast_hash()),
            );
        }
        HashMethodKind::KeysU128(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.fast_hash()),
            );
        }
        HashMethodKind::KeysU256(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.fast_hash()),
            );
        }
    }
    Ok(())
}

// Get row hash for Bloom filter by HashMethod. This always uses BloomHash,
// which is independent of SSE4.2 and provides a well-distributed 64-bit hash.
pub fn hash_by_method_for_bloom<T>(
    method: &HashMethodKind,
    columns: ProjectedBlock,
    num_rows: usize,
    hashes: &mut T,
) -> Result<()>
where
    T: Extend<u64>,
{
    match method {
        HashMethodKind::Serializer(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.bloom_hash()),
            );
        }
        HashMethodKind::SingleBinary(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.bloom_hash()),
            );
        }
        HashMethodKind::KeysU8(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.bloom_hash()),
            );
        }
        HashMethodKind::KeysU16(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.bloom_hash()),
            );
        }
        HashMethodKind::KeysU32(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.bloom_hash()),
            );
        }
        HashMethodKind::KeysU64(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.bloom_hash()),
            );
        }
        HashMethodKind::KeysU128(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.bloom_hash()),
            );
        }
        HashMethodKind::KeysU256(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.bloom_hash()),
            );
        }
    }
    Ok(())
}

pub(crate) fn min_max_filter(
    min: Scalar,
    max: Scalar,
    probe_key: &Expr<String>,
) -> Result<Expr<String>> {
    let probe_key = match probe_key {
        Expr::ColumnRef(col) => col,
        // Support simple cast that only changes nullability, e.g. CAST(col AS Nullable(T))
        Expr::Cast(cast) => match cast.expr.as_ref() {
            Expr::ColumnRef(col) => col,
            _ => unreachable!(),
        },
        _ => unreachable!(),
    };
    let raw_probe_key = RawExpr::ColumnRef {
        span: probe_key.span,
        id: probe_key.id.to_string(),
        data_type: probe_key.data_type.clone(),
        display_name: probe_key.display_name.clone(),
    };
    let min = RawExpr::Constant {
        span: None,
        scalar: min,
        data_type: None,
    };
    let max = RawExpr::Constant {
        span: None,
        scalar: max,
        data_type: None,
    };
    // Make gte and lte function
    let gte_func = RawExpr::FunctionCall {
        span: None,
        name: "gte".to_string(),
        params: vec![],
        args: vec![raw_probe_key.clone(), min],
    };
    let lte_func = RawExpr::FunctionCall {
        span: None,
        name: "lte".to_string(),
        params: vec![],
        args: vec![raw_probe_key, max],
    };
    // Make and_filters function
    let and_filters_func = RawExpr::FunctionCall {
        span: None,
        name: "and_filters".to_string(),
        params: vec![],
        args: vec![gte_func, lte_func],
    };
    let expr = type_check::check(&and_filters_func, &BUILTIN_FUNCTIONS)?;
    Ok(expr)
}
