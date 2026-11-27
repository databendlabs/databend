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

//! The condition cache (`enable_condition_cache`): for a prewhere filter and a block, the rows
//! the filter selects.
//!
//! Blocks are immutable, so the result of a deterministic filter on a block never changes. A
//! later scan with the same filter skips the blocks where it selects no row before reading
//! them. Of the other blocks, it decodes the prewhere columns for the granules with a selected
//! row; when the entry has the exact rows, it does not evaluate the filter, and reads nothing
//! if it outputs no column.
//!
//! An entry keeps the rows exactly when at most `MAX_EXACT_ROWS` pass, so it is at most a few
//! KiB, and the cache is bounded in bytes (`memory_cache_condition`).

use std::hash::Hash;
use std::hash::Hasher;

use databend_common_catalog::plan::PrewhereInfo;
use databend_common_exception::Result;
use databend_common_expression::Expr;
use databend_common_expression::FunctionCall;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableSchema;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::MutableBitmap;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_cache::BlockCondition;
use databend_storages_common_cache::CacheRef;
use sha2::Digest;
use sha2::Sha256;

use crate::fuse_part::FuseBlockPartInfo;
use crate::io::cache::FuseCaches;

pub const GRANULE_ROWS: usize = 8192;

/// The rows kept exactly at most: 4 KiB per block.
const MAX_EXACT_ROWS: usize = 1024;

pub type Condition = CacheRef<BlockCondition>;

/// The keys of a prewhere filter in the cache.
pub struct ConditionKeys {
    pub whole: u128,
    /// Of a filter of several conjuncts, the first one: a later filter that starts with it
    /// reads only the rows it selects.
    pub leading: Option<LeadingConjunct>,
}

pub struct LeadingConjunct {
    pub key: u128,
    /// A boolean filter of the first conjunct.
    pub conjunct: Expr<String>,
    /// A boolean filter of the others, evaluated on the rows the first one selects.
    pub rest: Expr<String>,
}

/// The keys of a prewhere filter, or `None` if its results cannot be cached: a
/// non-deterministic filter, a filter on virtual or internal columns.
pub fn condition_keys(
    prewhere_info: &PrewhereInfo,
    schema: &TableSchema,
    func_ctx: &FunctionContext,
) -> Option<ConditionKeys> {
    if prewhere_info
        .virtual_column_ids
        .as_ref()
        .is_some_and(|ids| !ids.is_empty())
    {
        return None;
    }
    let filter = prewhere_info.filter.as_expr(&BUILTIN_FUNCTIONS);
    if !filter.is_deterministic(&BUILTIN_FUNCTIONS) {
        return None;
    }
    let whole = key_of(&filter, schema, func_ctx)?;
    let mut conjuncts = split_conjuncts(&filter);
    let leading = if conjuncts.len() > 1 {
        let rest = conjunction(conjuncts.split_off(1)).ok()?;
        let conjunct = conjunction(conjuncts).ok()?;
        key_of(&conjunct, schema, func_ctx).map(|key| LeadingConjunct {
            key,
            conjunct,
            rest,
        })
    } else {
        None
    };
    Some(ConditionKeys { whole, leading })
}

/// The key of a filter: the filter, the columns it reads by id, type, default and computed
/// expression (a column can be renamed, dropped and added again, or altered), and the function
/// context the filter is evaluated with.
fn key_of(filter: &Expr<String>, schema: &TableSchema, func_ctx: &FunctionContext) -> Option<u128> {
    let mut columns: Vec<String> = Vec::new();
    for name in filter.column_refs().into_keys() {
        // Internal columns, e.g. `_row_id`, are not in the schema and depend on the snapshot.
        let field = schema.field_with_name(&name).ok()?;
        columns.push(format!(
            "{}:{}:{:?}:{:?}:{:?}",
            name,
            field.column_id(),
            field.data_type(),
            field.default_expr(),
            field.computed_expr(),
        ));
    }
    columns.sort();

    let func_ctx = format!("{:?}", FunctionContext {
        now: Default::default(),
        ..func_ctx.clone()
    });
    let columns = columns.join(",");

    // The hash of an expression leaves out its spans: the same filter is at different places
    // in the text of different queries.
    let mut hasher = Sha256Hasher(Sha256::new());
    filter.as_remote_expr().hash(&mut hasher);
    let mut hasher = hasher.0;
    for part in [columns.as_bytes(), func_ctx.as_bytes()] {
        hasher.update((part.len() as u64).to_le_bytes());
        hasher.update(part);
    }
    let digest = hasher.finalize();
    Some(u128::from_le_bytes(digest[..16].try_into().unwrap()))
}

/// Feeds `Hash` into SHA-256: `Hash` follows the equality of expressions, which ignores spans.
struct Sha256Hasher(Sha256);

impl Hasher for Sha256Hasher {
    fn write(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    fn finish(&self) -> u64 {
        unreachable!("the digest is read from the SHA-256 state")
    }
}

/// The conjuncts of a filter, split at the non-nullable ANDs.
fn split_conjuncts(expr: &Expr<String>) -> Vec<Expr<String>> {
    match expr {
        Expr::FunctionCall(FunctionCall { function, args, .. })
            if matches!(function.signature.name.as_str(), "and" | "and_filters")
                && expr.data_type() == &DataType::Boolean =>
        {
            args.iter().flat_map(split_conjuncts).collect()
        }
        _ => vec![expr.clone()],
    }
}

/// A boolean filter of the conjuncts.
fn conjunction(mut conjuncts: Vec<Expr<String>>) -> Result<Expr<String>> {
    if conjuncts.len() == 1 && conjuncts[0].data_type() == &DataType::Boolean {
        return Ok(conjuncts.pop().unwrap());
    }
    let name = if conjuncts.len() == 1 {
        "is_true"
    } else {
        "and_filters"
    };
    check_function(None, name, &[], &conjuncts, &BUILTIN_FUNCTIONS)
}

/// The cache entry of a block for the filter of a scan.
#[derive(Clone)]
pub enum CachedCondition {
    /// The entry of the whole filter.
    Whole(Condition),
    /// The entry of its leading conjunct: a superset of the rows it selects.
    Leading(Condition),
}

impl CachedCondition {
    pub fn condition(&self) -> &BlockCondition {
        match self {
            CachedCondition::Whole(condition) | CachedCondition::Leading(condition) => condition,
        }
    }

    /// Whether the entry has exactly the rows the filter selects.
    pub fn is_exact(&self) -> bool {
        matches!(self, CachedCondition::Whole(condition) if is_exact(condition))
    }

    /// Whether the filter selects no row.
    pub fn selects_nothing(&self) -> bool {
        matches!(self.condition(), BlockCondition::Nothing)
    }
}

/// The entry of the block for the filter of `keys`, if any.
pub fn lookup_condition(keys: &ConditionKeys, part: &FuseBlockPartInfo) -> Option<CachedCondition> {
    if let Some(condition) = lookup(keys.whole, part) {
        return Some(CachedCondition::Whole(condition));
    }
    let leading = keys.leading.as_ref()?;
    lookup(leading.key, part).map(CachedCondition::Leading)
}

/// The parts that cover a whole block are cached.
fn cacheable(part: &FuseBlockPartInfo) -> bool {
    part.range()
        .is_none_or(|range| range.start == 0 && range.end >= part.nums_rows)
}

fn cache_key(key: u128, part: &FuseBlockPartInfo) -> String {
    format!("{key:032x}/{}", part.location)
}

fn lookup(key: u128, part: &FuseBlockPartInfo) -> Option<Condition> {
    if !cacheable(part) {
        return None;
    }
    FuseCaches::instance()
        .conditions
        .lookup(&cache_key(key, part))
}

/// Records the rows of the block that the filter selects.
pub fn insert(key: u128, part: &FuseBlockPartInfo, rows: &Bitmap) {
    if !cacheable(part) || rows.len() != part.nums_rows {
        return;
    }
    let cache = &FuseCaches::instance().conditions;
    if cache.is_enabled() {
        cache.fill(&cache_key(key, part), condition_of(rows));
    }
}

/// Whether the entry has the exact rows, rather than a superset.
pub fn is_exact(condition: &BlockCondition) -> bool {
    !matches!(condition, BlockCondition::Granules { .. })
}

fn condition_of(rows: &Bitmap) -> BlockCondition {
    let num_rows = rows.len();
    let selected = num_rows - rows.null_count();
    if selected == 0 {
        return BlockCondition::Nothing;
    }
    if selected == num_rows {
        return BlockCondition::All;
    }
    if selected <= MAX_EXACT_ROWS {
        let offsets = rows
            .iter()
            .enumerate()
            .filter_map(|(row, set)| set.then_some(row as u32))
            .collect();
        return BlockCondition::Rows(offsets);
    }
    let granules = num_rows.div_ceil(GRANULE_ROWS);
    let mut bits = vec![0u64; granules.div_ceil(64)];
    for granule in 0..granules {
        let start = granule * GRANULE_ROWS;
        let len = GRANULE_ROWS.min(num_rows - start);
        if rows.clone().sliced(start, len).null_count() < len {
            bits[granule / 64] |= 1 << (granule % 64);
        }
    }
    BlockCondition::Granules {
        granule_rows: GRANULE_ROWS as u32,
        bits: bits.into(),
    }
}

/// The rows of the block the entry selects, a superset unless it [`is_exact`]; `None` for every
/// row.
pub fn rows_of(condition: &BlockCondition, num_rows: usize) -> Option<Bitmap> {
    match condition {
        BlockCondition::All => None,
        BlockCondition::Nothing => Some(MutableBitmap::from_len_zeroed(num_rows).into()),
        BlockCondition::Rows(offsets) => {
            let mut rows = MutableBitmap::from_len_zeroed(num_rows);
            for offset in offsets.iter() {
                rows.set(*offset as usize, true);
            }
            Some(rows.into())
        }
        BlockCondition::Granules { granule_rows, bits } => {
            let granule_rows = *granule_rows as usize;
            let granules = num_rows.div_ceil(granule_rows);
            let is_set = |granule: usize| bits[granule / 64] & (1 << (granule % 64)) != 0;
            if (0..granules).all(is_set) {
                return None;
            }
            let mut rows = MutableBitmap::with_capacity(num_rows);
            for granule in 0..granules {
                let len = granule_rows.min(num_rows - granule * granule_rows);
                rows.extend_constant(len, is_set(granule));
            }
            Some(rows.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bitmap(num_rows: usize, set: &[usize]) -> Bitmap {
        let mut rows = MutableBitmap::from_len_zeroed(num_rows);
        for row in set {
            rows.set(*row, true);
        }
        rows.into()
    }

    fn column(name: &str, data_type: DataType) -> Expr<String> {
        Expr::ColumnRef(databend_common_expression::ColumnRef {
            span: None,
            id: name.to_string(),
            data_type,
            display_name: name.to_string(),
        })
    }

    fn call(name: &str, args: &[Expr<String>]) -> Expr<String> {
        check_function(None, name, &[], args, &BUILTIN_FUNCTIONS).unwrap()
    }

    fn int(value: i32, span: Option<std::ops::Range<usize>>) -> Expr<String> {
        Expr::Constant(databend_common_expression::Constant {
            span: span.map(Into::into),
            scalar: databend_common_expression::Scalar::Number(value.into()),
            data_type: DataType::Number(databend_common_expression::types::NumberDataType::Int32),
        })
    }

    fn prewhere(filter: &Expr<String>) -> PrewhereInfo {
        PrewhereInfo {
            output_columns: databend_common_catalog::plan::Projection::Columns(vec![]),
            prewhere_columns: databend_common_catalog::plan::Projection::Columns(vec![0, 1]),
            remain_columns: databend_common_catalog::plan::Projection::Columns(vec![]),
            filter: filter.as_remote_expr(),
            virtual_column_ids: None,
        }
    }

    #[test]
    fn test_condition_keys() {
        use databend_common_expression::TableDataType;
        use databend_common_expression::TableField;
        use databend_common_expression::types::NumberDataType;

        let schema = TableSchema::new(vec![
            TableField::new("a", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("b", TableDataType::Number(NumberDataType::Int32)),
        ]);
        let func_ctx = FunctionContext::default();
        let a = column("a", DataType::Number(NumberDataType::Int32));
        let b = column("b", DataType::Number(NumberDataType::Int32));
        let a_eq = |span| call("eq", &[a.clone(), int(1, span)]);
        let b_gt = call("gt", &[b.clone(), int(2, None)]);

        let alone = condition_keys(&prewhere(&a_eq(None)), &schema, &func_ctx).unwrap();
        assert!(alone.leading.is_none());
        // The same filter elsewhere in the text of a query has the same key.
        let moved = condition_keys(&prewhere(&a_eq(Some(7..8))), &schema, &func_ctx).unwrap();
        assert_eq!(alone.whole, moved.whole);

        let both = call("and_filters", &[a_eq(Some(3..4)), b_gt.clone()]);
        let both = condition_keys(&prewhere(&both), &schema, &func_ctx).unwrap();
        assert_ne!(both.whole, alone.whole);
        let leading = both.leading.unwrap();
        assert_eq!(leading.key, alone.whole);
        assert_eq!(leading.rest, b_gt);

        let swapped = call("and_filters", &[b_gt.clone(), a_eq(None)]);
        let swapped = condition_keys(&prewhere(&swapped), &schema, &func_ctx).unwrap();
        assert_ne!(swapped.leading.unwrap().key, alone.whole);

        // Columns are part of the key: a renamed column is another one.
        let renamed = TableSchema::new(vec![
            TableField::new("b", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("a", TableDataType::Number(NumberDataType::Int32)),
        ]);
        let renamed = condition_keys(&prewhere(&a_eq(None)), &renamed, &func_ctx).unwrap();
        assert_ne!(renamed.whole, alone.whole);

        // And so are their defaults.
        let defaulted = TableSchema::new(vec![
            TableField::new("a", TableDataType::Number(NumberDataType::Int32))
                .with_default_expr(Some("1".to_string())),
            TableField::new("b", TableDataType::Number(NumberDataType::Int32)),
        ]);
        let defaulted = condition_keys(&prewhere(&a_eq(None)), &defaulted, &func_ctx).unwrap();
        assert_ne!(defaulted.whole, alone.whole);

        // Internal or non-deterministic filters are not cached.
        let row_id = call("eq", &[
            column("_row_id", DataType::Number(NumberDataType::UInt64)),
            call("to_uint64", &[int(1, None)]),
        ]);
        assert!(condition_keys(&prewhere(&row_id), &schema, &func_ctx).is_none());
        let rand = call("gt", &[
            call("rand", &[]),
            call("to_float64", &[int(1, None)]),
        ]);
        assert!(condition_keys(&prewhere(&rand), &schema, &func_ctx).is_none());
    }

    #[test]
    fn test_condition_of() {
        let num_rows = GRANULE_ROWS * 2 + 10;
        assert_eq!(
            condition_of(&bitmap(num_rows, &[])),
            BlockCondition::Nothing
        );
        let all: Bitmap = MutableBitmap::from_len_set(num_rows).into();
        assert_eq!(condition_of(&all), BlockCondition::All);
        assert!(rows_of(&BlockCondition::All, num_rows).is_none());

        // Few rows are kept exactly.
        let rows = bitmap(num_rows, &[3, GRANULE_ROWS * 2 + 9]);
        let condition = condition_of(&rows);
        assert_eq!(
            condition,
            BlockCondition::Rows(vec![3, GRANULE_ROWS as u32 * 2 + 9].into())
        );
        assert!(is_exact(&condition));
        assert_eq!(rows_of(&condition, num_rows).unwrap(), rows);

        // Many as the granules that hold them.
        let many: Vec<usize> = (GRANULE_ROWS * 2..num_rows)
            .chain(0..MAX_EXACT_ROWS)
            .collect();
        let condition = condition_of(&bitmap(num_rows, &many));
        assert!(!is_exact(&condition));
        let rows = rows_of(&condition, num_rows).unwrap();
        assert_eq!(rows.len(), num_rows);
        assert_eq!(rows.null_count(), GRANULE_ROWS);
        assert!(rows.get_bit(0) && !rows.get_bit(GRANULE_ROWS) && rows.get_bit(num_rows - 1));
    }
}
