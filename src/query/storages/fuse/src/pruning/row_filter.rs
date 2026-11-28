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

//! Filters on table columns evaluated per block while pruning, so that index
//! pruners search only the passing rows.

use std::sync::Arc;

use databend_common_catalog::plan::Projection;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::VECTOR_SCORE_COL_NAME;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::Bitmap;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::executor::cast_expr_to_non_null_boolean;
use databend_storages_common_index::RangeIndex;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_pruner::RangeIndexInput;
use databend_storages_common_pruner::RangePruner;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use tokio::sync::OwnedSemaphorePermit;

use crate::FuseStorageFormat;
use crate::io::BlockReader;
use crate::pruning::PruningContext;

/// The conjuncts of `filter` under `and` and `and_filters`, flattened, in order.
pub(crate) fn conjuncts(filter: Expr<String>) -> Vec<Expr<String>> {
    let mut conjuncts = Vec::new();
    let mut parts = vec![filter];
    while let Some(expr) = parts.pop() {
        if let Expr::FunctionCall(call) = &expr
            && matches!(call.function.signature.name.as_str(), "and" | "and_filters")
        {
            parts.extend(call.args.iter().rev().cloned());
            continue;
        }
        conjuncts.push(expr);
    }
    conjuncts
}

/// A filter split by the columns its conjuncts read.
#[derive(Default)]
pub(crate) struct ScoreConjuncts {
    /// The conjuncts on the vector score only.
    pub(crate) score: Option<Expr<String>>,
    /// The conjuncts on other columns only.
    pub(crate) columns: Option<Expr<String>>,
}

/// `None` when a conjunct reads both the vector score and other columns.
pub(crate) fn split_score_conjuncts(filter: Expr<String>) -> Result<Option<ScoreConjuncts>> {
    let mut score = Vec::new();
    let mut columns = Vec::new();
    for expr in conjuncts(filter) {
        let column_refs = expr.column_refs();
        if !column_refs.contains_key(VECTOR_SCORE_COL_NAME) {
            columns.push(expr);
        } else if column_refs.len() == 1 {
            score.push(expr);
        } else {
            return Ok(None);
        }
    }
    Ok(Some(ScoreConjuncts {
        score: conjunction(score)?,
        columns: conjunction(columns)?,
    }))
}

/// The conjunction of `exprs`, with NULL as false; `None` when empty.
pub(crate) fn conjunction(mut exprs: Vec<Expr<String>>) -> Result<Option<Expr<String>>> {
    Ok(match exprs.len() {
        0 => None,
        1 => Some(cast_expr_to_non_null_boolean(exprs.pop().unwrap())?),
        _ => Some(check_function(
            None,
            "and_filters",
            &[],
            &exprs,
            &BUILTIN_FUNCTIONS,
        )?),
    })
}

/// The rows of a block that pass the filters.
pub(crate) enum BlockRows {
    /// All rows, known from the column statistics.
    All,
    /// One bit per row, from the block data; may be none or all.
    Rows(Bitmap),
}

/// Evaluates `filter` on the blocks concurrently and maps their rows with
/// `select`, dropping the blocks it maps to `None`. Keeps the block order.
pub(crate) async fn filter_blocks<T, F>(
    filter: &Arc<ColumnFilter>,
    pruning_ctx: &Arc<PruningContext>,
    metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
    select: F,
) -> Result<(Vec<(BlockMetaIndex, Arc<BlockMeta>)>, Vec<T>)>
where
    T: Send + 'static,
    F: Fn(BlockRows) -> Option<T> + Clone + Send + 'static,
{
    let mut metas = metas.into_iter();
    let tasks = std::iter::from_fn(|| {
        if pruning_ctx.check_aborting().is_err() {
            return None;
        }
        let (block_meta_index, block_meta) = metas.next()?;
        let filter = filter.clone();
        let pruning_ctx = pruning_ctx.clone();
        let select = select.clone();
        Some(move |permit: OwnedSemaphorePermit| async move {
            let _permit = permit;
            pruning_ctx.check_aborting()?;
            let rows = filter.block_rows(block_meta.as_ref()).await?;
            Ok(select(rows).map(|selected| (block_meta_index, block_meta, selected)))
        })
    });
    let semaphore = pruning_ctx.pruning_semaphore.clone();
    let results = pruning_ctx.run_tasks(semaphore, tasks).await?;
    Ok(results
        .into_iter()
        .flatten()
        .map(|(block_meta_index, block_meta, selected)| ((block_meta_index, block_meta), selected))
        .unzip())
}

/// Deterministic filters on table columns, evaluated per block while pruning.
pub(crate) struct ColumnFilter {
    func_ctx: FunctionContext,
    /// Over the columns read by `reader`.
    expr: Expr,
    reader: Arc<BlockReader>,
    read_settings: ReadSettings,
    /// Range index of `NOT filter`: a block it prunes passes whole.
    inverse_range_index: RangeIndex,
}

impl ColumnFilter {
    /// `filter` is a non-NULL boolean. `None` when it reads the vector score or
    /// a column that is not a table column (an inner or virtual column).
    pub(crate) fn try_create(
        pruning_ctx: &PruningContext,
        func_ctx: FunctionContext,
        schema: TableSchemaRef,
        filter: Expr<String>,
    ) -> Result<Option<Self>> {
        let mut names: Vec<String> = filter.column_refs().into_keys().collect();
        if names.is_empty() || names.iter().any(|name| name == VECTOR_SCORE_COL_NAME) {
            return Ok(None);
        }
        names.sort();
        let mut indices = Vec::with_capacity(names.len());
        for name in &names {
            match schema.index_of(name) {
                Ok(index) => indices.push(index),
                Err(_) => return Ok(None),
            }
        }
        let reader = BlockReader::create(
            pruning_ctx.ctx.clone(),
            pruning_ctx.dal.clone(),
            schema.clone(),
            Projection::Columns(indices),
            pruning_ctx.ctx.cache_policy(),
        )?;
        let data_schema = reader.data_schema();
        let expr = filter.project_column_ref(|name| data_schema.index_of(name))?;
        let read_settings = ReadSettings::from_ctx(&pruning_ctx.ctx)?;
        let inverse = check_function(None, "not", &[], &[filter.clone()], &BUILTIN_FUNCTIONS)?;
        let inverse_range_index = RangeIndex::try_create(
            func_ctx.clone(),
            &inverse,
            schema,
            StatisticsOfColumns::default(),
        )?;
        Ok(Some(Self {
            func_ctx,
            expr,
            reader,
            read_settings,
            inverse_range_index,
        }))
    }

    /// The rows of one block that pass the filters.
    pub(crate) async fn block_rows(&self, block_meta: &BlockMeta) -> Result<BlockRows> {
        if self.passes_whole_block(block_meta) {
            return Ok(BlockRows::All);
        }
        let rows = self.evaluate(block_meta).await?;
        Ok(BlockRows::Rows(rows))
    }

    /// Whether the column statistics show that every row passes.
    fn passes_whole_block(&self, block_meta: &BlockMeta) -> bool {
        let input = RangeIndexInput::from_columns(&block_meta.col_stats);
        !self.inverse_range_index.should_keep(&input, None)
    }

    /// The passing rows, from the block data.
    async fn evaluate(&self, block_meta: &BlockMeta) -> Result<Bitmap> {
        let block = self
            .reader
            .read_by_meta(&self.read_settings, block_meta, &FuseStorageFormat::Parquet)
            .await?;
        let num_rows = block.num_rows();
        let evaluator = Evaluator::new(&block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let value = evaluator.run(&self.expr)?;
        let bitmap = match value.into_full_column(self.expr.data_type(), num_rows) {
            Column::Boolean(bitmap) => bitmap,
            Column::Nullable(nullable) => match nullable.column() {
                // NULL fails a filter.
                Column::Boolean(bitmap) => bitmap & nullable.validity(),
                _ => {
                    return Err(ErrorCode::Internal(
                        "vector TopN filters must evaluate to booleans",
                    ));
                }
            },
            _ => {
                return Err(ErrorCode::Internal(
                    "vector TopN filters must evaluate to booleans",
                ));
            }
        };
        if bitmap.len() != block_meta.row_count as usize {
            return Err(ErrorCode::Internal(format!(
                "vector TopN filters evaluated {} rows of a block of {}",
                bitmap.len(),
                block_meta.row_count
            )));
        }
        Ok(bitmap)
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::ColumnRef;
    use databend_common_expression::Constant;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::F32;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;

    use super::*;

    fn column(name: &str, data_type: NumberDataType) -> Expr<String> {
        Expr::ColumnRef(ColumnRef {
            span: None,
            id: name.to_string(),
            data_type: DataType::Number(data_type),
            display_name: name.to_string(),
        })
    }

    fn constant(scalar: NumberScalar) -> Expr<String> {
        Expr::Constant(Constant {
            span: None,
            data_type: DataType::Number(scalar.data_type()),
            scalar: Scalar::Number(scalar),
        })
    }

    fn call(name: &str, args: Vec<Expr<String>>) -> Result<Expr<String>> {
        check_function(None, name, &[], &args, &BUILTIN_FUNCTIONS)
    }

    /// Conjuncts go to the score or the column part; one reading both makes the
    /// filter unsplittable.
    #[test]
    fn test_split_score_conjuncts() -> Result<()> {
        let score = || column(VECTOR_SCORE_COL_NAME, NumberDataType::Float32);
        let id = || column("id", NumberDataType::Int32);
        let score_lt = call("lt", vec![
            score(),
            constant(NumberScalar::Float32(F32::from(0.5))),
        ])?;
        let id_gt = call("gt", vec![id(), constant(NumberScalar::Int32(1))])?;
        let id_lt = call("lt", vec![id(), constant(NumberScalar::Int32(9))])?;
        let names = |expr: &Option<Expr<String>>| -> Vec<String> {
            let mut names: Vec<String> = expr
                .iter()
                .flat_map(|expr| expr.column_refs().into_keys())
                .collect();
            names.sort();
            names
        };

        // Nested `and_filters` are flattened.
        let filter = call("and_filters", vec![
            call("and_filters", vec![score_lt.clone(), id_gt.clone()])?,
            id_lt.clone(),
        ])?;
        let ScoreConjuncts {
            score: score_part,
            columns: column_part,
        } = split_score_conjuncts(filter)?.unwrap();
        assert_eq!(names(&score_part), vec![VECTOR_SCORE_COL_NAME.to_string()]);
        assert_eq!(names(&column_part), vec!["id".to_string()]);
        assert!(matches!(
            &column_part,
            Some(Expr::FunctionCall(call))
                if call.function.signature.name == "and_filters" && call.args.len() == 2
        ));
        assert_eq!(score_part.unwrap().data_type(), &DataType::Boolean);
        assert_eq!(column_part.unwrap().data_type(), &DataType::Boolean);

        let ScoreConjuncts {
            score: score_part,
            columns: column_part,
        } = split_score_conjuncts(call("and", vec![score_lt.clone(), id_gt.clone()])?)?.unwrap();
        assert!(score_part.is_some() && column_part.is_some());

        // One side only.
        let ScoreConjuncts {
            score: score_part,
            columns: column_part,
        } = split_score_conjuncts(score_lt.clone())?.unwrap();
        assert!(score_part.is_some() && column_part.is_none());
        let ScoreConjuncts {
            score: score_part,
            columns: column_part,
        } = split_score_conjuncts(id_gt.clone())?.unwrap();
        assert!(score_part.is_none() && column_part.is_some());

        // Conjuncts reading both: `OR`, arithmetic, or one among others.
        let or = call("or", vec![score_lt.clone(), id_lt.clone()])?;
        assert!(split_score_conjuncts(or.clone())?.is_none());
        let sum = call("gt", vec![
            call("plus", vec![score(), id()])?,
            constant(NumberScalar::Float32(F32::from(3.0))),
        ])?;
        assert!(split_score_conjuncts(sum)?.is_none());
        assert!(split_score_conjuncts(call("and_filters", vec![id_gt, or])?)?.is_none());
        Ok(())
    }
}
