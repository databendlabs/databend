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

use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnRef;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_functions::aggregates::eval_aggr;
use databend_common_sql::evaluator::BlockOperator;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::table::ClusterType;

use crate::FuseTable;

#[derive(Default, Clone)]
pub struct ClusterStatisticsBuilder {
    cluster_key_id: u32,
    cluster_key_index: Vec<usize>,

    extra_key_num: usize,
    operators: Vec<BlockOperator>,
    func_ctx: FunctionContext,
}

impl ClusterStatisticsBuilder {
    pub fn try_create(
        table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        source_schema: &TableSchemaRef,
    ) -> Result<Arc<Self>> {
        let cluster_type = table.cluster_type();
        if cluster_type.is_none_or(|v| v == ClusterType::Hilbert) {
            return Ok(Default::default());
        }

        let input_schema: Arc<DataSchema> = DataSchema::from(source_schema).into();
        let input_field_len = input_schema.fields.len();

        let cluster_keys = table.linear_cluster_keys(ctx.clone());
        let mut cluster_key_index = Vec::with_capacity(cluster_keys.len());
        let mut extra_key_num = 0;

        let mut exprs = Vec::with_capacity(cluster_keys.len());
        for remote_expr in &cluster_keys {
            let expr = remote_expr
                .as_expr(&BUILTIN_FUNCTIONS)
                .project_column_ref(|name| input_schema.index_of(name))?;
            let index = match &expr {
                Expr::ColumnRef(ColumnRef { id, .. }) => *id,
                _ => {
                    exprs.push(expr);
                    let offset = input_field_len + extra_key_num;
                    extra_key_num += 1;
                    offset
                }
            };
            cluster_key_index.push(index);
        }

        let operators = if exprs.is_empty() {
            vec![]
        } else {
            vec![BlockOperator::Map {
                exprs,
                projections: None,
            }]
        };
        Ok(Arc::new(Self {
            cluster_key_id: table.cluster_key_id().unwrap(),
            cluster_key_index,
            extra_key_num,
            func_ctx: ctx.get_function_context()?,
            operators,
        }))
    }

    pub fn cluster_key_id(&self) -> u32 {
        self.cluster_key_id
    }

    /// Evaluate the cluster-key tuple column for a block: apply any extra-key map operators, then
    /// gather the cluster-key columns into a single `Column::Tuple`. The block is cloned so the
    /// caller's columns (including any extra key columns) are untouched.
    pub fn eval_cluster_tuple(&self, block: &DataBlock) -> Result<Column> {
        let block = self
            .operators
            .iter()
            .try_fold(block.clone(), |input, op| op.execute(&self.func_ctx, input))?;
        let cols = self
            .cluster_key_index
            .iter()
            .map(|&i| block.get_by_offset(i).to_column())
            .collect();
        Ok(Column::Tuple(cols))
    }
}

pub struct ClusterStatisticsState {
    level: i32,
    mins: Vec<Scalar>,
    maxs: Vec<Scalar>,

    builder: Arc<ClusterStatisticsBuilder>,
}

impl ClusterStatisticsState {
    pub fn new(builder: Arc<ClusterStatisticsBuilder>) -> Self {
        Self {
            level: 0,
            mins: vec![],
            maxs: vec![],
            builder,
        }
    }

    pub fn add_block(&mut self, input: DataBlock) -> Result<DataBlock> {
        if self.builder.cluster_key_index.is_empty() {
            return Ok(input);
        }

        let num_rows = input.num_rows();
        let mut block = self
            .builder
            .operators
            .iter()
            .try_fold(input, |input, op| op.execute(&self.builder.func_ctx, input))?;
        let cols = self
            .builder
            .cluster_key_index
            .iter()
            .map(|&i| block.get_by_offset(i).to_column())
            .collect();
        let entries = [Column::Tuple(cols).into()];
        let (min, _) = eval_aggr("min", vec![], &entries, num_rows, vec![])?;
        let (max, _) = eval_aggr("max", vec![], &entries, num_rows, vec![])?;
        assert_eq!(min.len(), 1);
        assert_eq!(max.len(), 1);
        self.mins.push(min.index(0).unwrap().to_owned());
        self.maxs.push(max.index(0).unwrap().to_owned());
        block.pop_columns(self.builder.extra_key_num);
        Ok(block)
    }

    pub fn finalize(self, perfect: bool) -> Result<Option<ClusterStatistics>> {
        if self.builder.cluster_key_index.is_empty() {
            return Ok(None);
        }

        let min = self
            .mins
            .into_iter()
            .min_by(|x, y| x.as_ref().cmp(&y.as_ref()))
            .unwrap()
            .as_tuple()
            .unwrap()
            .clone();
        let max = self
            .maxs
            .into_iter()
            .max_by(|x, y| x.as_ref().cmp(&y.as_ref()))
            .unwrap()
            .as_tuple()
            .unwrap()
            .clone();

        let level = if min == max && perfect {
            -1
        } else {
            self.level
        };

        Ok(Some(ClusterStatistics {
            max,
            min,
            level,
            cluster_key_id: self.builder.cluster_key_id,
            pages: None,
        }))
    }
}

/// Tracks the cluster-key min for each `granule_rows`-sized granule, used to build the sparse
/// page index. Rows are fed in arbitrary-sized blocks; this accumulates a running per-granule min
/// (over the cluster-key tuple) and emits one min Scalar per completed granule. Granule boundaries
/// here must line up with the page flushes the writer performs at the same row stride.
pub struct GranuleMinState {
    granule_rows: usize,
    builder: Arc<ClusterStatisticsBuilder>,
    /// Rows already absorbed into the current, not-yet-complete granule.
    current_rows: usize,
    /// Running min (cluster-key tuple Scalar) of the current granule; None while empty.
    current_min: Option<Scalar>,
    /// Completed per-granule mins, in granule order.
    granule_mins: Vec<Scalar>,
}

impl GranuleMinState {
    pub fn new(builder: Arc<ClusterStatisticsBuilder>, granule_rows: usize) -> Self {
        Self {
            granule_rows,
            builder,
            current_rows: 0,
            current_min: None,
            granule_mins: Vec::new(),
        }
    }

    pub fn is_active(&self) -> bool {
        !self.builder.cluster_key_index.is_empty() && self.granule_rows > 0
    }

    /// Absorb one block's rows, folding their cluster-key min into the current granule and
    /// emitting completed-granule mins as boundaries are crossed. The block must be the same one
    /// (pre extra-key pop) handed to the writer, so row counts stay aligned with the page flushes.
    pub fn add_block(&mut self, block: &DataBlock) -> Result<()> {
        if !self.is_active() {
            return Ok(());
        }
        let tuple = self.builder.eval_cluster_tuple(block)?;
        let num_rows = block.num_rows();
        let mut offset = 0;
        while offset < num_rows {
            let remaining_in_granule = self.granule_rows - self.current_rows;
            let take = remaining_in_granule.min(num_rows - offset);
            let slice = tuple.slice(offset..offset + take);
            let min = column_min(&slice)?;
            self.current_min = Some(match self.current_min.take() {
                Some(prev) if prev.as_ref().cmp(&min.as_ref()).is_le() => prev,
                _ => min,
            });
            self.current_rows += take;
            offset += take;
            if self.current_rows == self.granule_rows {
                self.granule_mins.push(self.current_min.take().unwrap());
                self.current_rows = 0;
            }
        }
        Ok(())
    }

    /// Whether the per-granule mins (including the trailing partial granule) are non-decreasing.
    /// Sparse-index pruning uses `mins[i+1]` as granule `i`'s upper bound, which is only sound when
    /// the block is cluster-sorted. Non-monotonic mins mean the block was not sorted (e.g. a fresh
    /// insert), so the caller must skip emitting the index.
    pub fn is_monotonic(&self) -> bool {
        scalars_non_decreasing(self.granule_mins.iter().chain(self.current_min.as_ref()))
    }

    /// Close out a trailing partial granule (the block's final rows that didn't fill a granule)
    /// and return all granule mins. A partial last granule still gets a min, matching the final
    /// page boundary the writer emits at end-of-block.
    pub fn finalize(mut self) -> Vec<Scalar> {
        if self.current_rows > 0 {
            if let Some(min) = self.current_min.take() {
                self.granule_mins.push(min);
            }
        }
        self.granule_mins
    }
}

fn column_min(col: &Column) -> Result<Scalar> {
    let entries = [col.clone().into()];
    let (min, _) = eval_aggr("min", vec![], &entries, col.len(), vec![])?;
    Ok(min.index(0).unwrap().to_owned())
}

/// True when `scalars` is non-decreasing under the natural scalar ordering. Used to validate that a
/// block's per-granule cluster-key mins are monotonic before emitting a sparse page index.
fn scalars_non_decreasing<'a>(scalars: impl Iterator<Item = &'a Scalar>) -> bool {
    let mut prev: Option<&Scalar> = None;
    for cur in scalars {
        if let Some(prev) = prev {
            if prev.as_ref().cmp(&cur.as_ref()).is_gt() {
                return false;
            }
        }
        prev = Some(cur);
    }
    true
}

#[cfg(test)]
mod tests {
    use databend_common_expression::Scalar;
    use databend_common_expression::types::number::NumberScalar;

    use super::scalars_non_decreasing;

    fn i64s(vals: &[i64]) -> Vec<Scalar> {
        vals.iter()
            .map(|v| Scalar::Number(NumberScalar::Int64(*v)))
            .collect()
    }

    #[test]
    fn test_scalars_non_decreasing() {
        assert!(scalars_non_decreasing(i64s(&[]).iter()));
        assert!(scalars_non_decreasing(i64s(&[5]).iter()));
        assert!(scalars_non_decreasing(i64s(&[1, 2, 2, 3, 100]).iter()));
        // Equal values are allowed (non-strict).
        assert!(scalars_non_decreasing(i64s(&[7, 7, 7]).iter()));
        // A single descending step breaks monotonicity.
        assert!(!scalars_non_decreasing(i64s(&[1, 2, 1]).iter()));
        assert!(!scalars_non_decreasing(i64s(&[5, 4]).iter()));
        assert!(!scalars_non_decreasing(i64s(&[1, 2, 3, 2, 4]).iter()));
    }
}
