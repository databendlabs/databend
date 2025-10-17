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
use databend_common_functions::aggregates::eval_aggr;
use databend_common_functions::BUILTIN_FUNCTIONS;
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
            cluster_key_id: table.cluster_key_meta.as_ref().unwrap().0,
            cluster_key_index,
            extra_key_num,
            func_ctx: ctx.get_function_context()?,
            operators,
        }))
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
