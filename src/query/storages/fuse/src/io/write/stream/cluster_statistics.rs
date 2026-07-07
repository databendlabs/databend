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
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_functions::aggregates::eval_aggr;
use databend_common_sql::evaluator::BlockOperator;
use databend_storages_common_table_meta::meta::ClusterStatistics;

use crate::FuseTable;

#[derive(Default, Clone)]
pub struct ClusterStatisticsBuilder {
    cluster_key_id: u32,
    cluster_key_index: Vec<usize>,
    vector_cluster_id_offset: Option<usize>,

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
        let input_schema: Arc<DataSchema> = DataSchema::from(source_schema).into();

        let cluster_stats_gen = table.get_cluster_stats_gen(
            ctx.clone(),
            0,
            table.get_block_thresholds(),
            input_schema,
        )?;
        let vector_cluster_id_offset = cluster_stats_gen
            .vector_operator
            .as_ref()
            .map(|vector_operator| vector_operator.vector_cluster_id_offset);
        let extra_key_num = cluster_stats_gen.operator_extra_key_num();
        let cluster_key_index = cluster_stats_gen
            .cluster_key_index
            .into_iter()
            .filter(|index| Some(*index) != vector_cluster_id_offset)
            .collect::<Vec<_>>();

        if cluster_key_index.is_empty() && vector_cluster_id_offset.is_none() {
            return Ok(Default::default());
        }
        Ok(Arc::new(Self {
            cluster_key_id: table.cluster_key_id().unwrap(),
            cluster_key_index,
            vector_cluster_id_offset,
            extra_key_num,
            func_ctx: cluster_stats_gen.func_ctx,
            operators: cluster_stats_gen.operators,
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
        if !self.has_cluster_key() {
            return Ok(input);
        }

        let num_rows = input.num_rows();
        let mut block = self
            .builder
            .operators
            .iter()
            .try_fold(input, |input, op| op.execute(&self.builder.func_ctx, input))?;

        let (min, max) = self.scalar_cluster_min_max(&block, num_rows)?;
        self.mins.push(min);
        self.maxs.push(max);

        block.pop_columns(self.builder.extra_key_num);
        Ok(block)
    }

    pub fn finalize(self, perfect: bool) -> Result<Option<ClusterStatistics>> {
        if !self.has_cluster_key() {
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

        let level = if self.builder.vector_cluster_id_offset.is_none() && min == max && perfect {
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

    fn has_cluster_key(&self) -> bool {
        !self.builder.cluster_key_index.is_empty()
            || self.builder.vector_cluster_id_offset.is_some()
    }

    fn scalar_cluster_min_max(
        &self,
        block: &DataBlock,
        num_rows: usize,
    ) -> Result<(Scalar, Scalar)> {
        if self.builder.cluster_key_index.is_empty() {
            return Ok((Scalar::Tuple(vec![]), Scalar::Tuple(vec![])));
        }
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
        Ok((
            min.index(0).unwrap().to_owned(),
            max.index(0).unwrap().to_owned(),
        ))
    }
}
