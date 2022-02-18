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

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use common_datablocks::DataBlock;
use common_datablocks::HashMethod;
use common_datablocks::HashMethodKind;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_planners::Expression;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Processor;
use crate::pipelines::transforms::group_by::Aggregator;
use crate::pipelines::transforms::group_by::AggregatorParams;
use crate::pipelines::transforms::group_by::PolymorphicKeysHelper;

pub struct GroupByPartialTransform {
    aggr_exprs: Vec<Expression>,
    group_exprs: Vec<Expression>,

    schema: DataSchemaRef,
    schema_before_group_by: DataSchemaRef,
    input: Arc<dyn Processor>,
}

impl GroupByPartialTransform {
    pub fn create(
        schema: DataSchemaRef,
        schema_before_group_by: DataSchemaRef,
        aggr_exprs: Vec<Expression>,
        group_exprs: Vec<Expression>,
    ) -> Self {
        Self {
            aggr_exprs,
            group_exprs,
            schema,
            schema_before_group_by,
            input: Arc::new(EmptyProcessor::create()),
        }
    }

    fn extract_group_columns(&self) -> Vec<String> {
        self.group_exprs
            .iter()
            .map(|x| x.column_name())
            .collect::<Vec<_>>()
    }

    #[inline]
    async fn aggregate<Method: HashMethod + PolymorphicKeysHelper<Method>>(
        &self,
        method: Method,
        group_cols: Vec<String>,
    ) -> Result<SendableDataBlockStream> {
        let start = Instant::now();

        let stream = self.input.execute().await?;
        let aggr_exprs = &self.aggr_exprs;
        let aggregator_params = AggregatorParams::try_create(
            &self.schema,
            &self.schema_before_group_by,
            aggr_exprs,
            &group_cols,
        )?;

        let aggregator = Aggregator::create(method, aggregator_params);
        let state = aggregator.aggregate(group_cols, stream).await?;

        let delta = start.elapsed();
        tracing::debug!("Group by partial cost: {:?}", delta);

        let finalized_schema = self.schema.clone();
        aggregator.aggregate_finalized(&state, finalized_schema)
    }
}

#[async_trait::async_trait]
impl Processor for GroupByPartialTransform {
    fn name(&self) -> &str {
        "GroupByPartialTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn Processor>) -> Result<()> {
        self.input = input;
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn Processor>> {
        vec![self.input.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Create hash group based on row index and apply the function with vector.
    /// For example:
    /// row_idx, A
    /// 0, 1
    /// 1, 2
    /// 2, 3
    /// 3, 4
    /// 4, 5
    ///
    /// grouping by [A%3]
    /// 1.1)
    /// For each row, allocate the state if key not exists in the map, and apply accumulate_row row by row
    ///  row_idx, A % 3 -> state place
    ///  0, 1 -> state1
    ///  1, 2 -> state2
    ///  2, 3 -> state3
    ///  3, 1 -> state1
    ///  4, 2 -> state2
    /// 1.2)  serialize the state to the output block
    #[tracing::instrument(level = "debug", name = "group_by_partial_execute", skip(self))]
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        tracing::debug!("execute...");
        let group_cols = self.extract_group_columns();
        let sample_block = DataBlock::empty_with_schema(self.schema_before_group_by.clone());
        let hash_method = DataBlock::choose_hash_method(&sample_block, &group_cols)?;

        match hash_method {
            HashMethodKind::KeysU8(method) => self.aggregate(method, group_cols).await,
            HashMethodKind::KeysU16(method) => self.aggregate(method, group_cols).await,
            HashMethodKind::KeysU32(method) => self.aggregate(method, group_cols).await,
            HashMethodKind::KeysU64(method) => self.aggregate(method, group_cols).await,
            HashMethodKind::Serializer(method) => self.aggregate(method, group_cols).await,
        }
    }
}
