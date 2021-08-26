// Copyright 2020 Datafuse Labs.
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
use std::collections::HashMap;
use std::hash::Hasher;
use std::sync::Arc;
use std::time::Instant;

use bumpalo::Bump;
use byteorder::ByteOrder;
use byteorder::LittleEndian;
use common_datablocks::{DataBlock, HashMethodKeysU8};
use common_datablocks::HashMethod;
use common_datablocks::HashMethodKind;
use common_datavalues::arrays::BinaryArrayBuilder;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::aggregates::get_layout_offsets;
use common_functions::aggregates::StateAddr;
use common_infallible::RwLock;
use common_io::prelude::*;
use common_planners::Expression;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use futures::stream::StreamExt;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Processor;
use crate::pipelines::transforms::group_by::{Aggregator, PolymorphicKeysHelper};
use crate::common::HashTableKeyable;

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
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        tracing::debug!("execute...");
        let group_cols = self
            .group_exprs
            .iter()
            .map(|x| x.column_name())
            .collect::<Vec<_>>();
        let sample_block = DataBlock::empty_with_schema(self.schema_before_group_by.clone());
        let method = DataBlock::choose_hash_method(&sample_block, &group_cols)?;

        match method {
            HashMethodKind::Serializer(hash_method) => {
                let start = Instant::now();

                let mut stream = self.input.execute().await?;
                let aggr_exprs = &self.aggr_exprs;
                let schema = self.schema_before_group_by.clone();
                let aggregator = Aggregator::create(hash_method, aggr_exprs, schema)?;
                let groups_locker = aggregator.aggregate(group_cols, stream).await?;

                let delta = start.elapsed();
                tracing::debug!("Group by partial cost: {:?}", delta);

                let groups = groups_locker.read();
                let finalized_schema = self.schema.clone();
                aggregator.aggregate_finalized(&groups, finalized_schema)
            }
            HashMethodKind::KeysU8(hash_method) => {
                let start = Instant::now();

                let mut stream = self.input.execute().await?;
                let aggr_exprs = &self.aggr_exprs;
                let schema = self.schema_before_group_by.clone();
                let aggregator = Aggregator::create(hash_method, aggr_exprs, schema)?;
                let groups_locker = aggregator.aggregate(group_cols, stream).await?;

                let delta = start.elapsed();
                tracing::debug!("Group by partial cost: {:?}", delta);

                let groups = groups_locker.read();
                let finalized_schema = self.schema.clone();
                aggregator.aggregate_finalized(&groups, finalized_schema)
            }
            HashMethodKind::KeysU16(hash_method) => {
                let start = Instant::now();

                let mut stream = self.input.execute().await?;
                let aggr_exprs = &self.aggr_exprs;
                let schema = self.schema_before_group_by.clone();
                let aggregator = Aggregator::create(hash_method, aggr_exprs, schema)?;
                let groups_locker = aggregator.aggregate(group_cols, stream).await?;

                let delta = start.elapsed();
                tracing::debug!("Group by partial cost: {:?}", delta);

                let groups = groups_locker.read();
                let finalized_schema = self.schema.clone();
                aggregator.aggregate_finalized(&groups, finalized_schema)

            }
            HashMethodKind::KeysU32(hash_method) => {
                let start = Instant::now();

                let mut stream = self.input.execute().await?;
                let aggr_exprs = &self.aggr_exprs;
                let schema = self.schema_before_group_by.clone();
                let aggregator = Aggregator::create(hash_method, aggr_exprs, schema)?;
                let groups_locker = aggregator.aggregate(group_cols, stream).await?;

                let delta = start.elapsed();
                tracing::debug!("Group by partial cost: {:?}", delta);

                let groups = groups_locker.read();
                let finalized_schema = self.schema.clone();
                aggregator.aggregate_finalized(&groups, finalized_schema)
            }
            HashMethodKind::KeysU64(hash_method) => {
                let start = Instant::now();

                let mut stream = self.input.execute().await?;
                let aggr_exprs = &self.aggr_exprs;
                let schema = self.schema_before_group_by.clone();
                let aggregator = Aggregator::create(hash_method, aggr_exprs, schema)?;
                let groups_locker = aggregator.aggregate(group_cols, stream).await?;

                let delta = start.elapsed();
                tracing::debug!("Group by partial cost: {:?}", delta);

                let groups = groups_locker.read();
                let finalized_schema = self.schema.clone();
                aggregator.aggregate_finalized(&groups, finalized_schema)
            }
        }
    }
}

// impl GroupByPartialTransform {
//     #[inline]
//     async fn execute_impl<M>(&self, group_cols: Vec<String>, hash_method: M) -> Result<SendableDataBlockStream>
//         where M: HashMethod + PolymorphicKeysHelper<M>
//     {
//         let start = Instant::now();
//
//         let mut stream = self.input.execute().await?;
//         let aggr_exprs = &self.aggr_exprs;
//         let schema = self.schema_before_group_by.clone();
//         let aggregator = Aggregator::create(hash_method, aggr_exprs, schema)?;
//         let groups_locker = aggregator.aggregate(group_cols, stream).await?;
//
//         let delta = start.elapsed();
//         tracing::debug!("Group by partial cost: {:?}", delta);
//
//         let groups = groups_locker.read();
//         let finalized_schema = self.schema.clone();
//         aggregator.aggregate_finalized(&groups.0, finalized_schema)
//     }
// }
