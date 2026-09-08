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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::physical_plans::AggregateShuffleMode;
use crate::pipelines::processors::transforms::aggregator::AggregateBucketPartitionStream;
use crate::pipelines::processors::transforms::aggregator::AggregateExchangeDataCodec;
use crate::pipelines::processors::transforms::aggregator::AggregateRowPartitionStream;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::servers::flight::v1::exchange::ExchangeInjector;
use crate::servers::flight::v1::exchange::GlobalShuffleExchange;
use crate::servers::flight::v1::network::ExchangeDataCodec;
use crate::servers::flight::v1::partition::PartitionStream;
use crate::sessions::QueryContext;

pub struct AggregateInjector {
    aggregator_params: Arc<AggregatorParams>,
    shuffle_mode: AggregateShuffleMode,
}

impl AggregateInjector {
    pub fn create(
        params: Arc<AggregatorParams>,
        shuffle_mode: AggregateShuffleMode,
    ) -> Arc<dyn ExchangeInjector> {
        Arc::new(AggregateInjector {
            aggregator_params: params,
            shuffle_mode,
        })
    }
}

impl ExchangeInjector for AggregateInjector {
    fn exchange_data_codec(&self) -> Arc<dyn ExchangeDataCodec> {
        AggregateExchangeDataCodec::create(self.aggregator_params.clone())
    }

    fn partition_streams(
        &self,
        _: &Arc<QueryContext>,
        exchange: &GlobalShuffleExchange,
        streams: usize,
        _: usize,
        _: usize,
    ) -> Result<Vec<Box<dyn PartitionStream>>> {
        let partitions = exchange
            .destination_channels
            .iter()
            .map(|(_, channels)| channels.len())
            .sum();
        let expected_partitions = self.shuffle_mode.parallelism();
        if partitions != expected_partitions {
            return Err(ErrorCode::Internal(format!(
                "Aggregate shuffle has {partitions} destination lanes, expected {expected_partitions}",
            )));
        }
        Ok((0..streams)
            .map(|_| match self.shuffle_mode {
                AggregateShuffleMode::Row(_) => Box::new(AggregateRowPartitionStream {
                    buckets: partitions,
                    aggregate_params: self.aggregator_params.clone(),
                }) as Box<dyn PartitionStream>,
                AggregateShuffleMode::Bucket(_) => Box::new(AggregateBucketPartitionStream {
                    buckets: partitions,
                }) as Box<dyn PartitionStream>,
            })
            .collect())
    }
}
