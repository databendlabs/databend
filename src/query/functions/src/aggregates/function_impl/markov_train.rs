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

use std::alloc::Layout;
use std::collections::BTreeMap;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_base::obfuscator::CodePoint;
use databend_common_base::obfuscator::NGramHash;
use databend_common_base::obfuscator::consume;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::AggrStateType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::BuilderExt;
use databend_common_expression::types::DataType;
use databend_common_expression::types::F64;
use databend_common_expression::types::MapType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::types::ValueType;

use super::super::common::extract_number_param;
use super::AggregateRegistration;
use super::adaptors::*;

struct MarkovTrainBuilder;

impl MarkovTrainBuilder {
    fn register(registry: &mut AggregateRegistry) {
        DirectNameRoute::new(
            &["markov_train"],
            MarkovTrainBuilder::markov_train_arguments(),
            MarkovTrainBuilder::MARKOV_TRAIN_FEATURES,
            NullPolicy::Skip,
        )
        .then(MergeRoute::multi_arg(false, MarkovTrainBuilder::create))
        .then(MergeRoute::multi_arg(true, MarkovTrainBuilder::create))
        .then(PlainRoute::multi_arg(MarkovTrainBuilder::create))
        .then(IfRoute::multi_arg(MarkovTrainBuilder::create))
        .then(StateRoute::multi_arg(MarkovTrainBuilder::create))
        .then(DistinctRoute::multi_arg(MarkovTrainBuilder::create))
        .register(registry);
    }
}

inventory::submit! {
    AggregateRegistration {
        register: MarkovTrainBuilder::register,
    }
}

impl MarkovTrainBuilder {
    fn markov_train_arguments() -> ArgumentsPattern {
        ArgumentsPattern::fixed(vec![ArgumentPattern::exact(DataType::String)])
    }

    const MARKOV_TRAIN_FEATURES: AggregateFeatures = AggregateFeatures {
        is_decomposable: false,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "trains a markov model",
        definition: "markov_train([params])(expr)",
        example: "select markov_train(text) from t",
    };
}

#[derive(Debug, Clone)]
struct TrainParameters {
    order: usize,
    frequency_cutoff: u32,
    num_buckets_cutoff: usize,
    frequency_add: u32,
    frequency_desaturate: f64,
}

impl Default for TrainParameters {
    fn default() -> Self {
        Self {
            order: 5,
            frequency_cutoff: 0,
            num_buckets_cutoff: 0,
            frequency_add: 0,
            frequency_desaturate: 0.0,
        }
    }
}

#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
struct Histogram {
    buckets: BTreeMap<CodePoint, u32>,
    count_end: u32,
    #[borsh(skip)]
    total: Option<u32>,
}

impl Histogram {
    fn add(&mut self, code: Option<CodePoint>) {
        if let Some(code) = code {
            *self.buckets.entry(code).or_insert(0) += 1;
        } else {
            self.count_end += 1;
        }
    }

    fn merge(&mut self, rhs: &Self) {
        self.count_end += rhs.count_end;
        for (key, value) in rhs.buckets.iter() {
            *self.buckets.entry(*key).or_default() += *value;
        }
    }

    fn update_total(&mut self) {
        self.total = Some(self.buckets.values().sum());
    }

    fn frequency_cutoff(&mut self, limit: u32) {
        self.buckets.retain(|_, count| *count >= limit);
    }

    fn frequency_add(&mut self, n: u32) {
        if self.total.unwrap() == 0 {
            return;
        }
        self.count_end += n;
        for count in self.buckets.values_mut() {
            *count += n;
        }
    }

    fn frequency_desaturate(&mut self, p: f64) {
        let total = self.total.unwrap();
        if total == 0 {
            return;
        }

        let average = (total as f64 / self.buckets.len() as f64 * p) as u32;
        for count in self.buckets.values_mut() {
            *count = average + (*count as f64 * (1.0 - p)) as u32;
        }
    }
}

#[derive(Clone, Default, BorshSerialize, BorshDeserialize)]
pub struct AggregateMarkovTrainState {
    table: BTreeMap<NGramHash, Histogram>,
}

impl AggregateMarkovTrainState {
    fn state_description() -> AggregateStateDescription {
        AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<Self>())], vec![
            StateSerdeItem::Binary(None),
        ])
        .with_manual_drop(true)
    }

    fn consume(&mut self, order: usize, data: &[u8], code_points: &mut Vec<CodePoint>) {
        consume(
            order,
            data,
            |context_hash, code| {
                let histogram = self.table.entry(context_hash).or_default();
                histogram.add(code);
            },
            code_points,
        )
    }

    fn merge(&mut self, rhs: &mut Self) {
        use std::collections::btree_map::Entry;
        for (key, histogram) in std::mem::take(&mut rhs.table) {
            match self.table.entry(key) {
                Entry::Occupied(mut entry) => entry.get_mut().merge(&histogram),
                Entry::Vacant(entry) => {
                    entry.insert(histogram);
                }
            }
        }
    }

    fn finalize(&mut self, params: &TrainParameters) {
        for histogram in self.table.values_mut() {
            if params.num_buckets_cutoff > 0 && histogram.buckets.len() < params.num_buckets_cutoff
            {
                histogram.buckets.clear();
            }

            if params.frequency_cutoff > 0 {
                histogram.frequency_cutoff(params.frequency_cutoff);
            }

            histogram.update_total();

            if params.frequency_add > 0 {
                histogram.frequency_add(params.frequency_add);
            }

            if params.frequency_desaturate > 0.0 {
                histogram.frequency_desaturate(params.frequency_desaturate);
            }
        }
    }
}

struct MarkovTrainEval {
    params: TrainParameters,
}

impl MarkovTrainEval {
    fn append_model_result(
        &self,
        model: &AggregateMarkovTrainState,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        let ColumnBuilder::Array(array_builder) = builder else {
            unreachable!()
        };
        let ColumnBuilder::Tuple(builders) = &mut array_builder.builder else {
            unreachable!()
        };
        let [hash_builder, total_builder, end_builder, bucket_builder] = &mut builders[..] else {
            unreachable!()
        };
        let mut hash_builder = UInt32Type::downcast_builder(hash_builder);
        let mut total_builder = UInt32Type::downcast_builder(total_builder);
        let mut end_builder = UInt32Type::downcast_builder(end_builder);
        let mut bucket_builder =
            MapType::<UInt32Type, UInt32Type>::downcast_builder(bucket_builder);

        for (hash, histogram) in model.table.iter() {
            hash_builder.push_item(*hash);
            total_builder.push_item(histogram.total.unwrap());
            end_builder.push_item(histogram.count_end);
            for (code, weight) in histogram.buckets.iter() {
                bucket_builder.put_item((*code, *weight));
            }
            bucket_builder.commit_row();
        }
        array_builder.commit_row();
        Ok(())
    }
}

impl AggregateEval for MarkovTrainEval {
    fn init_state(&self, state: AggrState<'_>) {
        state.write(AggregateMarkovTrainState::default);
    }

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        let state = input.state.get::<AggregateMarkovTrainState>();
        let values = input.columns[0].downcast::<StringType>().unwrap();
        let mut code_points = Vec::new();
        match input.validity {
            Some(validity) => {
                for (value, valid) in values.iter().zip(validity.iter()) {
                    if valid {
                        state.consume(self.params.order, value.as_bytes(), &mut code_points);
                    }
                }
            }
            None => {
                for value in values.iter() {
                    state.consume(self.params.order, value.as_bytes(), &mut code_points);
                }
            }
        }
        Ok(())
    }

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        let values = input.columns[0].downcast::<StringType>().unwrap();
        let mut code_points = Vec::new();
        for (row, state) in input.states.iter().enumerate() {
            state.get::<AggregateMarkovTrainState>().consume(
                self.params.order,
                values.index(row).unwrap().as_bytes(),
                &mut code_points,
            );
        }
        Ok(())
    }

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        let values = input.columns[0].downcast::<StringType>().unwrap();
        let mut code_points = Vec::new();
        input.state.get::<AggregateMarkovTrainState>().consume(
            self.params.order,
            values.index(input.row).unwrap().as_bytes(),
            &mut code_points,
        );
        Ok(())
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        let binary_builder = input.builders[0].as_binary_mut().unwrap();
        for state in input.states.iter() {
            state
                .get::<AggregateMarkovTrainState>()
                .serialize(&mut binary_builder.data)?;
            binary_builder.commit_row();
        }
        Ok(())
    }

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        for (row, state) in input.states.iter().enumerate() {
            if input.filter.is_some_and(|filter| !filter.get(row).unwrap()) {
                continue;
            }
            let ScalarRef::Binary(mut data) = super::serialized_scalar_at(input.state, row, 0)
            else {
                unreachable!()
            };
            let mut rhs = AggregateMarkovTrainState::deserialize_reader(&mut data)?;
            state.get::<AggregateMarkovTrainState>().merge(&mut rhs);
        }
        Ok(())
    }

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        input
            .state
            .get::<AggregateMarkovTrainState>()
            .merge(input.rhs.get::<AggregateMarkovTrainState>());
        Ok(())
    }

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        let model = input.state.get::<AggregateMarkovTrainState>();
        model.finalize(&self.params);
        self.append_model_result(model, input.builder)
    }

    fn merge_result_read_only(&self, input: MergeResultInput<'_>) -> Result<()> {
        let mut model = input.state.get::<AggregateMarkovTrainState>().clone();
        model.finalize(&self.params);
        self.append_model_result(&model, input.builder)
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        unsafe { std::ptr::drop_in_place(state.get::<AggregateMarkovTrainState>()) };
    }
}

impl MarkovTrainBuilder {
    fn create(build: MultiArgBuildContext<'_, impl Combinator>) -> Result<AggregateCallRef> {
        if build.args_type()[0] != DataType::String {
            return Err(ErrorCode::BadDataValueType(format!(
                "{} does not support type '{:?}', must be string type",
                build.name(),
                build.args_type()[0]
            )));
        }

        let params = match build.params() {
            [] => TrainParameters::default(),
            [order] => {
                let order = extract_number_param::<u64>(order.clone())? as usize;
                TrainParameters {
                    order,
                    ..Default::default()
                }
            }
            [
                order,
                frequency_cutoff,
                num_buckets_cutoff,
                frequency_add,
                frequency_desaturate,
            ] => {
                let order = extract_number_param::<u64>(order.clone())? as usize;
                let frequency_cutoff = extract_number_param(frequency_cutoff.clone())?;
                let num_buckets_cutoff =
                    extract_number_param::<u64>(num_buckets_cutoff.clone())? as usize;
                let frequency_add = extract_number_param(frequency_add.clone())?;
                let frequency_desaturate =
                    extract_number_param::<F64>(frequency_desaturate.clone())?.0;
                TrainParameters {
                    order,
                    frequency_cutoff,
                    num_buckets_cutoff,
                    frequency_add,
                    frequency_desaturate,
                }
            }
            params => {
                return Err(ErrorCode::NumberArgumentsNotMatch(format!(
                    "{} expect to have 0, 1 or 5 params, but got {}",
                    build.name(),
                    params.len()
                )));
            }
        };

        build.create_multi_arg_or_null(
            DataType::Array(Box::new(DataType::Tuple(vec![
                UInt32Type::data_type(),
                UInt32Type::data_type(),
                UInt32Type::data_type(),
                MapType::<UInt32Type, UInt32Type>::data_type(),
            ])))
            .wrap_nullable(),
            AggregateMarkovTrainState::state_description(),
            MarkovTrainEval { params },
        )
    }
}
