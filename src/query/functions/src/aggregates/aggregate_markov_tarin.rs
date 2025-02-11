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
use std::fmt;
use std::sync::Arc;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_base::obfuscator::consume;
use databend_common_base::obfuscator::CodePoint;
use databend_common_base::obfuscator::NGramHash;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::array::ArrayColumnBuilder;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::MapType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::F64;
use databend_common_expression::AggrState;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::AggrStateType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::InputColumns;

use super::aggregate_function_factory::AggregateFunctionDescription;
use super::assert_unary_arguments;
use super::borsh_deserialize_state;
use super::borsh_serialize_state;
use super::extract_number_param;
use super::AggregateFunction;

pub struct MarkovTarin {
    display_name: String,
    params: TrainParameters,
}

impl AggregateFunction for MarkovTarin {
    fn name(&self) -> &str {
        "AggregateMarkovTarinFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(DataType::Array(Box::new(DataType::Tuple(vec![
            UInt32Type::data_type(),                        // hash
            UInt32Type::data_type(),                        // total
            UInt32Type::data_type(),                        // count_end
            MapType::<UInt32Type, UInt32Type>::data_type(), // buckets
        ]))))
    }

    fn init_state(&self, place: AggrState) {
        place.write(MarkovModel::default);
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        registry.register(AggrStateType::Custom(Layout::new::<MarkovModel>()));
    }

    fn accumulate(
        &self,
        place: AggrState,
        columns: InputColumns,
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let model: &mut MarkovModel = place.get();
        let col = StringType::try_downcast_column(&columns[0]).unwrap();

        let mut code_points = Vec::new();
        if let Some(validity) = validity {
            for s in col
                .iter()
                .zip(validity.iter())
                .filter_map(|(s, b)| b.then_some(s))
            {
                model.consume(self.params.order, s.as_bytes(), &mut code_points);
            }
        } else {
            for s in col.iter() {
                model.consume(self.params.order, s.as_bytes(), &mut code_points);
            }
        }
        Ok(())
    }

    fn accumulate_row(&self, place: AggrState, columns: InputColumns, row: usize) -> Result<()> {
        let model = place.get::<MarkovModel>();
        let col = StringType::try_downcast_column(&columns[0]).unwrap();
        let mut code_points = Vec::new();
        model.consume(
            self.params.order,
            col.index(row).unwrap().as_bytes(),
            &mut code_points,
        );
        Ok(())
    }

    fn serialize(&self, place: AggrState, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<MarkovModel>();
        borsh_serialize_state(writer, state)
    }

    fn merge(&self, place: AggrState, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<MarkovModel>();
        let mut rhs = borsh_deserialize_state::<MarkovModel>(reader)?;
        state.merge(&mut rhs);

        Ok(())
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        let state = place.get::<MarkovModel>();
        let other = rhs.get::<MarkovModel>();

        state.merge(other);
        Ok(())
    }

    fn merge_result(&self, place: AggrState, builder: &mut ColumnBuilder) -> Result<()> {
        let model = place.get::<MarkovModel>();
        model.finalize(&self.params);

        let ColumnBuilder::Array(box array_builder) = builder else {
            unreachable!()
        };
        let ColumnBuilder::Tuple(builders) = &mut array_builder.builder else {
            unreachable!()
        };
        let [hash_builder, total_builder, end_builder, ColumnBuilder::Map(box bucket_builder)] =
            &mut builders[..]
        else {
            unreachable!()
        };
        let hash_builder = UInt32Type::try_downcast_builder(hash_builder).unwrap();
        let total_builder = UInt32Type::try_downcast_builder(total_builder).unwrap();
        let end_builder = UInt32Type::try_downcast_builder(end_builder).unwrap();

        let ArrayColumnBuilder::<AnyType> {
            builder: ColumnBuilder::Tuple(kv),
            offsets: bucket_offsets,
        } = bucket_builder
        else {
            unreachable!()
        };
        let [keys, values] = &mut kv[..] else {
            unreachable!()
        };
        let keys = UInt32Type::try_downcast_builder(keys).unwrap();
        let values = UInt32Type::try_downcast_builder(values).unwrap();

        for (hash, histogram) in model.table.iter() {
            hash_builder.push(*hash);
            total_builder.push(histogram.total.unwrap());
            end_builder.push(histogram.count_end);
            for (c, w) in histogram.buckets.iter() {
                keys.push(*c);
                values.push(*w);
            }
            bucket_offsets.push(keys.len() as u64);
        }
        array_builder.commit_row();
        Ok(())
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: AggrState) {
        let state = place.get::<MarkovModel>();
        std::ptr::drop_in_place(state);
    }
}

impl fmt::Display for MarkovTarin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

#[derive(Debug, Clone)]
struct TrainParameters {
    pub order: usize,

    // We can consider separating the process of modifying the model, so we don't need these parameters here
    pub frequency_cutoff: u32,
    pub num_buckets_cutoff: usize,
    pub frequency_add: u32,
    pub frequency_desaturate: f64,
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

    fn marge(&mut self, rhs: &Self) {
        self.count_end += rhs.count_end;
        for (k, v) in rhs.buckets.iter() {
            *self.buckets.entry(*k).or_default() += *v
        }
    }

    fn update_total(&mut self) {
        self.total = Some(self.buckets.values().sum())
    }
}

#[derive(Default, BorshSerialize, BorshDeserialize)]
struct MarkovModel {
    table: BTreeMap<NGramHash, Histogram>,
}

impl MarkovModel {
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

    fn merge(&mut self, rhs: &mut Self) {
        use std::collections::btree_map::Entry;
        for (k, v) in std::mem::take(&mut rhs.table).into_iter() {
            match self.table.entry(k) {
                Entry::Occupied(mut occupied) => occupied.get_mut().marge(&v),
                Entry::Vacant(vacant) => {
                    vacant.insert(v);
                }
            }
        }
    }
}

pub fn aggregate_markov_train_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(|display_name, params, arguments| {
        assert_unary_arguments(display_name, arguments.len())?;

        let params = match &params[..] {
            [] => TrainParameters::default(),
            [order] => {
                let order = extract_number_param::<u64>(order.clone())? as usize;
                TrainParameters {
                    order,
                    ..Default::default()
                }
            }
            [order, frequency_cutoff, num_buckets_cutoff, frequency_add, frequency_desaturate] => {
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
                    display_name,
                    params.len()
                )))
            }
        };

        Ok(Arc::new(MarkovTarin {
            display_name: display_name.to_string(),
            params,
        }))
    }))
}
