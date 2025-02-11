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

use databend_common_base::obfuscator::generate;
use databend_common_base::obfuscator::CodePoint;
use databend_common_base::obfuscator::Histogram;
use databend_common_base::obfuscator::NGramHash;
use databend_common_base::obfuscator::Table;
use databend_common_column::buffer::Buffer;
use databend_common_expression::types::map::KvPair;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::ArrayColumn;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::GenericType;
use databend_common_expression::types::MapType;
use databend_common_expression::types::NumberColumn;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::types::ValueType;
use databend_common_expression::Column;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Value;

struct ColumnHistogram {
    total: u32,
    count_end: u32,
    codes: Buffer<u32>,
    weights: Buffer<u32>,
}

impl Histogram<'_> for ColumnHistogram {
    fn sample(&self, random: u64, end_multiplier: f64) -> Option<CodePoint> {
        let range = self.total + (self.count_end as f64 * end_multiplier) as u32;
        if range == 0 {
            return None;
        }

        let mut random = random as u32 % range;
        self.codes
            .iter()
            .zip(self.weights.iter())
            .find(|(_, weighted)| {
                if random <= **weighted {
                    true
                } else {
                    random -= **weighted;
                    false
                }
            })
            .map(|(code, _)| *code)
    }

    fn is_empty(&self) -> bool {
        self.total == 0 && self.count_end == 0
    }
}

#[derive(Clone)]
struct ColumnTable {
    hash: Buffer<NGramHash>,
    total: Buffer<u32>,
    count_end: Buffer<u32>,
    buckets: ArrayColumn<KvPair<UInt32Type, UInt32Type>>,
}

impl Table<'_, ColumnHistogram> for ColumnTable {
    fn get(&self, context_hash: &NGramHash) -> Option<ColumnHistogram> {
        let row = self.hash.binary_search(context_hash).ok()?;
        let bucket = self.buckets.index(row).unwrap();
        Some(ColumnHistogram {
            total: self.total[row],
            count_end: self.count_end[row],
            codes: bucket.keys,
            weights: bucket.values,
        })
    }
}

impl ColumnTable {
    fn try_new(column: Column) -> Option<Self> {
        let Column::Tuple(mut tuple) = column else {
            return None;
        };
        if tuple.len() != 4 {
            return None;
        }
        let buckets = tuple.pop()?;
        let buckets = MapType::<UInt32Type, UInt32Type>::try_downcast_column(&buckets)?;
        let Some(Column::Number(NumberColumn::UInt32(count_end))) = tuple.pop() else {
            return None;
        };
        let Some(Column::Number(NumberColumn::UInt32(total))) = tuple.pop() else {
            return None;
        };
        let Some(Column::Number(NumberColumn::UInt32(hash))) = tuple.pop() else {
            return None;
        };

        Some(ColumnTable {
            hash,
            total,
            count_end,
            buckets,
        })
    }
}

#[derive(Clone, Copy, Debug, serde::Deserialize)]
struct MarkovModelParameters {
    order: usize,
    seed: u64,
    sliding_window_size: usize,
}

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_3_arg::<ArrayType<GenericType<0>>,StringType,StringType,StringType,_,_>(
        "markov_generate",
         |_,_,_,_|FunctionDomain::MayThrow,
         move |model_arg, params_arg, determinator, ctx| {
            let generics = ctx.generics.to_vec();

            let input_all_scalars =
                model_arg.is_scalar() && params_arg.is_scalar() && determinator.is_scalar();
            let process_rows = if input_all_scalars { 1 } else { ctx.num_rows };

            let mut output = StringType::create_builder(process_rows, &generics);

            let table = model_arg
                .as_scalar()
                .and_then(|model| ColumnTable::try_new(model.clone()));
            let params = params_arg
                .as_scalar()
                .and_then(|params| serde_json::from_str::<MarkovModelParameters>(params).ok());

            let mut code_points = Vec::new();
            for index in 0..process_rows {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(index) {
                        output.commit_row();
                        continue;
                    }
                }

                let table = match &table {
                    Some(table) => table.clone(),
                    None => {
                        let model = unsafe { model_arg.index_unchecked(index) };
                        let Some(table) = ColumnTable::try_new(model.clone()) else {
                            let expect = DataType::Array(Box::new(DataType::Tuple(vec![
                                UInt32Type::data_type(),                        // hash
                                UInt32Type::data_type(),                        // total
                                UInt32Type::data_type(),                        // count_end
                                MapType::<UInt32Type, UInt32Type>::data_type(), // buckets
                            ])));
                            let got = DataType::Array(Box::new(model.data_type()));
                            ctx.set_error(
                                output.len(),
                                format!("invalid model expect {expect:?}, but got {got:?}"),
                            );
                            output.commit_row();
                            continue;
                        };
                        table
                    }
                };

                let MarkovModelParameters {
                    order,
                    seed,
                    sliding_window_size,
                } = match params {
                    Some(params) => params,
                    None => {
                        let params = unsafe { params_arg.index_unchecked(index) };
                        let Ok(params) = serde_json::from_str::<MarkovModelParameters>(params) else {
                            ctx.set_error(output.len(), "invalid params");
                            output.commit_row();
                            continue;
                        };
                        if params.order == 0 {
                            ctx.set_error(output.len(), "invalid order");
                            output.commit_row();
                            continue;
                        }
                        if params.sliding_window_size == 0 {
                            ctx.set_error(output.len(), "invalid sliding_window_size");
                            output.commit_row();
                            continue;
                        }
                        params
                    }
                };

                let determinator = unsafe { determinator.index_unchecked(index) };
                let desired_size = determinator.chars().count();
                let mut writer = vec![0; determinator.len() * 2];

                match generate(
                    &table,
                    order,
                    seed,
                    &mut writer,
                    desired_size,
                    sliding_window_size,
                    determinator.as_bytes(),
                    &mut code_points,
                ) {
                    Some(n) => {
                        writer.truncate(n);
                        output.put_and_commit(std::str::from_utf8(&writer).unwrap());
                    }
                    None => {
                        ctx.set_error(output.len(), "logical error in markov model");
                        output.commit_row();
                    }
                }
            }
            if input_all_scalars {
                Value::Scalar(StringType::build_scalar(output))
            } else {
                Value::Column(StringType::build_column(output))
            }
        }
    );
}
