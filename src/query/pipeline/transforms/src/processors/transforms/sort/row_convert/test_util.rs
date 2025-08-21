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

use arrow_array::ArrayRef;
use arrow_ord::sort::LexicographicalComparator;
use arrow_ord::sort::SortColumn;
use arrow_schema::SortOptions;
use databend_common_expression::types::*;
use databend_common_expression::BlockEntry;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::SortField;
use proptest::prelude::*;

pub fn print_row(entries: &[BlockEntry], row: usize) -> String {
    let t: Vec<_> = entries
        .iter()
        .map(|x| format!("{:?}", x.index(row).unwrap()))
        .collect();
    t.join(",")
}

pub fn print_options(cols: &[(bool, bool)]) -> String {
    let t: Vec<_> = cols
        .iter()
        .map(|(asc, null_first)| {
            format!(
                "({}, {})",
                if *asc { "ASC" } else { "DESC" },
                if *null_first {
                    "NULL_FIRST"
                } else {
                    "NULL_LAST"
                }
            )
        })
        .collect();
    t.join(",")
}

pub fn string_strategy() -> impl Strategy<Value = String> {
    prop::string::string_regex("[a-zA-Z0-9]{0,100}").unwrap()
}

pub fn string_column_strategy(len: usize, valid_percent: f64) -> impl Strategy<Value = BlockEntry> {
    prop_oneof![
        1 =>
        string_strategy().prop_map(move |value| {
            BlockEntry::Const(Scalar::String(value), StringType::data_type(), len)
        }),
        1 =>
        prop::collection::vec(
            string_strategy(),
            len
        ).prop_map(|data| {
            StringType::from_data(data).into()
        }),
        8 =>
        prop::collection::vec(
            prop::option::weighted(
                valid_percent,
                string_strategy()
            ),
            len
        ).prop_map(|data| StringType::from_opt_data(data).into())
    ]
}

pub fn number_column_strategy<K>(
    len: usize,
    valid_percent: f64,
) -> impl Strategy<Value = BlockEntry>
where
    K: Number + Arbitrary + 'static,
    NumberType<K>: FromData<K>,
{
    prop::collection::vec(prop::option::weighted(valid_percent, any::<K>()), len)
        .prop_map(|data| NumberType::<K>::from_opt_data(data).into())
}

pub fn f32_column_strategy(len: usize, valid_percent: f64) -> impl Strategy<Value = BlockEntry> {
    prop::collection::vec(
        prop::option::weighted(valid_percent, any::<f32>().prop_map(F32::from)),
        len,
    )
    .prop_map(|data| NumberType::<F32>::from_opt_data(data).into())
}

pub fn f64_column_strategy(len: usize, valid_percent: f64) -> impl Strategy<Value = BlockEntry> {
    prop::collection::vec(
        prop::option::weighted(valid_percent, any::<f64>().prop_map(F64::from)),
        len,
    )
    .prop_map(|data| NumberType::<F64>::from_opt_data(data).into())
}

#[derive(Debug)]
pub struct TestCase {
    pub entries: Vec<BlockEntry>,
    pub sort_options: Vec<(bool, bool)>,
    pub num_rows: usize,
}

pub fn create_arrow_comparator(
    entries: &[BlockEntry],
    sort_options: &[(bool, bool)],
) -> LexicographicalComparator {
    let order_columns = entries
        .iter()
        .map(|entry| entry.to_column().into_arrow_rs())
        .collect::<Vec<ArrayRef>>();

    let sort_columns = sort_options
        .iter()
        .copied()
        .zip(order_columns.iter())
        .map(|((asc, nulls_first), a)| SortColumn {
            values: a.clone(),
            options: Some(SortOptions {
                descending: !asc,
                nulls_first,
            }),
        })
        .collect::<Vec<_>>();

    LexicographicalComparator::try_new(&sort_columns).unwrap()
}

pub fn create_sort_fields(entries: &[BlockEntry], sort_options: &[(bool, bool)]) -> Vec<SortField> {
    sort_options
        .iter()
        .copied()
        .zip(entries)
        .map(|((asc, nulls_first), col)| {
            SortField::new_with_options(col.data_type(), asc, nulls_first)
        })
        .collect()
}
