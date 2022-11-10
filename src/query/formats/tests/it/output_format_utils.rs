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

use common_arrow::arrow::bitmap::MutableBitmap;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;

pub fn get_simple_block(is_nullable: bool) -> Result<DataBlock> {
    let schema = match is_nullable {
        false => DataSchemaRefExt::create(vec![
            DataField::new("c1", i32::to_data_type()),
            DataField::new("c2", Vu8::to_data_type()),
            DataField::new("c3", bool::to_data_type()),
            DataField::new("c4", f64::to_data_type()),
            DataField::new("c5", DateType::new_impl()),
        ]),
        true => DataSchemaRefExt::create(vec![
            DataField::new_nullable("c1", i32::to_data_type()),
            DataField::new_nullable("c2", Vu8::to_data_type()),
            DataField::new_nullable("c3", bool::to_data_type()),
            DataField::new_nullable("c4", f64::to_data_type()),
            DataField::new_nullable("c5", DateType::new_impl()),
        ]),
    };

    let mut columns = vec![
        Series::from_data(vec![1i32, 2, 3]),
        Series::from_data(vec!["a", "b\"", "c'"]),
        Series::from_data(vec![true, true, false]),
        Series::from_data(vec![1.1f64, 2.2, 3.3]),
        Series::from_data(vec![1_i32, 2_i32, 3_i32]),
    ];

    if is_nullable {
        columns = columns
            .iter()
            .map(|c| {
                let mut validity = MutableBitmap::new();
                validity.extend_constant(c.len(), true);
                NullableColumn::wrap_inner(c.clone(), Some(validity.into()))
            })
            .collect();
    }

    let block = DataBlock::create(schema, columns);

    Ok(block)
}
