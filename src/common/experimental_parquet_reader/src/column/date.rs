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

use databend_common_expression::Column;
use parquet2::schema::types::PhysicalType;

use crate::column::common::ParquetColumnIterator;
use crate::column::common::ParquetColumnType;
use crate::column::number::IntegerMetadata;

#[derive(Copy, Clone)]
#[repr(transparent)]
pub struct Date(i32);

impl ParquetColumnType for Date {
    type Metadata = IntegerMetadata;
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Int32;

    fn create_column(data: Vec<Self>, _metadata: &Self::Metadata) -> Column {
        let raw_data: Vec<i32> = unsafe { std::mem::transmute(data) };
        Column::Date(raw_data.into())
    }
}

pub type DateIter<'a> = ParquetColumnIterator<'a, Date>;
