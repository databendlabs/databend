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

use std::sync::Arc;

use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::Result;

use crate::columns::mutable::MutableColumn;
use crate::prelude::*;
use crate::types::create_primitive_datatype;

#[derive(Debug)]
pub struct MutablePrimitiveColumn<T>
where T: PrimitiveType
{
    data_type: DataTypePtr,
    pub(crate) values: Vec<T>,
}

impl<T> MutableColumn for MutablePrimitiveColumn<T>
where T: PrimitiveType
{
    fn data_type(&self) -> DataTypePtr {
        self.data_type.clone()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn append_default(&mut self) {
        self.append_value(T::default());
    }

    fn validity(&self) -> Option<&MutableBitmap> {
        None
    }

    fn shrink_to_fit(&mut self) {
        self.values.shrink_to_fit();
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn to_column(&mut self) -> crate::ColumnRef {
        self.shrink_to_fit();
        Arc::new(PrimitiveColumn::<T> {
            values: std::mem::take(&mut self.values).into(),
        })
    }

    fn append_data_value(&mut self, value: crate::DataValue) -> Result<()> {
        let t: T = DFTryFrom::try_from(value)?;
        self.append_value(t);
        Ok(())
    }
}

impl<T> Default for MutablePrimitiveColumn<T>
where T: PrimitiveType
{
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

// for nullable values

impl<T> MutablePrimitiveColumn<T>
where T: PrimitiveType
{
    pub fn from_data(data_type: DataTypePtr, values: Vec<T>) -> Self {
        Self { data_type, values }
    }

    pub fn append_value(&mut self, val: T) {
        self.values.push(val);
    }

    pub fn values(&self) -> &Vec<T> {
        &self.values
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let data_type = create_primitive_datatype::<T>();
        MutablePrimitiveColumn {
            data_type,
            values: Vec::<T>::with_capacity(capacity),
        }
    }
}

impl<T> ScalarColumnBuilder for MutablePrimitiveColumn<T>
where
    T: PrimitiveType,
    T: Scalar<ColumnType = PrimitiveColumn<T>>,
    for<'a> T: ScalarRef<'a, ScalarType = T, ColumnType = PrimitiveColumn<T>>,
    for<'a> T: Scalar<RefType<'a> = T>,
{
    type ColumnType = PrimitiveColumn<T>;

    fn with_capacity(capacity: usize) -> Self {
        let data_type = create_primitive_datatype::<T>();
        MutablePrimitiveColumn {
            data_type,
            values: Vec::<T>::with_capacity(capacity),
        }
    }

    fn push(&mut self, value: <T as Scalar>::RefType<'_>) {
        self.values.push(value);
    }

    fn finish(&mut self) -> Self::ColumnType {
        self.shrink_to_fit();
        PrimitiveColumn::<T> {
            values: std::mem::take(&mut self.values).into(),
        }
    }
}
