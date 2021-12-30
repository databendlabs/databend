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

use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::BooleanArray;
use common_arrow::arrow::bitmap::MutableBitmap;

use crate::arrays::mutable::MutableArrayBuilder;
use crate::series::IntoSeries;
use crate::series::Series;
use crate::DataType;

pub struct MutableBooleanArrayBuilder {
    data_type: DataType,
    values: MutableBitmap,
    validity: Option<MutableBitmap>,
}

impl MutableArrayBuilder for MutableBooleanArrayBuilder {
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn as_series(&mut self) -> Series {
        let array: Arc<dyn Array> = Arc::new(BooleanArray::from_data(
            self.data_type.to_arrow(),
            std::mem::take(&mut self.values).into(),
            std::mem::take(&mut self.validity).map(|x| x.into()),
        ));
        array.into_series()
    }

    fn push_null(&mut self) {
        self.push_option(None);
    }
}

impl Default for MutableBooleanArrayBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl MutableBooleanArrayBuilder {
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data_type: DataType::Boolean,
            values: MutableBitmap::with_capacity(capacity),
            validity: None,
        }
    }

    pub fn from_data(values: MutableBitmap, validity: Option<MutableBitmap>) -> Self {
        Self {
            data_type: DataType::Boolean,
            values,
            validity,
        }
    }

    pub fn push(&mut self, value: bool) {
        self.values.push(value);
        match &mut self.validity {
            Some(validity) => validity.push(true),
            None => {}
        }
    }

    pub fn push_option(&mut self, value: Option<bool>) {
        match value {
            Some(value) => {
                self.push(value);
            }
            None => {
                self.values.push(false);
                match &mut self.validity {
                    Some(validity) => validity.push(false),
                    None => self.init_validity(),
                }
            }
        }
    }

    fn init_validity(&mut self) {
        let mut validity = MutableBitmap::with_capacity(self.values.capacity());
        validity.extend_constant(self.len(), true);
        validity.set(self.len() - 1, false);
        self.validity = Some(validity)
    }

    fn len(&self) -> usize {
        self.values.len()
    }
}
