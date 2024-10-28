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

use arrow_data::ArrayData;
use arrow_data::ArrayDataBuilder;
use arrow_schema::DataType;

use crate::arrow::array::Arrow2Arrow;
use crate::arrow::array::BinaryViewArray;
use crate::arrow::array::BinaryViewArrayGeneric;
use crate::arrow::array::MutableBinaryViewArray;
use crate::arrow::array::Utf8ViewArray;
use crate::arrow::array::ViewType;
use crate::arrow::bitmap::Bitmap;

impl<T: ViewType + ?Sized, P: AsRef<T>> FromIterator<Option<P>> for BinaryViewArrayGeneric<T> {
    #[inline]
    fn from_iter<I: IntoIterator<Item = Option<P>>>(iter: I) -> Self {
        MutableBinaryViewArray::<T>::from_iter(iter).into()
    }
}

impl Arrow2Arrow for BinaryViewArray {
    fn to_data(&self) -> ArrayData {
        let builder = ArrayDataBuilder::new(DataType::BinaryView)
            .len(self.len())
            .add_buffer(self.views.clone().into())
            .add_buffers(self.buffers.iter().map(|x| x.clone().into()).collect())
            .nulls(self.validity.clone().map(Into::into));
        unsafe { builder.build_unchecked() }
    }

    fn from_data(data: &ArrayData) -> Self {
        let views = crate::arrow::buffer::Buffer::from(data.buffers()[0].clone());
        let buffers = data.buffers()[1..]
            .iter()
            .map(|x| crate::arrow::buffer::Buffer::from(x.clone()))
            .collect();
        let validity = data.nulls().map(|x| Bitmap::from_null_buffer(x.clone()));
        Self::try_new(
            crate::arrow::datatypes::DataType::BinaryView,
            views,
            buffers,
            validity,
        )
        .unwrap()
    }
}

impl Arrow2Arrow for Utf8ViewArray {
    fn to_data(&self) -> ArrayData {
        let builder = ArrayDataBuilder::new(DataType::Utf8View)
            .len(self.len())
            .add_buffer(self.views.clone().into())
            .add_buffers(self.buffers.iter().map(|x| x.clone().into()).collect())
            .nulls(self.validity.clone().map(Into::into));
        unsafe { builder.build_unchecked() }
    }

    fn from_data(data: &ArrayData) -> Self {
        let views = crate::arrow::buffer::Buffer::from(data.buffers()[0].clone());
        let buffers = data.buffers()[1..]
            .iter()
            .map(|x| crate::arrow::buffer::Buffer::from(x.clone()))
            .collect();
        let validity = data.nulls().map(|x| Bitmap::from_null_buffer(x.clone()));
        Self::try_new(
            crate::arrow::datatypes::DataType::Utf8View,
            views,
            buffers,
            validity,
        )
        .unwrap()
    }
}
