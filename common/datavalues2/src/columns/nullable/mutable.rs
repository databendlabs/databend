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

use std::any::Any;
use std::sync::Arc;
use common_arrow::arrow::bitmap::MutableBitmap;
use crate::{ColumnRef, DataTypePtr, MutableColumn, NullableColumn};

pub struct MutableNullableColumn<M: MutableColumn> {
    bitmap: MutableBitmap,
    values: M,
    data_type: DataTypePtr,
}

impl<M: MutableColumn + 'static> MutableColumn for MutableNullableColumn<M> where M:  MutableColumn {
    fn data_type(&self) -> DataTypePtr {
        self.data_type.clone()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn as_column(&mut self) -> ColumnRef {
        let column = self.values.as_column();
        let bitmap =   std::mem::take(&mut self.bitmap);
        Arc::new(NullableColumn::new(column, bitmap.into()))
    }

    fn append_default(&mut self) {
        todo!()
    }

    fn append_null(&mut self) -> bool {
        todo!()
    }

    fn validity(&self) -> Option<&MutableBitmap> {
        todo!()
    }

    fn shrink_to_fit(&mut self) {
        todo!()
    }
}