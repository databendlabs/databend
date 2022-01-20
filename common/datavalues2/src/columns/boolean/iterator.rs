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

use common_arrow::arrow::bitmap::utils::BitmapIter;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_infallible::RwLock;

use crate::prelude::*;

impl<'a> BooleanColumn {
    pub fn iter(&'a self) -> BitmapIter<'a> {
        self.values.iter()
    }
}

impl NewColumn<bool> for BooleanColumn {
    fn new_from_slice<P: AsRef<[bool]>>(slice: P) -> Self {
        let bitmap = MutableBitmap::from_iter(slice.as_ref().iter().cloned());
        BooleanColumn {
            values: bitmap.into(),
            data_cached: Arc::new(RwLock::new(false)),
            data: Arc::new(Vec::new()),
        }
    }

    fn new_from_iter(it: impl Iterator<Item = bool>) -> Self {
        let bitmap = MutableBitmap::from_iter(it);
        BooleanColumn {
            values: bitmap.into(),
            data_cached: Arc::new(RwLock::new(false)),
            data: Arc::new(Vec::new()),
        }
    }
}
