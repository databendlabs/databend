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

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::types::Index;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::prelude::*;

impl Series {
    pub fn take<I: Index>(column: &ColumnRef, indices: &[I]) -> Result<ColumnRef> {
        if column.is_const() {
            Ok(column.slice(0, indices.len()))
        } else if column.is_nullable() {
            let nullable_c: &NullableColumn = unsafe { Series::static_cast(column) };
            let inner_result = Self::take(nullable_c.inner(), indices)?;

            let values = nullable_c.ensure_validity();
            let values = indices.iter().map(|index| values.get_bit(index.to_usize()));
            let validity_result = Bitmap::from_trusted_len_iter(values);

            Ok(Arc::new(NullableColumn::new(inner_result, validity_result)))
        } else {
            let type_id = column.data_type_id().to_physical_type();

            with_match_scalar_type!(type_id, |$T| {
                let col: &<$T as Scalar>::ColumnType = Series::check_get(column)?;
                type Builder = <<$T as Scalar>::ColumnType as ScalarColumn>::Builder;
                let mut builder = Builder::with_capacity(indices.len());

                for index in indices {
                    builder.push( col.get_data(index.to_usize()) );
                }

                let result = builder.finish();
                Ok(Arc::new(result))
            },
            {
                Err(ErrorCode::BadDataValueType(format!(
                    "Column with type: {:?} does not support take",
                    type_id
                )))
            })
        }
    }
}
