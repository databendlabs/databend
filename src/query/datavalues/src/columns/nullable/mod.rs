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

mod mutable;

use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::ArrayRef;
pub use mutable::*;

use crate::prelude::*;

#[derive(Clone)]
pub struct NullableColumn {
    validity: Bitmap,
    column: ColumnRef,
}

impl NullableColumn {
    pub fn wrap_inner(column: ColumnRef, validity: Option<Bitmap>) -> ColumnRef {
        if column.is_nullable() {
            return column;
        }
        if !column.is_const() {
            Self::new_from_opt(column, validity).arc()
        } else {
            let c: &ConstColumn = Series::check_get(&column).unwrap();
            let inner = c.inner().clone();
            // If the column is const, it means the `inner` column is just one size
            // So we just need the first bit of the validity

            let validity = if let Some(b) = validity {
                if b.is_empty() {
                    None
                } else {
                    Some(b.slice(0, 1))
                }
            } else {
                None
            };
            let nullable_column = Self::new_from_opt(inner, validity).arc();
            ConstColumn::new(nullable_column, c.len()).arc()
        }
    }

    pub fn inner(&self) -> &ColumnRef {
        &self.column
    }

    pub fn ensure_validity(&self) -> &Bitmap {
        &self.validity
    }

    // Set `new` to private, this avoids the user to put wrong column to construct a nullable column
    fn new(column: ColumnRef, validity: Bitmap) -> Self {
        debug_assert!(!column.is_const());
        debug_assert!(
            column.data_type().can_inside_nullable(),
            "{} can't be inside of nullable.",
            column.data_type().name()
        );
        Self { column, validity }
    }

    fn new_from_opt(column: ColumnRef, validity: Option<Bitmap>) -> Self {
        let validity = match validity {
            Some(v) => v,
            None => {
                let mut bitmap = MutableBitmap::with_capacity(column.len());
                bitmap.extend_constant(column.len(), true);
                bitmap.into()
            }
        };

        Self::new(column, validity)
    }
}

impl Column for NullableColumn {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> DataTypeImpl {
        let nest = self.column.data_type();
        NullableType::new_impl(nest)
    }

    fn column_type_name(&self) -> String {
        format!("Nullable({})", self.column.column_type_name())
    }

    fn is_nullable(&self) -> bool {
        true
    }

    fn len(&self) -> usize {
        self.column.len()
    }

    fn null_at(&self, row: usize) -> bool {
        !self.validity.get_bit(row)
    }

    fn only_null(&self) -> bool {
        self.validity.unset_bits() == self.validity.len()
    }

    fn validity(&self) -> (bool, Option<&Bitmap>) {
        (self.only_null(), Some(&self.validity))
    }

    fn memory_size(&self) -> usize {
        self.column.memory_size() + self.validity.as_slice().0.len()
    }

    fn as_arrow_array(&self, logical_type: DataTypeImpl) -> ArrayRef {
        let result = self.column.as_arrow_array(logical_type);
        result.with_validity(Some(self.validity.clone()))
    }

    fn arc(&self) -> ColumnRef {
        Arc::new(self.clone())
    }

    fn slice(&self, offset: usize, length: usize) -> ColumnRef {
        Arc::new(Self {
            column: self.column.slice(offset, length),
            validity: self.validity.clone().slice(offset, length),
        })
    }

    fn filter(&self, filter: &BooleanColumn) -> ColumnRef {
        if filter.values().unset_bits() == 0 {
            return Arc::new(self.clone());
        }
        let inner = self.inner().filter(filter);
        let iter = self
            .validity
            .iter()
            .zip(filter.values().iter())
            .filter(|(_, f)| *f)
            .map(|(v, _)| v);
        let validity = MutableBitmap::from_iter(iter);

        Arc::new(Self::new(inner, validity.into()))
    }

    fn scatter(&self, indices: &[usize], scattered_size: usize) -> Vec<ColumnRef> {
        let inner_values = self.inner().scatter(indices, scattered_size);
        let mut bitmaps = Vec::with_capacity(scattered_size);
        for _ in 0..scattered_size {
            let bitmap = MutableBitmap::with_capacity(self.len());
            bitmaps.push(bitmap);
        }
        unsafe {
            indices.iter().zip(self.validity.iter()).for_each(|(i, f)| {
                bitmaps[*i].push_unchecked(f);
            });
        }

        let mut results = Vec::with_capacity(scattered_size);

        for (index, value) in inner_values.iter().enumerate().take(scattered_size) {
            let bitmap = bitmaps.get_mut(index).unwrap();
            let bitmap = std::mem::take(bitmap).into();
            results.push(NullableColumn::wrap_inner(value.clone(), Some(bitmap)));
        }

        results
    }

    fn replicate(&self, offsets: &[usize]) -> ColumnRef {
        debug_assert!(
            offsets.len() == self.len(),
            "Size of offsets must match size of column"
        );

        let column = self.column.replicate(offsets);

        let capacity = *offsets.last().unwrap();
        let mut bitmap = MutableBitmap::with_capacity(capacity);
        let mut previous_offset: usize = 0;

        (0..self.len()).for_each(|i| {
            let offset: usize = offsets[i];
            let bit = self.validity.get_bit(i);
            bitmap.extend_constant(offset - previous_offset, bit);
            previous_offset = offset;
        });

        Arc::new(Self {
            validity: bitmap.into(),
            column,
        })
    }

    fn convert_full_column(&self) -> ColumnRef {
        Arc::new(Self {
            column: self.column.convert_full_column(),
            validity: self.validity.clone(),
        })
    }

    fn get(&self, index: usize) -> DataValue {
        if self.validity.get_bit(index) {
            self.column.get(index)
        } else {
            DataValue::Null
        }
    }

    fn serialize(&self, vec: &mut Vec<u8>, row: usize) {
        let valid = self.validity.get_bit(row);
        vec.push(valid as u8);
        if valid {
            self.column.serialize(vec, row);
        }
    }
}

impl std::fmt::Debug for NullableColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut data = Vec::new();
        for idx in 0..self.len() {
            if self.validity.get_bit(idx) {
                let val = self.column.get(idx);
                data.push(format!("{:?}", val));
            } else {
                data.push("NULL".to_string());
            }
        }
        let head = "NullableColumn";
        display_fmt(
            data.iter(),
            head,
            self.len(),
            self.inner().data_type_id(),
            f,
        )
    }
}
