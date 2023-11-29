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

use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;

use crate::arrow::bitmap_into_mut;
use crate::types::BooleanType;
use crate::Value;
use crate::BIT_MASK;

pub struct FilterHelpers;

impl FilterHelpers {
    #[inline]
    pub fn is_all_unset(predicate: &Value<BooleanType>) -> bool {
        match &predicate {
            Value::Scalar(v) => !v,
            Value::Column(bitmap) => bitmap.unset_bits() == bitmap.len(),
        }
    }

    pub fn filter_to_bitmap(predicate: Value<BooleanType>, rows: usize) -> MutableBitmap {
        match predicate {
            Value::Scalar(true) => MutableBitmap::from_len_set(rows),
            Value::Scalar(false) => MutableBitmap::from_len_zeroed(rows),
            Value::Column(bitmap) => bitmap_into_mut(bitmap),
        }
    }

    pub fn selection_to_bitmap(selection: &[u32], rows: usize) -> Bitmap {
        if selection.is_empty() {
            return Bitmap::new_constant(false, rows);
        } else if selection.len() == rows {
            return Bitmap::new_constant(true, rows);
        }

        let capacity = rows.saturating_add(7) / 8;
        let mut builder: Vec<u8> = Vec::with_capacity(capacity);
        let mut builder_len = 0;
        let mut value = 0;
        let mut unset_bits = 0;

        unsafe {
            let mut i = 0;
            for idx in selection {
                while (i as u32) < *idx {
                    unset_bits += 1;
                    i += 1;
                    if i % 8 == 0 {
                        *builder.get_unchecked_mut(builder_len) = value;
                        builder_len += 1;
                        value = 0;
                    }
                }
                value |= BIT_MASK[i % 8];
                i += 1;
                if i % 8 == 0 {
                    *builder.get_unchecked_mut(builder_len) = value;
                    builder_len += 1;
                    value = 0;
                }
            }
            while i < rows {
                unset_bits += 1;
                i += 1;
                if i % 8 == 0 {
                    *builder.get_unchecked_mut(builder_len) = value;
                    builder_len += 1;
                    value = 0;
                }
            }
            if i % 8 != 0 {
                *builder.get_unchecked_mut(builder_len) = value;
                builder_len += 1;
            }
            builder.set_len(builder_len);
            Bitmap::from_inner(Arc::new(builder.into()), 0, rows, unset_bits)
                .ok()
                .unwrap()
        }
    }

    pub fn selection_to_mutable_bitmap(selection: &[u32], rows: usize) -> MutableBitmap {
        if selection.is_empty() {
            return MutableBitmap::from_len_zeroed(rows);
        } else if selection.len() == rows {
            return MutableBitmap::from_len_set(rows);
        }

        let capacity = rows.saturating_add(7) / 8;
        let mut builder: Vec<u8> = Vec::with_capacity(capacity);
        let mut builder_len = 0;
        let mut value = 0;
        let mut idx = 0;

        unsafe {
            let mut i = 0;
            for idx in selection {
                while (i as u32) < *idx {
                    i += 1;
                    if i % 8 == 0 {
                        *builder.get_unchecked_mut(builder_len) = value;
                        builder_len += 1;
                        value = 0;
                    }
                }
                value |= BIT_MASK[i % 8];
                i += 1;
                if i % 8 == 0 {
                    *builder.get_unchecked_mut(builder_len) = value;
                    builder_len += 1;
                    value = 0;
                }
            }
            while i < rows {
                i += 1;
                if i % 8 == 0 {
                    *builder.get_unchecked_mut(builder_len) = value;
                    builder_len += 1;
                    value = 0;
                }
            }
            if i % 8 != 0 {
                *builder.get_unchecked_mut(builder_len) = value;
                builder_len += 1;
            }

            let mut i = 0;
            while i < rows {
                if i as u32 == selection[idx] {
                    value |= BIT_MASK[i % 8];
                    idx += 1;
                }
                i += 1;
                if i % 8 == 0 {
                    *builder.get_unchecked_mut(builder_len) = value;
                    builder_len += 1;
                    value = 0;
                }
            }
            if i % 8 != 0 {
                *builder.get_unchecked_mut(builder_len) = value;
                builder_len += 1;
            }
            MutableBitmap::from_vec(builder, builder_len)
        }
    }
}
