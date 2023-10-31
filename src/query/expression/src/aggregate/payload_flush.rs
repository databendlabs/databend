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

use std::alloc::Layout;
use std::sync::Arc;

use bumpalo::Bump;

use super::payload::Payload;
use super::payload_row::rowformat_size;
use super::payload_row::serialize_column_to_rowformat;
use super::probe_state::ProbeState;
use crate::get_layout_offsets;
use crate::load;
use crate::select_vector::SelectVector;
use crate::store;
use crate::types::ArgType;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::TimestampType;
use crate::AggregateFunctionRef;
use crate::Column;
use crate::StateAddr;

const FLUSH_BATCH_SIZE: usize = 8192;

pub(crate) struct PayloadFlushState {
    probe_state: ProbeState,
    flush_offset: usize,
}

impl Payload {
    pub fn flush(&self, state: &mut PayloadFlushState) -> bool {
        let flush_end = (state.flush_offset + FLUSH_BATCH_SIZE).min(self.len());

        let rows = flush_end - state.flush_offset;
        if rows == 0 {
            return false;
        }

        state.probe_state.ajust_row_count(rows);

        for row in state.flush_offset..flush_end {
            state.probe_state.addresses[idx] = self.get_row_ptr(row);
        }

        state.flush_offset = flush_end;
    }

    fn flush_column(&self, col_index: usize, state: &mut PayloadFlushState) -> Column {
        let len = state.probe_state.row_count;

        let bitmap = if self.group_types[col_index].is_nullable() {
            // todo: read bitmap
            None
        } else {
            None
        };
        match self.group_types[col_index].remove_nullable() {
            DataType::Null => Column::Null { len },
            DataType::EmptyArray => Column::EmptyArray { len },
            DataType::EmptyMap => Column::EmptyMap { len },
            DataType::Boolean => self.flush_column_type::<BooleanType>(col_index, state),
            DataType::String => todo!(),
            DataType::Number(_) => todo!(),
            DataType::Decimal(_) => todo!(),
            DataType::Timestamp => self.flush_column_type::<TimestampType>(col_index, state),
            DataType::Date => todo!(),
            DataType::Nullable(_) => todo!(),
            DataType::Array(_) => todo!(),
            DataType::Map(_) => todo!(),
            DataType::Bitmap => todo!(),
            DataType::Tuple(_) => todo!(),
            DataType::Variant => todo!(),
            DataType::Generic(_) => todo!(),
        }
    }

    fn flush_column_type<T: ArgType>(
        &self,
        col_index: usize,
        state: &mut PayloadFlushState,
    ) -> Column {
        let len = state.probe_state.row_count;
        let col_offset = self.group_offsets[col_index];
        let iter = (0..len).map(|idx| unsafe {
            load::<T::Scalar>(state.probe_state.addresses[idx].offset(col_offset as isize))
        });
        let col = T::column_from_iter(iter, &[]);
        T::upcast_column(col)
    }
}
