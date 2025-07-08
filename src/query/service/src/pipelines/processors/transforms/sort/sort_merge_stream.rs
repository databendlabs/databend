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

use std::marker::PhantomData;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_transforms::sort::Rows;
use databend_common_pipeline_transforms::sort::SortedStream;

use crate::pipelines::processors::transforms::SortBound;

/// InputBoundStream is a stream of blocks that are cutoff less or equal than bound.
struct InputBoundStream<R: Rows> {
    data: Option<DataBlock>,
    input: Arc<InputPort>,
    remove_order_col: bool,
    bound: Option<Scalar>,
    sort_row_offset: usize,
    _r: PhantomData<R>,
}

impl<R: Rows> SortedStream for InputBoundStream<R> {
    fn next(&mut self) -> Result<(Option<(DataBlock, Column)>, bool)> {
        if self.pull()? {
            return Ok((None, true));
        }

        match self.take_next_bounded_block() {
            None => Ok((None, false)),
            Some(mut block) => {
                let col = sort_column(&block, self.sort_row_offset).clone();
                if self.remove_order_col {
                    block.remove_column(self.sort_row_offset);
                }
                Ok((Some((block, col)), false))
            }
        }
    }
}

fn sort_column(data: &DataBlock, sort_row_offset: usize) -> &Column {
    data.get_by_offset(sort_row_offset).as_column().unwrap()
}

impl<R: Rows> InputBoundStream<R> {
    fn pull(&mut self) -> Result<bool> {
        if self.data.is_some() {
            return Ok(false);
        }

        if self.input.has_data() {
            let block = self.input.pull_data().unwrap()?;
            self.input.set_need_data();
            self.data = Some(block);
            Ok(false)
        } else if self.input.is_finished() {
            Ok(false)
        } else {
            self.input.set_need_data();
            Ok(true)
        }
    }

    fn take_next_bounded_block(&mut self) -> Option<DataBlock> {
        let Some(bound) = &self.bound else {
            return self.data.take();
        };

        let meta = self
            .data
            .as_ref()?
            .get_meta()
            .and_then(SortBound::downcast_ref_from)
            .expect("require a SortBound");

        if meta.bound.as_ref() == Some(bound) {
            self.data.take()
        } else {
            None
        }
    }
}
