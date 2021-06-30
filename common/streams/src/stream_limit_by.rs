// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::convert::TryInto;
use std::task::Context;
use std::task::Poll;

use common_arrow::arrow;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_datavalues::DFBooleanArray;
use common_exception::Result;
use futures::Stream;
use futures::StreamExt;

use crate::SendableDataBlockStream;

pub struct LimitByStream {
    input: SendableDataBlockStream,
    limit: usize,
    limit_by_columns_name: Vec<String>,
}

impl LimitByStream {
    pub fn try_create(
        input: SendableDataBlockStream,
        limit: usize,
        limit_by_columns_name: Vec<String>,
    ) -> Result<Self> {
        Ok(LimitByStream {
            input,
            limit,
            limit_by_columns_name,
        })
    }

    pub fn limit_by(&mut self, block: &DataBlock) -> Result<Option<DataBlock>> {
        // TODO: use BitVec here.
        let mut filter_vec = vec![false; block.num_rows()];
        let group_indices = DataBlock::group_by_get_indices(&block, &self.limit_by_columns_name)?;
        for (_, rows) in group_indices {
            let mut cnt = 0;
            for row in rows {
                cnt += 1;
                filter_vec[row as usize] = cnt <= self.limit;
            }
        }

        let filter_array = DFBooleanArray::new_from_slice(&filter_vec);
        let batch = block.clone().try_into()?;
        let batch = arrow::compute::filter_record_batch(&batch, filter_array.downcast_ref())?;
        Some(batch.try_into()).transpose()
    }
}

impl Stream for LimitByStream {
    type Item = Result<DataBlock>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(ctx).map(|x| match x {
            Some(Ok(ref v)) => self.limit_by(v).transpose(),
            other => other,
        })
    }
}
