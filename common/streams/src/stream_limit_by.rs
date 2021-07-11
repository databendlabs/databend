// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::convert::TryInto;
use std::task::Context;
use std::task::Poll;

use common_arrow::arrow;
use common_arrow::arrow::array::BooleanArray;
use common_datablocks::DataBlock;
use common_datablocks::HashMethod;
use common_datablocks::HashMethodSerializer;
use common_exception::Result;
use futures::Stream;
use futures::StreamExt;

use crate::SendableDataBlockStream;

pub struct LimitByStream {
    input: SendableDataBlockStream,
    limit: usize,
    limit_by_columns_name: Vec<String>,
    keys_count: HashMap<Vec<u8>, usize>,
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
            keys_count: HashMap::new(),
        })
    }

    pub fn limit_by(&mut self, block: &DataBlock) -> Result<Option<DataBlock>> {
        // TODO: use BitVec here.
        let mut filter_vec = vec![false; block.num_rows()];
        let method = HashMethodSerializer::default();
        let group_indices = method.group_by_get_indices(block, &self.limit_by_columns_name)?;

        for (limit_by_key, (rows, _)) in group_indices {
            for row in rows {
                let count = self.keys_count.entry(limit_by_key.clone()).or_default();
                *count += 1;
                filter_vec[row as usize] = *count <= self.limit;
            }
        }

        let filter_array = BooleanArray::from(filter_vec);
        let batch = block.clone().try_into()?;
        let batch = arrow::compute::filter_record_batch(&batch, &filter_array)?;
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
