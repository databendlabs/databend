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

use std::cmp::Ordering;
use std::iter;
use std::marker::PhantomData;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_core::processors::Exchange;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_core::PipeItem;
use databend_common_pipeline_transforms::processors::sort::select_row_type;
use databend_common_pipeline_transforms::processors::sort::Rows;
use databend_common_pipeline_transforms::processors::sort::RowsTypeVisitor;

use super::sort_simple::SortSimpleState;
use crate::pipelines::processors::PartitionProcessor;

pub struct SortRangeExchange<R: Rows> {
    sort_desc: Arc<Vec<SortColumnDescription>>,
    state: Arc<SortSimpleState>,
    _r: PhantomData<R>,
}

unsafe impl<R: Rows> Send for SortRangeExchange<R> {}

unsafe impl<R: Rows> Sync for SortRangeExchange<R> {}

impl<R: Rows + 'static> Exchange for SortRangeExchange<R> {
    const NAME: &'static str = "SortRange";
    fn partition(&self, data: DataBlock, n: usize) -> Result<Vec<DataBlock>> {
        let bounds = self.state.bounds().unwrap();
        debug_assert_eq!(n, self.state.partitions());
        debug_assert!(bounds.len() < n);

        if data.is_empty() {
            return Ok(vec![]);
        }

        if bounds.len() == 0 {
            return Ok(vec![data]);
        }

        let bounds = R::from_column(&bounds, &self.sort_desc)?;
        let rows = R::from_column(data.get_last_column(), &self.sort_desc)?;

        let mut i = 0;
        let mut j = 0;
        let mut bound = bounds.row(j);
        let mut indices = Vec::new();
        while i < rows.len() {
            match rows.row(i).cmp(&bound) {
                Ordering::Less => indices.push(j as u32),
                Ordering::Greater if j + 1 < bounds.len() => {
                    j += 1;
                    bound = bounds.row(j);
                    continue;
                }
                _ => indices.push(j as u32 + 1),
            }
            i += 1;
        }

        DataBlock::scatter(&data, &indices, n)
    }
}

pub fn create_exchange_pipe(
    inputs: usize,
    partitions: usize,
    schema: DataSchemaRef,
    sort_desc: Arc<Vec<SortColumnDescription>>,
    state: Arc<SortSimpleState>,
) -> Pipe {
    let mut builder = Builder {
        inputs,
        partitions,
        sort_desc,
        schema,
        state,
        items: Vec::new(),
    };

    select_row_type(&mut builder);

    Pipe::create(inputs, inputs * partitions, builder.items)
}

struct Builder {
    inputs: usize,
    partitions: usize,
    sort_desc: Arc<Vec<SortColumnDescription>>,
    schema: DataSchemaRef,
    state: Arc<SortSimpleState>,
    items: Vec<PipeItem>,
}

impl RowsTypeVisitor for Builder {
    fn visit_type<R: Rows + 'static>(&mut self) {
        let exchange = Arc::new(SortRangeExchange::<R> {
            sort_desc: self.sort_desc.clone(),
            state: self.state.clone(),
            _r: PhantomData,
        });
        self.items = iter::repeat_with(|| {
            let input = InputPort::create();
            let outputs = iter::repeat_with(OutputPort::create)
                .take(self.partitions)
                .collect::<Vec<_>>();

            PipeItem::create(
                PartitionProcessor::create(input.clone(), outputs.clone(), exchange.clone()),
                vec![input],
                outputs,
            )
        })
        .take(self.inputs)
        .collect::<Vec<_>>();
    }

    fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }

    fn sort_desc(&self) -> &[SortColumnDescription] {
        &self.sort_desc
    }
}
