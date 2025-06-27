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
use std::sync::RwLock;

use databend_common_base::base::WatchNotify;
use databend_common_exception::Result;
use databend_common_expression::sampler::FixedSizeSampler;
use databend_common_expression::visitor::ValueVisitor;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::SortCompare;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_core::PipeItem;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_transforms::processors::create_multi_sort_merge_processor;
use databend_common_pipeline_transforms::processors::sort::convert_rows;
use databend_common_pipeline_transforms::processors::Transform;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use rand::rngs::StdRng;
use rand::SeedableRng;

use super::sort_exchange::create_exchange_pipe;
use super::sort_wait::TransformSortSampleWait;

pub struct SortSampleState {
    inner: RwLock<StateInner>,
    pub(super) done: WatchNotify,
}

impl SortSampleState {
    pub fn partitions(&self) -> usize {
        self.inner.read().unwrap().partitions
    }
}

struct StateInner {
    partitions: usize,
    // schema for bounds DataBlock
    schema: DataSchemaRef,
    // sort_desc for bounds DataBlock
    sort_desc: Vec<SortColumnDescription>,
    partial: Vec<Option<DataBlock>>,
    bounds: Option<Column>,
}

impl StateInner {
    fn determine_bounds(&mut self) -> Result<()> {
        let partial = std::mem::take(&mut self.partial)
            .into_iter()
            .filter_map(|b| {
                let b = b.unwrap();
                if b.is_empty() {
                    None
                } else {
                    Some(b)
                }
            })
            .collect::<Vec<_>>();

        if partial.is_empty() {
            let bounds = convert_rows(
                self.schema.clone(),
                &self.sort_desc,
                DataBlock::empty_with_schema(self.schema.clone()),
            )?;

            self.bounds = Some(bounds);
            return Ok(());
        }

        let candidates = DataBlock::concat(&partial)?;
        let rows = candidates.num_rows();

        let mut sort_compare = SortCompare::with_force_equality(self.sort_desc.clone(), rows);

        for desc in &self.sort_desc {
            let array = candidates.get_by_offset(desc.offset).value.clone();
            sort_compare.visit_value(array)?;
            sort_compare.increment_column_index();
        }

        let equality = sort_compare.equality_index().to_vec();
        let permutation = sort_compare.take_permutation();

        let step = permutation.len() as f64 / self.partitions as f64;
        let mut target = step;
        let mut bounds = Vec::with_capacity(self.partitions - 1);
        let mut equals = true;
        for (i, (&pos, eq)) in permutation.iter().zip(equality).enumerate() {
            if bounds.len() >= self.partitions - 1 {
                break;
            }
            if equals && eq == 0 {
                equals = false
            }
            if i as f64 >= target && (!equals || i != 0) {
                bounds.push(pos);
                target += step;
                equals = true
            }
        }

        let bounds = convert_rows(
            self.schema.clone(),
            &self.sort_desc,
            candidates.take(&bounds)?,
        )?;
        self.bounds = Some(bounds);
        Ok(())
    }
}

impl SortSampleState {
    pub fn new(
        inputs: usize,
        partitions: usize,
        schema: DataSchemaRef,
        sort_desc: Arc<[SortColumnDescription]>,
    ) -> Arc<SortSampleState> {
        let columns = sort_desc.iter().map(|desc| desc.offset).collect::<Vec<_>>();
        let schema = schema.project(&columns).into();
        let sort_desc = sort_desc
            .iter()
            .enumerate()
            .map(|(i, desc)| SortColumnDescription {
                offset: i,
                asc: desc.asc,
                nulls_first: desc.nulls_first,
            })
            .collect::<Vec<_>>();
        Arc::new(SortSampleState {
            inner: RwLock::new(StateInner {
                partitions,
                schema,
                sort_desc,
                partial: vec![None; inputs],
                bounds: None,
            }),
            done: WatchNotify::new(),
        })
    }

    pub fn bounds(&self) -> Option<Column> {
        if let Some(bounds) = &self.inner.read().unwrap().bounds {
            return Some(bounds.clone());
        }
        None
    }

    pub fn commit_sample(&self, id: usize, block: Option<DataBlock>) -> Result<bool> {
        let mut inner = self.inner.write().unwrap();

        let block = block.unwrap_or(DataBlock::empty_with_schema(inner.schema.clone()));
        let x = inner.partial[id].replace(block);
        debug_assert!(x.is_none());
        let done = inner.partial.iter().all(|x| x.is_some());
        if done {
            inner.determine_bounds()?;
            self.done.notify_waiters();
        }
        Ok(done)
    }
}

pub struct TransformSortSample {
    id: usize,
    sampler: FixedSizeSampler<StdRng>,
    state: Arc<SortSampleState>,
}

unsafe impl Send for TransformSortSample {}

impl TransformSortSample {
    fn new(id: usize, k: usize, columns: Vec<usize>, state: Arc<SortSampleState>) -> Self {
        let rng = StdRng::from_rng(rand::thread_rng()).unwrap();
        let sampler = FixedSizeSampler::new(columns, 65536, k, rng);
        TransformSortSample { id, sampler, state }
    }
}

impl Transform for TransformSortSample {
    const NAME: &'static str = "TransformSortSample";

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        self.sampler.add_block(data.clone());
        Ok(data)
    }

    fn on_finish(&mut self) -> Result<()> {
        self.sampler.compact_blocks();
        let mut simple = self.sampler.take_blocks();
        assert!(simple.len() <= 1); // Unlikely to sample rows greater than 65536
        self.state.commit_sample(
            self.id,
            if simple.is_empty() {
                None
            } else {
                Some(simple.remove(0))
            },
        )?;
        Ok(())
    }
}

pub fn add_sort_sample(
    pipeline: &mut Pipeline,
    state: Arc<SortSampleState>,
    sort_desc: Arc<[SortColumnDescription]>,
    k: usize,
) -> Result<()> {
    use std::sync::atomic;
    let i = atomic::AtomicUsize::new(0);
    let columns = sort_desc.iter().map(|desc| desc.offset).collect::<Vec<_>>();
    pipeline.add_transformer(|| {
        let id = i.fetch_add(1, atomic::Ordering::AcqRel);
        TransformSortSample::new(id, k, columns.clone(), state.clone())
    });
    Ok(())
}

pub fn add_range_shuffle(
    pipeline: &mut Pipeline,
    state: Arc<SortSampleState>,
    sort_desc: Arc<[SortColumnDescription]>,
    schema: DataSchemaRef,
    block_size: usize,
    limit: Option<usize>,
    remove_order_col: bool,
    enable_loser_tree: bool,
) -> Result<()> {
    pipeline.add_transform(|input, output| {
        Ok(ProcessorPtr::create(Box::new(
            TransformSortSampleWait::new(input, output, state.clone()),
        )))
    })?;

    // partition data block
    let input_len = pipeline.output_len();
    let n = state.partitions();
    let exchange = create_exchange_pipe(input_len, n, schema.clone(), sort_desc.clone(), state);
    pipeline.add_pipe(exchange);

    let reorder_edges = (0..input_len * n)
        .map(|index| (index % n) * input_len + (index / n))
        .collect::<Vec<_>>();

    pipeline.reorder_inputs(reorder_edges);

    let mut items = Vec::with_capacity(input_len);
    for _ in 0..n {
        let output = OutputPort::create();
        let inputs: Vec<_> = (0..input_len).map(|_| InputPort::create()).collect();

        let proc = create_multi_sort_merge_processor(
            inputs.clone(),
            output.clone(),
            schema.clone(),
            block_size,
            limit,
            sort_desc.clone(),
            remove_order_col,
            enable_loser_tree,
        )?;

        items.push(PipeItem::create(ProcessorPtr::create(proc), inputs, vec![
            output,
        ]));
    }

    // merge partition
    pipeline.add_pipe(Pipe::create(input_len * n, n, items));

    Ok(())
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::ArgType;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::DataField;
    use databend_common_expression::DataSchemaRefExt;
    use databend_common_expression::FromData;

    use super::*;

    #[test]
    fn test_determine_bounds() {
        let partial = vec![vec![1, 2, 3, 4], vec![4, 5, 6, 7], vec![0, 2, 4, 5]]
            .into_iter()
            .map(|data| {
                Some(DataBlock::new_from_columns(vec![Int32Type::from_data(
                    data,
                )]))
            })
            .collect::<Vec<_>>();

        let schema = DataSchemaRefExt::create(vec![DataField::new("a", Int32Type::data_type())]);
        let mut inner = StateInner {
            partitions: 3,
            schema,
            sort_desc: vec![SortColumnDescription {
                offset: 0,
                asc: true,
                nulls_first: false,
            }],
            partial,
            bounds: None,
        };

        inner.determine_bounds().unwrap();

        // 0 1 2 2 | 3 4 4 4 | 5 5 6 7
        assert_eq!(Int32Type::from_data(vec![3, 5]), inner.bounds.unwrap())
    }
}
