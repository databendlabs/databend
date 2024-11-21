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
use databend_common_expression::simpler::Simpler;
use databend_common_expression::visitor::ValueVisitor;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::SortCompare;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_transforms::processors::Transform;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use rand::rngs::StdRng;
use rand::SeedableRng;

use super::sort_exchange::SortRangeExchange;
use super::sort_spill::TransformSortSimpleWait;

pub struct SortSimpleState {
    inner: RwLock<StateInner>,
    pub(super) done: WatchNotify,
}

struct StateInner {
    partitions: usize,
    empty_block: DataBlock,
    sort_desc: Vec<SortColumnDescription>,
    partial: Vec<Option<DataBlock>>,
    bounds: Option<DataBlock>,
}

impl StateInner {
    fn determine_bounds(&mut self) -> Result<()> {
        let partial = std::mem::take(&mut self.partial)
            .into_iter()
            .filter_map(|b| {
                let b = b.unwrap();
                if b.is_empty() { None } else { Some(b) }
            })
            .collect::<Vec<_>>();

        if partial.is_empty() {
            self.bounds = Some(self.empty_block.clone());
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

        self.bounds = Some(candidates.take(&bounds)?);
        Ok(())
    }
}

impl SortSimpleState {
    fn new(
        inputs: usize,
        partitions: usize,
        empty_block: DataBlock,
        sort_desc: Arc<Vec<SortColumnDescription>>,
    ) -> Arc<SortSimpleState> {
        let sort_desc = sort_desc
            .iter()
            .enumerate()
            .map(|(i, desc)| SortColumnDescription {
                offset: i,
                asc: desc.asc,
                nulls_first: desc.nulls_first,
            })
            .collect::<Vec<_>>();
        Arc::new(SortSimpleState {
            inner: RwLock::new(StateInner {
                partitions,
                empty_block,
                sort_desc,
                partial: vec![None; inputs],
                bounds: None,
            }),
            done: WatchNotify::new(),
        })
    }

    pub fn bounds(&self) -> Option<DataBlock> {
        if let Some(bounds) = &self.inner.read().unwrap().bounds {
            return Some(bounds.clone());
        }
        None
    }

    fn commit_simple(&self, id: usize, block: Option<DataBlock>) -> Result<bool> {
        let mut inner = self.inner.write().unwrap();
        let block = block.unwrap_or(inner.empty_block.clone());
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

pub struct TransformSortSimple {
    id: usize,
    simpler: Simpler<StdRng>,
    state: Arc<SortSimpleState>,
}

unsafe impl Send for TransformSortSimple {}

impl TransformSortSimple {
    fn new(id: usize, k: usize, columns: Vec<usize>, state: Arc<SortSimpleState>) -> Self {
        let rng = StdRng::from_rng(rand::thread_rng()).unwrap();
        let simpler = Simpler::new(columns, 65536, k, rng);
        TransformSortSimple { id, simpler, state }
    }
}

impl Transform for TransformSortSimple {
    const NAME: &'static str = "TransformSortSimple";

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        self.simpler.add_block(data.clone());
        Ok(data)
    }

    fn on_finish(&mut self) -> Result<()> {
        self.simpler.compact_blocks();
        let mut simple = self.simpler.take_blocks();
        assert!(simple.len() <= 1); // Unlikely to sample rows greater than 65536
        self.state.commit_simple(
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

pub fn add_range_shuffle_exchange(
    pipeline: &mut Pipeline,
    schema: DataSchemaRef,
    sort_desc: Arc<Vec<SortColumnDescription>>,
    k: usize,
) -> Result<()> {
    use std::sync::atomic;
    let i = atomic::AtomicUsize::new(0);
    let n = pipeline.output_len();
    let columns = sort_desc.iter().map(|desc| desc.offset).collect::<Vec<_>>();
    let empty_block = DataBlock::empty_with_schema(Arc::new(schema.project(&columns)));
    let state = SortSimpleState::new(n, n, empty_block, sort_desc.clone());
    pipeline.add_transformer(|| {
        let id = i.fetch_add(1, atomic::Ordering::AcqRel);
        TransformSortSimple::new(id, k, columns.clone(), state.clone())
    });

    pipeline.add_transform(|input, output| {
        Ok(ProcessorPtr::create(Box::new(
            TransformSortSimpleWait::new(input, output, state.clone()),
        )))
    })?;

    pipeline.exchange(n, SortRangeExchange::new(n, sort_desc, state));
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
            sort_desc: vec![SortColumnDescription {
                offset: 0,
                asc: true,
                nulls_first: false,
            }],
            partial,
            bounds: None,
            empty_block: DataBlock::empty_with_schema(schema),
        };

        inner.determine_bounds().unwrap();

        // 0 1 2 2 | 3 4 4 4 | 5 5 6 7
        assert_eq!(
            &Int32Type::from_data(vec![3, 5]),
            inner.bounds.unwrap().get_last_column()
        )
    }
}
