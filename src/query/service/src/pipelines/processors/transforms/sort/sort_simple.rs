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

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::RwLock;

use databend_common_base::base::WatchNotify;
use databend_common_exception::Result;
use databend_common_expression::simpler::Simpler;
use databend_common_expression::visitor::ValueVisitor;
use databend_common_expression::DataBlock;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::SortCompare;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use rand::rngs::StdRng;
use rand::SeedableRng;

use super::sort_exchange::SortRangeExchange;

pub struct TransformSortSimple {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    input_data: Option<DataBlock>,
    output_data: VecDeque<DataBlock>,

    id: usize,
    simpler: Simpler<StdRng>,
    blocks: Vec<DataBlock>,
    state: Arc<SortSimpleState>,
    step: Step,
}

unsafe impl Send for TransformSortSimple {}

enum Step {
    Commit,
    Wait,
    Finish,
}

impl TransformSortSimple {
    fn new(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        id: usize,
        k: usize,
        columns: Vec<usize>,
        state: Arc<SortSimpleState>,
    ) -> Self {
        let rng = StdRng::from_rng(rand::thread_rng()).unwrap();
        let simpler = Simpler::new(columns, 65536, k, rng);
        TransformSortSimple {
            input,
            output,
            input_data: None,
            output_data: VecDeque::default(),
            id,
            simpler,
            blocks: Vec::default(),
            state,
            step: Step::Commit,
        }
    }

    fn collect(&mut self, data: DataBlock) -> Result<()> {
        self.simpler.add_block(data.clone());
        self.blocks.push(data);

        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        self.simpler.compact_blocks();
        let mut simple = self.simpler.take_blocks();
        assert!(simple.len() <= 1);
        let simple = if simple.is_empty() {
            DataBlock::empty()
        } else {
            simple.remove(0)
        };

        let done = self.state.commit_simple(self.id, simple)?;
        self.step = if done {
            self.output_data = VecDeque::from(std::mem::take(&mut self.blocks));
            Step::Finish
        } else {
            Step::Wait
        };

        Ok(())
    }
}

#[async_trait::async_trait]
impl Processor for TransformSortSimple {
    fn name(&self) -> String {
        "TransformSortSimple".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data.pop_front() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.input_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            self.input_data = Some(self.input.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            return match self.step {
                Step::Commit => Ok(Event::Sync),
                Step::Wait => Ok(Event::Async),
                Step::Finish => {
                    self.output.finish();
                    Ok(Event::Finished)
                }
            };
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(block) = self.input_data.take() {
            return self.collect(block);
        }

        match self.step {
            Step::Commit => self.commit(),
            _ => unreachable!(),
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        self.state.done.notified().await;
        self.output_data = VecDeque::from(std::mem::take(&mut self.blocks));
        self.step = Step::Finish;
        Ok(())
    }
}

pub struct SortSimpleState {
    inner: RwLock<StateInner>,
    done: WatchNotify,
}

struct StateInner {
    partitions: usize,
    sort_desc: Vec<SortColumnDescription>,
    partial: Vec<Option<DataBlock>>,
    bounds: Option<DataBlock>,
}

impl StateInner {
    fn determine_bounds(&mut self) -> Result<()> {
        let partial = std::mem::take(&mut self.partial)
            .into_iter()
            .map(|b| b.unwrap())
            .collect::<Vec<_>>();

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

    fn commit_simple(&self, id: usize, block: DataBlock) -> Result<bool> {
        let mut inner = self.inner.write().unwrap();
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

pub fn add_range_shuffle_exchange(
    pipeline: &mut Pipeline,
    sort_desc: Arc<Vec<SortColumnDescription>>,
    k: usize,
) -> Result<()> {
    use std::sync::atomic;
    let i = atomic::AtomicUsize::new(0);
    let n = pipeline.output_len();
    let columns = sort_desc.iter().map(|desc| desc.offset).collect::<Vec<_>>();
    let state = SortSimpleState::new(n, n, sort_desc.clone());
    pipeline.add_transform(|input, output| {
        let id = i.fetch_add(1, atomic::Ordering::AcqRel);
        Ok(ProcessorPtr::create(Box::new(TransformSortSimple::new(
            input,
            output,
            id,
            k,
            columns.clone(),
            state.clone(),
        ))))
    })?;

    pipeline.exchange(n, SortRangeExchange::new(n, sort_desc, state));
    Ok(())
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::Int32Type;
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

        let mut inner = StateInner {
            partitions: 3,
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
        assert_eq!(
            &Int32Type::from_data(vec![3, 5]),
            inner.bounds.unwrap().get_last_column()
        )
    }
}
