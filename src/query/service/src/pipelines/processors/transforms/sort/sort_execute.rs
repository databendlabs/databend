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

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_transforms::processors::sort::algorithm::SortAlgorithm;
use databend_common_pipeline_transforms::HookTransform;
use databend_common_pipeline_transforms::HookTransformer;

use super::sort_spill::SortSpill;
use super::Base;
use super::SortBound;
use super::SortCollectedMeta;
use crate::pipelines::processors::transforms::sort::sort_spill::OutputData;

pub struct TransformSortRestore<A: SortAlgorithm> {
    output: Option<DataBlock>,

    /// If the next transform of current transform is [`super::transform_multi_sort_merge::MultiSortMergeProcessor`],
    /// we can generate and output the order column to avoid the extra converting in the next transform.
    remove_order_col: bool,

    base: Base,
    inner: Option<SortSpill<A>>,
}

impl<A> TransformSortRestore<A>
where A: SortAlgorithm + Send + 'static
{
    pub(super) fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        base: Base,
        output_order_col: bool,
    ) -> Result<HookTransformer<Self>> {
        Ok(HookTransformer::new(input, output, Self {
            output: None,
            remove_order_col: !output_order_col,
            base,
            inner: None,
        }))
    }
}

#[async_trait::async_trait]
impl<A> HookTransform for TransformSortRestore<A>
where
    A: SortAlgorithm + 'static,
    A::Rows: 'static,
{
    const NAME: &'static str = "TransformSortExecute";

    fn on_input(&mut self, mut block: DataBlock) -> Result<()> {
        assert!(self.inner.is_none());
        let meta = block
            .take_meta()
            .and_then(SortCollectedMeta::downcast_from)
            .expect("require a SortCollectedMeta");

        self.inner = Some(SortSpill::<A>::from_meta(self.base.clone(), meta));
        Ok(())
    }

    fn on_output(&mut self) -> Result<Option<DataBlock>> {
        Ok(self.output.take())
    }

    fn need_process(&self, _: bool) -> Option<Event> {
        if self.inner.is_some() {
            Some(Event::Async)
        } else {
            None
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        let Some(spill_sort) = &mut self.inner else {
            unreachable!()
        };
        let OutputData {
            block,
            bound,
            finish,
        } = spill_sort.on_restore().await?;
        if let Some(block) = block {
            let mut block = block.add_meta(Some(SortBound { bound }.boxed()))?;
            if self.remove_order_col {
                block.pop_columns(1);
            }
            self.base.append_exchange_key(block, bound.as_ref());
            self.output = Some(block);
        }
        if finish {
            self.inner = None;
        }
        Ok(())
    }
}

impl Base {
    fn append_exchange_key(&self, &mut block: DataBlock, bound: Option<&Scalar>) {
        let data_type = self
            .schema
            .field(self.base.sort_row_offset)
            .data_type()
            .wrap_nullable();
        
        let mut builder = ColumnBuilder::with_capacity(&data_type, 1);
        let bound = match bound {
            Some(bound) => bound.as_ref(),
            None => Scalar::Null.as_ref(),
        };
        builder.push(bound);
        block.add_const_column(builder.build_scalar(), data_type);
    }
}
