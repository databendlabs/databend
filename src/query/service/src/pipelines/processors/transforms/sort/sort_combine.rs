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

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_transforms::sort::Rows;
use databend_common_pipeline_transforms::AccumulatingTransform;

use super::bounds::Bounds;
use super::SortCollectedMeta;

pub struct TransformSortCombine<R: Rows> {
    batch_rows: usize,
    metas: Vec<SortCollectedMeta>,
    _r: std::marker::PhantomData<R>,
}

impl<R: Rows> TransformSortCombine<R> {
    pub fn new(batch_rows: usize) -> Self {
        Self {
            batch_rows,
            metas: vec![],
            _r: Default::default(),
        }
    }
}

impl<R: Rows> AccumulatingTransform for TransformSortCombine<R> {
    const NAME: &'static str = "TransformSortCombine";

    fn transform(&mut self, mut data: DataBlock) -> Result<Vec<DataBlock>> {
        self.metas.push(
            data.take_meta()
                .and_then(SortCollectedMeta::downcast_from)
                .expect("require a SortCollectedMeta"),
        );
        Ok(vec![])
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        if !output || self.metas.is_empty() {
            return Ok(vec![]);
        }

        let params = self.metas.first().map(|meta| meta.params).unwrap();

        let bounds = self
            .metas
            .iter_mut()
            .map(|meta| std::mem::take(&mut meta.bounds))
            .collect();
        let bounds = Bounds::merge::<R>(bounds, self.batch_rows)?;

        let blocks = self
            .metas
            .drain(..)
            .flat_map(|meta| meta.blocks.into_iter())
            .collect();

        Ok(vec![DataBlock::empty_with_meta(Box::new(
            SortCollectedMeta {
                params,
                bounds,
                blocks,
            },
        ))])
    }
}
