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
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_core::TransformPipeBuilder;

use crate::processors::AccumulatingTransform;
use crate::processors::AccumulatingTransformer;
use crate::processors::AsyncAccumulatingTransform;
use crate::processors::AsyncAccumulatingTransformer;
use crate::processors::AsyncTransform;
use crate::processors::AsyncTransformer;
use crate::processors::Transform;
use crate::processors::Transformer;

pub trait TransformPipelineHelper {
    fn try_add_transform_with_builder<F, R>(
        &mut self,
        f: F,
        create: impl Fn(Arc<InputPort>, Arc<OutputPort>, R) -> Box<dyn Processor>,
    ) -> Result<()>
    where
        F: Fn() -> Result<R>;

    fn try_add_transformer<F, R>(&mut self, f: F) -> databend_common_exception::Result<()>
    where
        F: Fn() -> Result<R>,
        R: Transform + 'static,
    {
        self.try_add_transform_with_builder(f, Transformer::<R>::create)
    }

    fn add_transformer<F, R>(&mut self, f: F)
    where
        F: Fn() -> R,
        R: Transform + 'static,
    {
        // Safe to unwrap, since the closure always return Ok(_).
        self.try_add_transformer(|| Ok(f())).unwrap()
    }

    fn try_add_async_transformer<F, R>(&mut self, f: F) -> databend_common_exception::Result<()>
    where
        F: Fn() -> Result<R>,
        R: AsyncTransform + 'static,
    {
        self.try_add_transform_with_builder(f, AsyncTransformer::<R>::create)
    }

    fn add_async_transformer<F, R>(&mut self, f: F)
    where
        F: Fn() -> R,
        R: AsyncTransform + 'static,
    {
        // Safe to unwrap, since the closure always return Ok(_).
        self.try_add_async_transformer(|| Ok(f())).unwrap()
    }

    fn try_add_accumulating_transformer<F, R>(
        &mut self,
        f: F,
    ) -> databend_common_exception::Result<()>
    where
        F: Fn() -> Result<R>,
        R: AccumulatingTransform + 'static,
    {
        self.try_add_transform_with_builder(f, AccumulatingTransformer::<R>::create)
    }

    fn add_accumulating_transformer<F, R>(&mut self, f: F)
    where
        F: Fn() -> R,
        R: AccumulatingTransform + 'static,
    {
        // Safe to unwrap, since the closure always return Ok(_).
        self.try_add_accumulating_transformer(|| Ok(f())).unwrap()
    }

    fn try_add_async_accumulating_transformer<F, R>(
        &mut self,
        f: F,
    ) -> databend_common_exception::Result<()>
    where
        F: Fn() -> Result<R>,
        R: AsyncAccumulatingTransform + 'static,
    {
        self.try_add_transform_with_builder(f, AsyncAccumulatingTransformer::<R>::create)
    }

    fn add_async_accumulating_transformer<F, R>(&mut self, f: F)
    where
        F: Fn() -> R,
        R: AsyncAccumulatingTransform + 'static,
    {
        // Safe to unwrap, since the closure always return Ok(_).
        self.try_add_async_accumulating_transformer(|| Ok(f()))
            .unwrap()
    }

    fn try_create_transform_pipeline_builder_with_len<F, R>(
        &mut self,
        f: F,
        transform_len: usize,
    ) -> Result<TransformPipeBuilder>
    where
        F: Fn() -> Result<R>,
        R: Transform + 'static;
}

impl TransformPipelineHelper for Pipeline {
    fn try_add_transform_with_builder<F, R>(
        &mut self,
        f: F,
        create: impl Fn(Arc<InputPort>, Arc<OutputPort>, R) -> Box<dyn Processor>,
    ) -> Result<()>
    where
        F: Fn() -> Result<R>,
    {
        self.add_transform(|input, output| Ok(ProcessorPtr::create(create(input, output, f()?))))
    }

    fn try_create_transform_pipeline_builder_with_len<F, R>(
        &mut self,
        f: F,
        transform_len: usize,
    ) -> Result<TransformPipeBuilder>
    where
        F: Fn() -> Result<R>,
        R: Transform + 'static,
    {
        self.add_transform_with_specified_len(
            |input, output| {
                Ok(ProcessorPtr::create(Transformer::create(
                    input,
                    output,
                    f()?,
                )))
            },
            transform_len,
        )
    }
}
