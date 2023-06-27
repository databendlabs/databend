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
use std::time::Instant;

use common_exception::Result;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::Processor;
use common_profile::ProcessorProfile;
use common_profile::SharedProcessorProfiles;

use crate::processors::transforms::Transform;
use crate::processors::transforms::Transformer;

/// A profile wrapper for `Processor` trait.
/// This wrapper will record the time cost of each processor.
/// But because of the limitation of `Processor` trait,
/// we can't get the number of rows processed by the processor.
pub struct ProcessorProfileWrapper<T> {
    inner: T,
    prof_id: u32,
    proc_profs: SharedProcessorProfiles,

    prof: ProcessorProfile,
}

impl<T> ProcessorProfileWrapper<T>
where T: Processor + 'static
{
    pub fn create(
        inner: T,
        prof_id: u32,
        proc_profs: SharedProcessorProfiles,
    ) -> Box<dyn Processor> {
        Box::new(Self {
            inner,
            prof_id,
            proc_profs,
            prof: ProcessorProfile::default(),
        })
    }
}

#[async_trait::async_trait]
impl<T> Processor for ProcessorProfileWrapper<T>
where T: Processor + 'static
{
    fn name(&self) -> String {
        self.inner.name()
    }

    fn as_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        match self.inner.event()? {
            Event::Finished => {
                self.proc_profs
                    .lock()
                    .unwrap()
                    .update(self.prof_id, self.prof);
                Ok(Event::Finished)
            }
            v => Ok(v),
        }
    }

    fn process(&mut self) -> Result<()> {
        let instant = Instant::now();
        self.inner.process()?;
        let elapsed = instant.elapsed();
        self.prof = self.prof
            + ProcessorProfile {
                cpu_time: elapsed,
                ..Default::default()
            };
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        // TODO: record profile information for async process
        self.inner.async_process().await
    }
}

/// A profile wrapper for `Transform` trait.
/// This wrapper will record the time cost and the information
/// about the number of rows processed by the processor.
pub struct TransformProfileWrapper<T> {
    inner: T,
    prof_id: u32,
    proc_profs: SharedProcessorProfiles,

    prof: ProcessorProfile,
}

impl<T> TransformProfileWrapper<T>
where T: Transform + 'static
{
    pub fn create(
        inner: T,
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        prof_id: u32,
        proc_profs: SharedProcessorProfiles,
    ) -> Box<dyn Processor> {
        Box::new(Transformer::create(input_port, output_port, Self {
            inner,
            prof_id,
            proc_profs,
            prof: ProcessorProfile::default(),
        }))
    }
}

impl<T> Transform for TransformProfileWrapper<T>
where T: Transform + 'static
{
    const NAME: &'static str = "TransformProfileWrapper";

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        let input_rows = data.num_rows();
        let input_bytes = data.memory_size();

        let instant = Instant::now();
        let res = self.inner.transform(data)?;
        let elapsed = instant.elapsed();
        self.prof = self.prof
            + ProcessorProfile {
                cpu_time: elapsed,
                input_rows: self.prof.input_rows + input_rows,
                input_bytes: self.prof.input_bytes + input_bytes,
                output_rows: self.prof.output_rows + res.num_rows(),
                output_bytes: self.prof.output_bytes + res.memory_size(),
            };
        Ok(res)
    }

    fn on_finish(&mut self) -> Result<()> {
        self.proc_profs
            .lock()
            .unwrap()
            .update(self.prof_id, self.prof);
        Ok(())
    }
}
